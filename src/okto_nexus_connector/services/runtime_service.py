"""Managed runtime lifecycle on top of the public Core API (plan C05).

Every mutable intent is authorized by the Server (``intents:resolve``),
realized through ``LocalRuntimeCore`` and reported back as an operation.
Receipts are stable and queryable; ``interrupt`` and ``stop`` keep their
distinct semantics; attach targets are never force-stopped. Uncertain
outcomes are never replayed automatically (A.9).
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncIterator, Callable, Mapping

from nexus_connector_core import (
    CloseOperation, ControlOperation, CoreError, EventCursor,
    ExecutionContext, LaunchIntent, OpenOperation, OperationReceipt,
    ReconcileReport, ReconcileRequest, RuntimeEvent, SessionKey,
    ShutdownPolicy, TurnOperation,
)
from nexus_connector_core.harness_config import harness_http_template
from nexus_connector_core.receipt_reducer import receipt_frame

from ..errors import ConnectorError
from ..identity.vault import SecretVault
from ..redaction import redact_text
from ..storage.state_store import BindingRecord, ConnectorState, StateStore
from ..transport.https_client import NexusHTTPClient
from .core_host import CoreRuntimeHost, LaunchOverlay, LaunchSecretResolver

_HEARTBEAT_LOG = 512
_ADAPTER_HTTP_MCP = frozenset({"codex_app_server", "claude_stream"})


@dataclass(slots=True)
class ManagedSession:
    session_id: str
    binding: BindingRecord
    stream_epoch: str
    opened_at: float
    last_receipt: OperationReceipt | None = None
    lease_deadline: float = 0.0
    connection_generation: int = 1
    session_owner_generation: int = 1
    closing: bool = False
    log_ring: deque[dict[str, object]] = field(default_factory=lambda: deque(
        maxlen=_HEARTBEAT_LOG))
    waiters: list[asyncio.Queue] = field(default_factory=list)
    terminal_seen: bool = False

    @property
    def key(self) -> SessionKey:
        return SessionKey(self.binding.server_id, self.binding.executor_id,
                          self.session_id)

    def publish(self, event: RuntimeEvent) -> None:
        record = {
            "session_id": self.session_id,
            "stream_epoch": self.stream_epoch,
            "sequence": event.sequence,
            "category": event.category,
            "native_type": event.native_type,
            "operation_id": event.operation_id,
            "payload": dict(event.payload),
            "at": time.strftime("%H:%M:%S"),
        }
        self.log_ring.append(record)
        for queue in list(self.waiters):
            try:
                queue.put_nowait(record)
            except asyncio.QueueFull:
                pass


class RuntimeManager:
    """Daemon-side owner of managed sessions for all bindings."""

    def __init__(self, store: StateStore, vault: SecretVault,
                 host: CoreRuntimeHost, *,
                 event_publisher: Callable[[str, RuntimeEvent],
                                           Callable[[], None]] | None = None,
                 connection_generation: Callable[[str], int] | None = None):
        self._store = store
        self._vault = vault
        self._host = host
        self._sessions: dict[tuple, ManagedSession] = {}
        self._index: dict[str, set] = {}
        self._event_publisher = event_publisher
        self._connection_generation = connection_generation
        self._pumps: dict[str, asyncio.Task] = {}
        self._lease_tasks: dict[str, asyncio.Task] = {}

    # -- lookups ----------------------------------------------------------

    def binding_by_alias(self, alias: str) -> BindingRecord:
        state = self._store.load()
        binding = state.binding_by_alias(alias)
        if binding is None:
            raise ConnectorError("VALIDATION_ERROR", "binding",
                                 f"unknown binding alias {alias!r}",
                                 action="Run 'okto-nexus-connector connect' "
                                        "or 'bind create' first.")
        return binding

    def session(self, session_id: str) -> ManagedSession:
        """Resolve one managed session by its id.

        Session ids from independent Servers are namespaced internally;
        a collision across Servers is surfaced as an explicit ambiguity
        instead of silently addressing the wrong session.
        """
        keys = self._index.get(session_id, set())
        if not keys:
            raise ConnectorError("VALIDATION_ERROR", "session",
                                 f"unknown managed session {session_id!r}",
                                 action="List sessions with "
                                        "'runtime status'.")
        if len(keys) > 1:
            raise ConnectorError("AMBIGUOUS_BINDING", "session",
                                 f"session id {session_id!r} exists on "
                                 "multiple Servers",
                                 action="Stop it from its binding alias "
                                        "context or use a unique session "
                                        "id.")
        return self._sessions[next(iter(keys))]

    def status(self) -> list[dict[str, object]]:
        report = []
        for session in list(self._sessions.values()):
            report.append({
                "session_id": session.session_id,
                "binding_alias": session.binding.alias,
                "adapter_id": session.binding.adapter_id,
                "server_id": session.binding.server_id,
                "agent_id": session.binding.agent_id,
                "workspace_root": session.binding.workspace_root,
                "opened_at": session.opened_at,
                "lease_deadline_in": max(
                    0.0, session.lease_deadline - time.monotonic()),
                "closing": session.closing,
                "last_stage": session.last_receipt.stage
                if session.last_receipt else None,
            })
        return report

    # -- start ------------------------------------------------------------

    async def start(self, *, alias: str, project: Path | None,
                    harness: str | None, new_session: bool,
                    text: str | None) -> dict[str, object]:
        binding = self.binding_by_alias(alias)
        if harness and harness != binding.adapter_id:
            raise ConnectorError("VALIDATION_ERROR", "binding",
                                 f"binding {alias!r} uses "
                                 f"{binding.adapter_id}, not {harness!r}")
        root = (project.resolve() if project is not None
                else Path(binding.workspace_root).resolve())
        if not root.is_dir():
            raise ConnectorError("WORKSPACE_UNAVAILABLE", "start",
                                 f"project directory not found: {root}")
        try:
            resolved = root.resolve(strict=True)
        except OSError as exc:
            raise ConnectorError("WORKSPACE_UNAVAILABLE", "start",
                                 str(exc)) from exc
        if str(resolved) != binding.workspace_root:
            raise ConnectorError(
                "WORKSPACE_UNAVAILABLE", "start",
                f"current directory {resolved} is not the bound workspace "
                f"root {binding.workspace_root}",
                action="Run from the bound project directory, or create a "
                       "workspace binding for this directory with "
                       "'bind create'.")

        reusable = None if new_session else self._find_reusable(binding)
        if reusable is not None:
            return {
                "session_id": reusable.session_id,
                "binding_alias": binding.alias,
                "reused": True,
                "receipt": _receipt_json(reusable.last_receipt),
                "note": "Reused the compatible open session; use "
                        "--new-session to open another one.",
            }

        state = self._store.load()
        identity = state.identity_for(binding.server_id, binding.agent_id)
        if identity is None:
            raise ConnectorError("AGENT_AUTH_REQUIRED", "start",
                                 "the binding's identity is not imported")
        key = self._vault.resolve(identity.secret_handle)
        async with NexusHTTPClient(self._server_url(state, binding)) as http:
            resolution = await http.resolve_intent(
                key, binding_id=binding.binding_id,
                workspace_binding_id=binding.endpoint_id or
                binding.binding_id,
                intent="runtime.start", new_session=True, text=text)
            capabilities: dict[str, str] = {}
            overlay = LaunchOverlay()
            if binding.adapter_id in _ADAPTER_HTTP_MCP:
                capability = await http.session_capability(
                    key, binding_id=binding.binding_id,
                    session_id=resolution.session_id,
                    actions=("tools/call", "resources/read", "prompts/get"))
                capabilities[capability.capability_ref] = capability.capability
            resolver = LaunchSecretResolver(self._vault, capabilities)
            overlay.provider_home, overlay.trusted_home = \
                self._provider_home(state, binding)
            environment = _environment_with(resolver, overlay)
            runtime = self._host.build(binding, environment=environment)

            intent = LaunchIntent(
                agent_id=binding.agent_id, workspace_id=binding.workspace_id,
                adapter_id=binding.adapter_id,
                auth_refs=tuple(capabilities))
            context = self._context(binding, resolution, identity)
            prepared = await runtime.prepare(intent, context)
            open_op = OpenOperation(
                operation_id=resolution.operation_id,
                session_id=resolution.session_id,
                stream_epoch=f"ep-{uuid.uuid4().hex[:12]}",
                prepared=prepared)
            receipt = await runtime.open(open_op, context)

        session = ManagedSession(
            session_id=resolution.session_id, binding=binding,
            stream_epoch=open_op.stream_epoch, opened_at=time.time())
        session.last_receipt = receipt
        session.lease_deadline = (time.monotonic() +
                                  min(resolution.lease_seconds, 120.0))
        session.connection_generation = context.connection_generation
        session.session_owner_generation = context.session_owner_generation
        self._register(session)
        self._pumps[session.session_id] = asyncio.create_task(
            self._event_pump(session))
        self._lease_tasks[session.session_id] = asyncio.create_task(
            self._lease_loop(session))

        result: dict[str, object] = {
            "session_id": session.session_id,
            "binding_alias": binding.alias,
            "reused": False,
            "receipt": _receipt_json(receipt),
        }
        if text:
            turn = await self.submit(session.session_id, text)
            result["turn_receipt"] = turn["receipt"]
        return result

    # -- remote (WSS-initiated) operations -------------------------------

    async def submit_remote(self, session_id: str, operation_id: str,
                            text: str, binding_id: str):
        """Apply a Server-initiated turn submit under its operation ID."""
        session = self.session(session_id)
        if session.binding.binding_id != binding_id:
            raise ConnectorError("BINDING_NOT_AUTHORIZED", "wss_submit",
                                 "session belongs to another binding")
        runtime = self._host.get(session.binding.binding_id)
        if runtime is None:
            raise ConnectorError("SESSION_UNKNOWN", "wss_submit")
        context = self._remote_context(session)
        receipt = await runtime.submit(
            TurnOperation(operation_id, session_id, text), context)
        session.last_receipt = receipt
        return receipt

    async def interrupt_remote(self, session_id: str, operation_id: str,
                               binding_id: str):
        """Apply a Server-initiated interrupt under its operation ID."""
        session = self.session(session_id)
        if session.binding.binding_id != binding_id:
            raise ConnectorError("BINDING_NOT_AUTHORIZED", "wss_interrupt",
                                 "session belongs to another binding")
        runtime = self._host.get(session.binding.binding_id)
        if runtime is None:
            raise ConnectorError("SESSION_UNKNOWN", "wss_interrupt")
        context = self._remote_context(session)
        receipt = await runtime.control(
            ControlOperation(operation_id, session_id, "interrupt"), context)
        session.last_receipt = receipt
        return receipt

    def _remote_context(self, session: ManagedSession) -> ExecutionContext:
        """Context for WSS-initiated ops: the frame was the authority.

        Lease/ownership come from the session's current state; the
        connection generation tracks the transport that delivered the
        frame so stale channels fail STALE_GENERATION in the Core.
        """
        binding = session.binding
        lease = max(30.0, min(session.lease_deadline - time.monotonic(),
                              120.0))
        return ExecutionContext(
            server_id=binding.server_id,
            executor_id=binding.executor_id,
            binding_id=binding.binding_id,
            agent_id=binding.agent_id,
            workspace_id=binding.workspace_id,
            authorization_revision=binding.authorization_revision,
            configuration_revision=binding.configuration_revision,
            connection_generation=session.connection_generation,
            lease_deadline_monotonic=time.monotonic() + lease,
            allowed_actions=frozenset({"turn.submit", "turn.interrupt",
                                       "runtime.close"}),
            session_owner_generation=session.session_owner_generation)

    async def submit(self, session_id: str, text: str) -> dict[str, object]:
        session = self.session(session_id)
        if session.closing:
            raise ConnectorError("SESSION_CLOSED", "submit",
                                 "session is closing")
        state = self._store.load()
        identity = state.identity_for(session.binding.server_id,
                                      session.binding.agent_id)
        if identity is None:
            raise ConnectorError("AGENT_AUTH_REQUIRED", "submit")
        key = self._vault.resolve(identity.secret_handle)
        async with NexusHTTPClient(
                self._server_url(state, session.binding)) as http:
            resolution = await http.resolve_intent(
                key, binding_id=session.binding.binding_id,
                workspace_binding_id=session.binding.endpoint_id
                or session.binding.binding_id,
                intent="turn.submit", session_id=session_id, text=text)
            runtime = self._host.get(session.binding.binding_id)
            if runtime is None:
                raise ConnectorError("SESSION_UNKNOWN", "submit",
                                     "runtime instance is gone")
            context = self._context(session.binding, resolution, identity)
            receipt = await runtime.submit(
                TurnOperation(resolution.operation_id, session_id, text),
                context)
            await http.submit_operation(
                key, operation_id=receipt.operation_id,
                binding_id=session.binding.binding_id,
                session_id=session_id, action="turn.submit",
                payload={"text_length": len(text)})
        session.last_receipt = receipt
        session.lease_deadline = time.monotonic() + 90.0
        return {"session_id": session_id, "receipt": _receipt_json(receipt)}

    # -- observation --------------------------------------------------------

    async def inspect(self, session_id: str) -> dict[str, object]:
        session = self.session(session_id)
        runtime = self._host.get(session.binding.binding_id)
        if runtime is None:
            raise ConnectorError("SESSION_UNKNOWN", "inspect")
        snapshot = await runtime.inspect(session.key)
        payload = {
            "session_id": session_id,
            "ownership": snapshot.ownership,
            "process_state": snapshot.process_state,
            "turn_state": snapshot.turn_state,
            "lease_state": snapshot.lease_state,
            "last_sequence": snapshot.last_sequence,
            "log_tail": list(session.log_ring)[-10:],
            "lease_deadline_in": max(
                0.0, session.lease_deadline - time.monotonic()),
        }
        return payload

    async def logs(self, session_id: str, *, follow: bool
                   ) -> AsyncIterator[dict[str, object]]:
        """Yield buffered events, then live ones when following (TC-09)."""
        session = self.session(session_id)
        for record in list(session.log_ring):
            yield record
        if not follow:
            return
        queue: asyncio.Queue = asyncio.Queue(maxsize=1024)
        session.waiters.append(queue)
        try:
            while True:
                record = await queue.get()
                yield record
        finally:
            session.waiters.remove(queue)

    # -- controls -----------------------------------------------------------

    async def interrupt(self, session_id: str) -> dict[str, object]:
        """Cancel the active turn, preserving the runtime (plan 2.2)."""
        session = self.session(session_id)
        runtime = self._host.get(session.binding.binding_id)
        if runtime is None:
            raise ConnectorError("SESSION_UNKNOWN", "interrupt")
        state = self._store.load()
        identity = state.identity_for(session.binding.server_id,
                                      session.binding.agent_id)
        key = self._vault.resolve(identity.secret_handle)
        async with NexusHTTPClient(
                self._server_url(state, session.binding)) as http:
            resolution = await http.resolve_intent(
                key, binding_id=session.binding.binding_id,
                workspace_binding_id=session.binding.endpoint_id
                or session.binding.binding_id,
                intent="turn.interrupt", session_id=session_id)
            context = self._context(session.binding, resolution, identity)
            receipt = await runtime.control(
                ControlOperation(resolution.operation_id, session_id,
                                 "interrupt"), context)
        session.last_receipt = receipt
        return {"session_id": session_id,
                "receipt": _receipt_json(receipt),
                "semantics": "interrupt: cancels the active turn and keeps "
                             "the runtime process"}

    async def stop(self, session_id: str) -> dict[str, object]:
        """Close owned resources; external attach targets are detached."""
        session = self.session(session_id)
        if session.closing:
            return {"session_id": session_id, "already_closing": True}
        session.closing = True
        runtime = self._host.get(session.binding.binding_id)
        receipt = None
        if runtime is not None:
            state = self._store.load()
            identity = state.identity_for(session.binding.server_id,
                                          session.binding.agent_id)
            key = self._vault.resolve(identity.secret_handle)
            async with NexusHTTPClient(
                    self._server_url(state, session.binding)) as http:
                resolution = await http.resolve_intent(
                    key, binding_id=session.binding.binding_id,
                    workspace_binding_id=session.binding.endpoint_id
                    or session.binding.binding_id,
                    intent="runtime.close", session_id=session_id)
                context = self._context(session.binding, resolution, identity)
                receipt = await runtime.close(
                    CloseOperation(resolution.operation_id, session_id),
                    context)
        self._cleanup(session)
        return {"session_id": session_id,
                "receipt": _receipt_json(receipt),
                "semantics": "stop: closes resources owned by this daemon; "
                             "external attach targets are only detached"}

    async def shutdown(self) -> list[dict[str, object]]:
        """Drain all managed sessions with a bounded policy (C02.5)."""
        reports = []
        for session in list(self._sessions.values()):
            if session.closing:
                continue
            try:
                result = await self.stop(session.session_id)
                reports.append({"session_id": session.session_id,
                                "outcome": "graceful",
                                "receipt": result.get("receipt")})
            except ConnectorError as error:
                reports.append({"session_id": session.session_id,
                                "outcome": "forced" if error.possible_effect
                                else "unknown",
                                "error": error.to_json()})
        for binding_id, runtime in list(self._host._runtimes.items()):
            report = await runtime.shutdown(ShutdownPolicy(30.0, 15.0))
            for key, outcome in report.session_outcomes.items():
                reports.append({"session_id": key.session_id,
                                "outcome": str(outcome),
                                "source": "core-shutdown"})
        return reports

    async def reconcile(self, operation_ids=(), session_ids=(),
                        *, server_id: str = "", executor_id: str = "") -> dict:
        runtime = next(iter(self._host._runtimes.values()), None)
        if runtime is None:
            return {"receipts": [], "snapshots": []}
        if not server_id or not executor_id:
            binding = next(iter(self._store.load().bindings), None)
            server_id = server_id or (binding.server_id if binding else "")
            executor_id = executor_id or (
                binding.executor_id if binding else "")
        if not server_id or not executor_id:
            return {"receipts": [], "snapshots": []}
        report: ReconcileReport = await runtime.reconcile(ReconcileRequest(
            server_id, executor_id, tuple(operation_ids),
            tuple(session_ids)))
        return {"receipts": [_receipt_json(r) for r in report.receipts],
                "snapshots": [str(s) for s in report.snapshots]}

    # -- internals ----------------------------------------------------------

    def _find_reusable(self, binding: BindingRecord) -> ManagedSession | None:
        now = time.monotonic()
        for session in self._sessions.values():
            if (session.binding.binding_id == binding.binding_id
                    and not session.closing
                    and session.lease_deadline > now
                    and session.last_receipt is not None
                    and session.last_receipt.stage not in
                    ("SUCCEEDED", "FAILED", "CANCELLED")):
                return session
        return None

    def _context(self, binding: BindingRecord, resolution,
                 identity) -> ExecutionContext:
        execution = resolution.execution if resolution.execution else {}
        server_id = str(execution.get("server_id", binding.server_id))
        if server_id != binding.server_id:
            raise ConnectorError("SERVER_ID_CHANGED", "start",
                                 "the Server identity changed",
                                 action="Re-run connect after confirming the "
                                        "new installation identity.")
        generation = (self._connection_generation(server_id)
                      if self._connection_generation else 1)
        # The Server's intent resolution is the authority for allowed
        # actions; the fallback only serves offline contract tests.
        allowed = (frozenset(resolution.allowed_actions)
                   if resolution.allowed_actions else
                   frozenset({"runtime.open", "turn.submit",
                              "runtime.close"}))
        lease = min(resolution.lease_seconds, 120.0)
        return ExecutionContext(
            server_id=server_id,
            executor_id=binding.executor_id,
            binding_id=binding.binding_id,
            agent_id=binding.agent_id,
            workspace_id=binding.workspace_id,
            authorization_revision=int(execution.get(
                "authorization_revision", binding.authorization_revision)),
            configuration_revision=int(execution.get(
                "configuration_revision", binding.configuration_revision)),
            connection_generation=generation,
            lease_deadline_monotonic=time.monotonic() + lease,
            allowed_actions=allowed,
            session_owner_generation=int(execution.get(
                "session_owner_generation", 1)),
        )

    def _server_url(self, state: ConnectorState,
                    binding: BindingRecord) -> str:
        profile = state.servers.get(binding.server_id)
        if profile is None:
            raise ConnectorError("SERVER_ID_CHANGED", "transport",
                                 "server profile missing locally")
        return profile.base_url

    def _provider_home(self, state: ConnectorState, binding: BindingRecord
                       ) -> tuple[str | None, bool]:
        homes = {"codex_app_server": ".codex", "claude_stream": ".claude",
                 "pi_rpc": ".pi"}
        name = homes.get(binding.adapter_id)
        approved = bool(state.preferences.get(
            f"binding.{binding.binding_id}.trusted_provider_home", False))
        if not approved or name is None:
            return None, False
        home = Path.home() / name
        return (str(home), True) if home.is_dir() else (None, False)

    async def _event_pump(self, session: ManagedSession) -> None:
        runtime = self._host.get(session.binding.binding_id)
        if runtime is None:
            return
        publisher = (self._event_publisher(session.binding.server_id)
                     if self._event_publisher else None)
        cursor = EventCursor(session.binding.server_id,
                             session.binding.executor_id,
                             session.session_id, session.stream_epoch)
        try:
            async for event in runtime.events(cursor):
                session.publish(event)
                if publisher is not None:
                    publisher(event)
        except asyncio.CancelledError:
            raise
        except Exception:
            # The journal survives; `runtime status` still reports state.
            session.publish(_synthetic_event(session, "event.pump.error"))

    async def _lease_loop(self, session: ManagedSession) -> None:
        try:
            while (not session.closing
                   and (session.binding.server_id, session.session_id)
                   in self._sessions):
                await asyncio.sleep(30.0)
                if session.closing:
                    break
                runtime = self._host.get(session.binding.binding_id)
                if runtime is None:
                    break
                state = self._store.load()
                identity = state.identity_for(session.binding.server_id,
                                              session.binding.agent_id)
                if identity is None:
                    break
                try:
                    key = self._vault.resolve(identity.secret_handle)
                    async with NexusHTTPClient(
                            self._server_url(state, session.binding)) as http:
                        resolution = await http.resolve_intent(
                            key, binding_id=session.binding.binding_id,
                            workspace_binding_id=session.binding.endpoint_id
                            or session.binding.binding_id,
                            intent="lease.renew",
                            session_id=session.session_id)
                    context = self._context(session.binding, resolution,
                                            identity)
                    snapshot = await runtime.renew_lease(
                        session.key, context,
                        expected_connection_generation=(
                            session.connection_generation))
                    session.lease_deadline = (
                        time.monotonic() +
                        min(resolution.lease_seconds, 120.0))
                    session.connection_generation = \
                        context.connection_generation
                    session.session_owner_generation = \
                        context.session_owner_generation
                    _ = snapshot
                except (ConnectorError, CoreError):
                    # Lease renewal failed; the local fence keeps expiring.
                    session.publish(_synthetic_event(
                        session, "lease.renewal.failed"))
        except asyncio.CancelledError:
            raise

    def _register(self, session: ManagedSession) -> None:
        key = (session.binding.server_id, session.session_id)
        self._sessions[key] = session
        self._index.setdefault(session.session_id, set()).add(key)

    def _cleanup(self, session: ManagedSession) -> None:
        key = (session.binding.server_id, session.session_id)
        self._sessions.pop(key, None)
        keys = self._index.get(session.session_id)
        if keys is not None:
            keys.discard(key)
            if not keys:
                self._index.pop(session.session_id, None)
        pump = self._pumps.pop(session.session_id, None)
        if pump is not None:
            pump.cancel()
        lease = self._lease_tasks.pop(session.session_id, None)
        if lease is not None:
            lease.cancel()

    def session_ids(self) -> list[str]:
        return list(self._index)


def _environment_with(resolver: LaunchSecretResolver, overlay: LaunchOverlay):
    from .core_host import make_environment
    return make_environment(resolver, overlay)


def _synthetic_event(session: ManagedSession, kind: str) -> RuntimeEvent:
    return RuntimeEvent(
        session.binding.server_id, session.binding.executor_id,
        session.session_id, session.stream_epoch, 0, "lifecycle", kind, {})


def _receipt_json(receipt: OperationReceipt | None) -> dict[str, object] | None:
    if receipt is None:
        return None
    return {
        "operation_id": receipt.operation_id,
        "intent_hash": receipt.intent_hash,
        "stage": receipt.stage,
        "possible_effect": receipt.possible_effect,
        "retry_safe": receipt.retry_safe,
        "session_id": receipt.session_id,
        "native_id": receipt.native_id,
        "error_code": receipt.error_code,
    }


def receipt_nxl_frame(receipt: OperationReceipt, *, server_id: str,
                      executor_id: str) -> dict[str, object]:
    return receipt_frame(receipt, server_id=server_id, executor_id=executor_id)
