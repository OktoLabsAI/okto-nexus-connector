"""Managed runtime lifecycle on top of the public Core API (plan C05).

CN1 corrections applied here (audit A02/A03/A08/A10/A11):

* Typed scope keys everywhere — pumps, lease tasks and stop attempts
  are keyed by the Core's ``SessionKey``; two Servers may mint the same
  textual session id (A03).
* ``ExecutionContext`` is built ONLY from authenticated evidence: no
  fallback permission set, no fabricated lease extension (A02).
* Stop/shutdown separate request from physical result: a pre-effect
  failure leaves the attempt retryable; ``OUTCOME_UNKNOWN`` keeps the
  session under management; reports quote the Core's own outcomes
  verbatim, never inferring graceful/forced from exceptions (A08).
* The managed start wires the session capability into the effective
  child environment through the Core's public templates, with ONE
  runtime per session so credentials are never shared between sessions
  of the same binding (A10).
* Remote verbs preserve the full NXL operation envelope (IDs, hash,
  expected_turn_id) and return the receipt of the SAME intent (A11).
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
    ExecutionContext, LaunchIntent, NativeApprovalOperation,
    OpenOperation, OperationReceipt, ReconcileReport, ReconcileRequest,
    RuntimeEvent, SessionKey, ShutdownPolicy, TurnOperation,
)
from nexus_connector_core.harness_config import harness_http_template
from nexus_connector_core.receipt_reducer import receipt_frame

from ..errors import ConnectorError
from ..identity.vault import SecretVault
from ..redaction import redact_text
from ..storage.state_store import BindingRecord, ConnectorState, StateStore
from ..transport.https_client import NexusHTTPClient, origin_of
from .core_host import CoreRuntimeHost, LaunchOverlay, LaunchSecretResolver

_HEARTBEAT_LOG = 512
_ADAPTER_HTTP_MCP = frozenset({"codex_app_server", "claude_stream"})


@dataclass(slots=True)
class StopAttempt:
    """CN1/A08: a stop request is not a physical result.

    ``operation_id`` identifies the close intent; ``phase`` tracks it
    through resolving → closing → resolved/unknown. A failure that is
    PROVEN pre-effect resets the attempt so the operator can retry; an
    unknown outcome keeps the session recoverable.
    """

    operation_id: str
    phase: str = "resolving"  # resolving|closing|resolved|unknown
    receipt: OperationReceipt | None = None


@dataclass(slots=True)
class ManagedSession:
    session_id: str
    binding: BindingRecord
    stream_epoch: str
    opened_at: float
    runtime: object = None
    last_receipt: OperationReceipt | None = None
    lease_deadline: float = 0.0
    connection_generation: int = 1
    session_owner_generation: int = 1
    authorized_actions: frozenset = frozenset()
    closing: bool = False
    stop_attempt: StopAttempt | None = None
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
        self._sessions: dict[SessionKey, ManagedSession] = {}
        self._index: dict[str, set] = {}
        self._event_publisher = event_publisher
        self._connection_generation = connection_generation
        # CN1/A03: auxiliary registries keyed by the typed SessionKey.
        self._pumps: dict[SessionKey, asyncio.Task] = {}
        self._lease_tasks: dict[SessionKey, asyncio.Task] = {}
        # CN1/A08: draining blocks NEW admissions at the daemon edge.
        self._draining = False

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

    def begin_drain(self) -> None:
        self._draining = True

    def _refuse_draining(self, stage: str) -> None:
        if self._draining:
            raise ConnectorError(
                "DAEMON_DRAINING", stage,
                "the daemon is draining and refuses new work",
                retry_safe=False,
                action="Wait for shutdown to finish or start the daemon "
                       "again; queries and containment are still allowed.")

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

    def session_by_key(self, key: SessionKey) -> ManagedSession:
        session = self._sessions.get(key)
        if session is None:
            raise ConnectorError("VALIDATION_ERROR", "session",
                                 f"unknown managed session {key!r}",
                                 action="List sessions with "
                                        "'runtime status'.")
        return session

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
                "stop_phase": session.stop_attempt.phase
                if session.stop_attempt else None,
                "last_stage": session.last_receipt.stage
                if session.last_receipt else None,
            })
        return report

    # -- start ------------------------------------------------------------

    async def start(self, *, alias: str, project: Path | None,
                    harness: str | None, new_session: bool,
                    text: str | None) -> dict[str, object]:
        self._refuse_draining("start")
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
            # CN1/A10: request the session capability and build the
            # ephemeral direct-HTTP MCP client configuration that the
            # harness process will consume (URL + env-bound token ref).
            capabilities: dict[str, str] = {}
            templates: list = []
            server_url = self._server_url(state, binding)
            server_origin = origin_of(server_url)
            from urllib.parse import urlsplit
            is_loopback = (urlsplit(server_url).hostname or "").lower() \
                in ("127.0.0.1", "::1", "localhost")
            if binding.adapter_id in _ADAPTER_HTTP_MCP:
                capability = await http.session_capability(
                    key, binding_id=binding.binding_id,
                    session_id=resolution.session_id,
                    actions=("tools/call", "resources/read", "prompts/get"))
                capabilities[capability.capability_ref] = \
                    capability.capability
                templates.append(harness_http_template(
                    binding.adapter_id,
                    server_url + "/mcp",
                    capability.capability_ref,
                    entry_name=binding.mcp_entry_name or "nexus",
                    harness_is_local=is_loopback,
                    loopback_reachable=is_loopback,
                    approved_origins={server_origin},
                    format_qualified=True))
            overlay = LaunchOverlay(http_templates=tuple(templates))
            overlay.provider_home, overlay.trusted_home = \
                self._provider_home(state, binding)
            resolver = LaunchSecretResolver(self._vault, capabilities)
            environment = _environment_with(resolver, overlay)
            # CN1/A10: ONE runtime per session — each launch resolves
            # its OWN capability/env; no cross-session token reuse.
            runtime = await self._host.build(
                binding, environment=environment,
                session_id=resolution.session_id)

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
            stream_epoch=open_op.stream_epoch, opened_at=time.time(),
            runtime=runtime)
        session.last_receipt = receipt
        # CN1/A02: the AUTHORIZED evidence travels with the session —
        # remote operations derive their context from it, never from a
        # frame's self-declared fields.
        session.authorized_actions = frozenset(context.allowed_actions)
        session.lease_deadline = time.monotonic() + \
            min(resolution.lease_seconds, 120.0)
        session.connection_generation = context.connection_generation
        session.session_owner_generation = context.session_owner_generation
        self._register(session)
        self._pumps[session.key] = asyncio.create_task(
            self._event_pump(session))
        self._lease_tasks[session.key] = asyncio.create_task(
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

    async def submit(self, session_id: str, text: str) -> dict[str, object]:
        self._refuse_draining("submit")
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
            runtime = self._runtime_of(session)
            context = self._context(session.binding, resolution, identity)
            receipt = await runtime.submit(
                TurnOperation(resolution.operation_id, session_id, text),
                context)
            await _publish_operation_receipt(
                http, key, receipt, session.binding, "turn.submit",
                {"text_length": len(text)})
        session.last_receipt = receipt
        session.lease_deadline = time.monotonic() + 90.0
        return {"session_id": session_id, "receipt": _receipt_json(receipt)}

    # -- observation --------------------------------------------------------

    async def inspect(self, session_id: str) -> dict[str, object]:
        session = self.session(session_id)
        runtime = self._runtime_of(session)
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
        runtime = self._runtime_of(session)
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
        """Close owned resources with explicit stop-attempt semantics.

        CN1/A08: a failure PROVEN before any effect resets the attempt
        (``closing`` returns to False — the stop is retryable). An
        ``OUTCOME_UNKNOWN`` close keeps the session under management so
        the next lifecycle call can re-contain the same resource. Only
        a proven terminal path removes it.
        """
        session = self.session(session_id)
        if session.stop_attempt is not None and \
                session.stop_attempt.phase == "closing":
            return {"session_id": session_id, "already_closing": True}
        session.closing = True
        session.stop_attempt = StopAttempt(
            operation_id=f"stop-{uuid.uuid4().hex[:12]}")
        runtime = self._runtime_of(session)
        receipt = None
        try:
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
                session.stop_attempt.phase = "closing"
                context = self._context(session.binding, resolution,
                                        identity)
                receipt = await runtime.close(
                    CloseOperation(resolution.operation_id, session_id),
                    context)
        except ConnectorError as error:
            if not error.possible_effect:
                # Proven pre-effect: the attempt is released so the
                # operator can retry (test_13) — nothing happened.
                session.closing = False
                session.stop_attempt = None
                raise
            session.stop_attempt.phase = "unknown"
            session.stop_attempt.receipt = receipt
            return {"session_id": session_id,
                    "receipt": _receipt_json(receipt),
                    "outcome": "unknown",
                    "error": error.to_json(),
                    "note": "close outcome is unknown; the session stays "
                            "managed and a new stop re-contains it"}
        # Physical result gates removal (CN1/A08, test_14).
        stage = receipt.stage if receipt else "OUTCOME_UNKNOWN"
        if stage == "OUTCOME_UNKNOWN":
            session.stop_attempt.phase = "unknown"
            session.stop_attempt.receipt = receipt
            return {"session_id": session_id,
                    "receipt": _receipt_json(receipt),
                    "outcome": "unknown",
                    "note": "close outcome is unknown; the session stays "
                            "managed and a new stop re-contains it"}
        session.stop_attempt.phase = "resolved"
        self._cleanup(session)
        return {"session_id": session_id,
                "receipt": _receipt_json(receipt),
                "semantics": "stop: closes resources owned by this daemon; "
                             "external attach targets are only detached"}

    async def decide_native_approval(self, session_id: str,
                                     operation_id: str, request: Mapping,
                                     decision: str,
                                     response: Mapping | None
                                     ) -> dict[str, object]:
        """Answer one pending native approval/input through the Core.

        CN1/A11+CN-06.05: the decision travels the Core's public
        ``decide_native_approval`` with the pending request projection;
        containment-reply semantics (deny allowed while closing) belong
        to the Core.
        """
        session = self.session(session_id)
        runtime = self._runtime_of(session)
        context = self._authorized_context(session)
        receipt = await runtime.decide_native_approval(
            NativeApprovalOperation(operation_id, session_id, decision,
                                    dict(request) if request else {},
                                    dict(response) if response else None),
            context)
        session.last_receipt = receipt
        return {"session_id": session_id,
                "receipt": _receipt_json(receipt)}

    async def shutdown(self) -> list[dict[str, object]]:
        """Drain all managed sessions; report the Core's facts verbatim.

        CN1/A08: no inferred graceful/forced — every outcome quotes the
        Core's own ShutdownReport/close receipts; unknown sessions stay
        registered for the next lifecycle call.
        """
        self.begin_drain()
        reports = []
        for session in list(self._sessions.values()):
            if session.stop_attempt is not None and \
                    session.stop_attempt.phase == "closing":
                continue
            try:
                result = await self.stop(session.session_id)
                outcome = result.get("outcome", "resolved")
                reports.append({
                    "session_id": session.session_id,
                    "outcome": outcome,
                    "receipt": result.get("receipt"),
                    "source": "stop"})
            except ConnectorError as error:
                reports.append({
                    "session_id": session.session_id,
                    "outcome": "error",
                    "error": error.to_json(),
                    "source": "stop"})
        core_reports = await self._host.shutdown_all()
        for session_id, outcome in core_reports:
            reports.append({"session_id": session_id,
                            "outcome": outcome,
                            "source": "core-shutdown"})
        return reports

    # -- remote (WSS-initiated) operations -------------------------------

    def _runtime_of(self, session: ManagedSession):
        if session.runtime is not None:
            return session.runtime
        runtime = self._host.get(session.binding.binding_id,
                                 session.binding.server_id)
        if runtime is None:
            raise ConnectorError("SESSION_UNKNOWN", "runtime",
                                 "runtime instance is gone")
        return runtime

    async def submit_remote(self, session_id: str, operation_id: str,
                            text: str, binding_id: str, *,
                            expected_turn_id: str | None = None):
        """Apply a Server-initiated turn submit under its operation ID."""
        session = self.session(session_id)
        if session.binding.binding_id != binding_id:
            raise ConnectorError("BINDING_NOT_AUTHORIZED", "wss_submit",
                                 "session belongs to another binding")
        runtime = self._runtime_of(session)
        context = self._authorized_context(session, "turn.submit")
        receipt = await runtime.submit(
            TurnOperation(operation_id, session_id, text,
                          expected_turn_id=expected_turn_id), context)
        session.last_receipt = receipt
        return receipt

    async def steer_remote(self, session_id: str, operation_id: str,
                           text: str, binding_id: str, *,
                           expected_turn_id: str | None = None):
        """Apply a Server-initiated steer preserving the native turn ID."""
        session = self.session(session_id)
        if session.binding.binding_id != binding_id:
            raise ConnectorError("BINDING_NOT_AUTHORIZED", "wss_steer",
                                 "session belongs to another binding")
        runtime = self._runtime_of(session)
        context = self._authorized_context(session, "turn.steer")
        receipt = await runtime.control(
            ControlOperation(operation_id, session_id, "steer", text,
                             expected_turn_id=expected_turn_id), context)
        session.last_receipt = receipt
        return receipt

    async def interrupt_remote(self, session_id: str, operation_id: str,
                               binding_id: str, *,
                               expected_turn_id: str | None = None):
        """Apply a Server-initiated interrupt under its operation ID."""
        session = self.session(session_id)
        if session.binding.binding_id != binding_id:
            raise ConnectorError("BINDING_NOT_AUTHORIZED", "wss_interrupt",
                                 "session belongs to another binding")
        runtime = self._runtime_of(session)
        # Containment is always authorized on the live authorized grant:
        # deny/interrupt/force stay available past a lease deadline.
        context = self._authorized_context(session, "turn.interrupt",
                                           containment=True)
        receipt = await runtime.control(
            ControlOperation(operation_id, session_id, "interrupt",
                             expected_turn_id=expected_turn_id), context)
        session.last_receipt = receipt
        return receipt

    async def close_remote(self, session_id: str, operation_id: str,
                           binding_id: str):
        """Close through the SERVER's operation id and return ITS receipt.

        CN1/A11 (test: remote close recebe e devolve o mesmo ID): no
        second HTTP resolution, no locally-minted stop id.
        """
        session = self.session(session_id)
        if session.binding.binding_id != binding_id:
            raise ConnectorError("BINDING_NOT_AUTHORIZED", "wss_close",
                                 "session belongs to another binding")
        runtime = self._runtime_of(session)
        session.closing = True
        if session.stop_attempt is None:
            session.stop_attempt = StopAttempt(operation_id)
        session.stop_attempt.phase = "closing"
        context = self._authorized_context(session, "runtime.close",
                                           containment=True)
        receipt = await runtime.close(
            CloseOperation(operation_id, session_id), context)
        stage = receipt.stage
        if stage != "OUTCOME_UNKNOWN":
            session.stop_attempt.phase = "resolved"
            self._cleanup(session)
        else:
            session.stop_attempt.phase = "unknown"
            session.stop_attempt.receipt = receipt
        return receipt

    def _authorized_context(self, session: ManagedSession,
                            action: str, *,
                            containment: bool = False
                            ) -> ExecutionContext:
        """Context from the session's AUTHORIZED evidence only (A02).

        The deadline never extends past the session's authorized lease
        (test_04: no local resurrection of expired authorization) and
        the actions come from the grant stored at open — a frame never
        imports its own permissions. Containment verbs (interrupt,
        close, deny) stay available past the deadline per the Core's
        containment semantics; they are part of the authorized set only
        when the session's grant included them, or when they are the
        explicit containment-reply exemption for negative decisions.
        """
        binding = session.binding
        deadline = session.lease_deadline  # never extended locally
        allowed = set(session.authorized_actions)
        if containment:
            allowed.update({"turn.interrupt", "runtime.close"})
        if action not in allowed:
            raise ConnectorError(
                "BINDING_NOT_AUTHORIZED", "remote_context",
                f"action {action!r} is not part of the session's "
                "authorized grant",
                action="The Server must authorize the operation; the "
                       "connector never widens a grant.")
        return ExecutionContext(
            server_id=binding.server_id,
            executor_id=binding.executor_id,
            binding_id=binding.binding_id,
            agent_id=binding.agent_id,
            workspace_id=binding.workspace_id,
            authorization_revision=binding.authorization_revision,
            configuration_revision=binding.configuration_revision,
            connection_generation=session.connection_generation,
            lease_deadline_monotonic=deadline,
            allowed_actions=frozenset(allowed),
            session_owner_generation=session.session_owner_generation)

    async def reconcile(self, operation_ids=(), session_ids=(), *,
                        server_id: str = "", executor_id: str = ""
                        ) -> dict[str, object]:
        """Reconcile one namespace; receipts/snapshots are NXL-valid.

        CN1/A09: the ORIGINATING namespace is mandatory (never the
        first binding's); snapshots serialize as typed
        ``{session_id, ownership}`` projections that the real codec
        accepts; with no live handle the report is still answerable by
        composing a runtime for the namespace's binding (composition
        never spawns).
        """
        operation_ids = tuple(operation_ids or ())
        session_ids = tuple(session_ids or ())
        state = self._store.load()
        bindings = ([b for b in state.bindings if b.server_id == server_id]
                    if server_id else list(state.bindings))
        runtime = None
        for binding in bindings:
            runtime = self._host.get(binding.binding_id, binding.server_id)
            if runtime is not None:
                break
        if runtime is None and bindings:
            async def _no_environment(prepared):
                return {}
            runtime = await self._host.build(
                bindings[0], environment=_no_environment)
        if runtime is None:
            return {"receipts": [], "snapshots": []}
        report: ReconcileReport = await runtime.reconcile(ReconcileRequest(
            server_id or bindings[0].server_id,
            executor_id or bindings[0].executor_id,
            operation_ids, session_ids))
        receipts = [{"operation_id": receipt.operation_id,
                     "stage": receipt.stage}
                    for receipt in report.receipts if receipt is not None]
        snapshots = []
        for snapshot in report.snapshots:
            snapshots.append({
                "session_id": snapshot.session_id,
                "ownership": snapshot.ownership})
        return {"receipts": receipts, "snapshots": snapshots}

    # -- internals ----------------------------------------------------------

    def _find_reusable(self, binding: BindingRecord) -> ManagedSession | None:
        now = time.monotonic()
        for session in self._sessions.values():
            if (session.binding.binding_id == binding.binding_id
                    and session.binding.server_id == binding.server_id
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
        # CN1/A02 (test_03): the Server's intent resolution is the ONLY
        # authority — an EMPTY grant stays empty; no fallback set.
        allowed = frozenset(resolution.allowed_actions)
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
        runtime = self._runtime_of(session)
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
            session.publish(_synthetic_event(session, "event.pump.error"))

    async def _lease_loop(self, session: ManagedSession) -> None:
        try:
            while (not session.closing and session.key in self._sessions):
                await asyncio.sleep(30.0)
                if session.closing:
                    break
                runtime = self._runtime_of(session)
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
                    session.publish(_synthetic_event(
                        session, "lease.renewal.failed"))
        except asyncio.CancelledError:
            raise

    def _register(self, session: ManagedSession) -> None:
        self._sessions[session.key] = session
        self._index.setdefault(session.session_id, set()).add(session.key)

    def _cleanup(self, session: ManagedSession) -> None:
        key = session.key
        self._sessions.pop(key, None)
        keys = self._index.get(session.session_id)
        if keys is not None:
            keys.discard(key)
            if not keys:
                self._index.pop(session.session_id, None)
        pump = self._pumps.pop(key, None)
        if pump is not None:
            pump.cancel()
        lease = self._lease_tasks.pop(key, None)
        if lease is not None:
            lease.cancel()

    def session_ids(self) -> list[str]:
        return list(self._index)


def _environment_with(resolver: LaunchSecretResolver, overlay: LaunchOverlay):
    from .core_host import make_environment
    return make_environment(resolver, overlay)


async def _publish_operation_receipt(http, key, receipt, binding, action,
                                     payload) -> None:
    """Publish the local receipt; failure never erases it (CN1/A07)."""
    try:
        await http.submit_operation(
            key, operation_id=receipt.operation_id,
            binding_id=binding.binding_id,
            session_id=receipt.session_id, action=action, payload=payload)
    except ConnectorError:
        # The local receipt is durable in the journal; the report to the
        # Server can be retried without minting a second intent.
        pass


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
