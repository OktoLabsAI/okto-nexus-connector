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
            # CN2/N06.1 (b09): the URL/config reaches the harness
            # process through an EPHEMERAL per-session provider home —
            # a fresh directory containing ONLY the generated direct-
            # HTTP MCP client config (no hooks, no user data). The
            # token stays env-bound (never in the file); the file
            # carries the Server URL + bearer env reference.
            overlay = LaunchOverlay(http_templates=tuple(templates))
            ephemeral_home = self._mcp_session_home(
                binding, resolution.session_id, templates)
            if ephemeral_home is not None:
                overlay.provider_home = str(ephemeral_home)
                overlay.trusted_home = True  # our own fresh directory
            else:
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
        # CN2-01.03: no local lease resurrection on submit completion -
        # only an authorized renewal advances the deadline.
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
        except CoreError as error:
            # CN2/N05 (b07[core_exception]): typed Core failure during
            # close. A proven pre-admission/pre-effect refusal releases
            # only the stop reservation (retryable); anything else is
            # unknown and keeps the session managed.
            if error.retry_safe and not error.possible_effect:
                session.closing = False
                session.stop_attempt = None
                raise ConnectorError(error.code, error.stage,
                                     error.message or error.code,
                                     retry_safe=True,
                                     action="The close was refused before "
                                            "any effect; retry the stop.") \
                    from None
            session.stop_attempt.phase = "unknown"
            session.stop_attempt.receipt = None
            return {"session_id": session_id,
                    "receipt": None,
                    "outcome": "unknown",
                    "error": {"code": error.code, "stage": error.stage},
                    "note": "close outcome is unknown; the session stays "
                            "managed and a new stop re-contains it"}
        # CN2/N05 (b07[failed_receipt]): typed receipt classification.
        self._classify_close_receipt(session, receipt)
        if session.stop_attempt is not None and \
                session.stop_attempt.phase == "unknown":
            return {"session_id": session_id,
                    "receipt": _receipt_json(receipt),
                    "outcome": "unknown",
                    "note": "close outcome is unknown; the session stays "
                            "managed and a new stop re-contains it"}
        if session.stop_attempt is None:
            # FAILED pre-effect: released, retryable, session stays.
            return {"session_id": session_id,
                    "receipt": _receipt_json(receipt),
                    "outcome": "failed_pre_effect",
                    "note": "the close failed before any effect; the "
                            "session stays managed and the stop can be "
                            "retried"}
        return {"session_id": session_id,
                "receipt": _receipt_json(receipt),
                "semantics": "stop: closes resources owned by this daemon; "
                             "external attach targets are only detached"}

    async def decide_native_approval(self, *, target,
                                     operation_id: str,
                                     request: Mapping,
                                     decision: str,
                                     response: Mapping | None
                                     ) -> dict[str, object]:
        """Answer one pending native approval/input through the Core.

        CN5-01.03: the application is MANDATORILY scoped — the session
        resolves by its FULL ``SessionKey`` (never a bare session id,
        which could select another Server's Core), the target's
        namespace/agent/workspace and its captured generation/revision
        snapshot are compared against the session's authorized state
        BEFORE the Core is called, and a divergence is a typed
        ``BINDING_NOT_AUTHORIZED``/``STALE_GENERATION`` refusal with
        zero effects (never repaired from a foreign session).

        CN4-01.03: the ACTION derives from the observed request KIND
        (never ``decision.startswith``); the decision is the Core's
        public vocabulary (accept/decline/cancel); the context carries
        exactly the grant's authorized actions.
        """
        from nexus_connector_core import CoreError  # noqa: F401
        if target.session_key is None:
            # An administrative request never reaches this port.
            raise ConnectorError(
                "VALIDATION_ERROR", "approval",
                "administrative request has no native application")
        key = target.session_key
        session = self.session_by_key(key)
        binding = session.binding
        if (binding.server_id != key.server_id
                or binding.executor_id != key.executor_id
                or binding.binding_id != target.binding_id
                or binding.agent_id != target.agent_id
                or binding.workspace_id != target.workspace_id):
            raise ConnectorError(
                "BINDING_NOT_AUTHORIZED", "approval",
                "approval target does not match the session's "
                "namespace")
        if (target.connection_generation is not None
                and target.connection_generation !=
                session.connection_generation):
            raise ConnectorError(
                "STALE_GENERATION", "approval",
                "approval captured an outdated connection generation")
        if (target.session_owner_generation is not None
                and target.session_owner_generation !=
                session.session_owner_generation):
            raise ConnectorError(
                "STALE_GENERATION", "approval",
                "approval captured an outdated session owner "
                "generation")
        if (target.authorization_revision !=
                binding.authorization_revision
                or target.configuration_revision !=
                binding.configuration_revision):
            raise ConnectorError(
                "STALE_GENERATION", "approval",
                "approval captured an outdated revision snapshot")
        runtime = self._runtime_of(session)
        action = ("input.provide" if target.kind == "input.request"
                  else "approval.decide")
        context = self._authorized_context(
            session, action,
            containment=action == "approval.decide"
            and decision == "decline")
        receipt = await runtime.decide_native_approval(
            NativeApprovalOperation(
                operation_id=operation_id,
                session_id=key.session_id,
                request=dict(request) if request else {},
                decision=decision,
                operator_response=dict(response) if response else None),
            context)
        session.last_receipt = receipt
        return {"session_id": key.session_id,
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

    def _resolve_remote_session(self, operation) -> ManagedSession:
        """CN2/N01 + CN3-01: resolve ONLY in the wire's authenticated
        namespace and compare the FULL envelope — identity AND the
        generation/revision expectations — against the session's
        authorized snapshot. Divergence is a typed refusal with ZERO
        effects: the connector never silently "corrects" a frame by
        substituting the session's local values, because that would
        hand the Core a context whose incompatibility it must never
        see (D01 variants)."""
        session = self.session_by_key(SessionKey(
            operation.server_id, operation.executor_id,
            operation.session_id))
        binding = session.binding
        if (binding.binding_id != operation.binding_id or
                binding.agent_id != operation.agent_id or
                binding.workspace_id != operation.workspace_id):
            raise ConnectorError(
                "BINDING_NOT_AUTHORIZED", "wss_scope",
                "the authenticated envelope does not match the session "
                "resolved in its namespace",
                action="The frame targets a different binding/agent/"
                       "workspace; the connector refuses to remap it.")
        # CN3-01: the frame's EXPECTATIONS must equal the session's
        # authorized snapshot. A newer legitimate generation completes
        # the Core's public renewal FIRST; until then, work under the
        # old snapshot typed-refuses with zero effects.
        mismatches = []
        if operation.connection_generation != session. \
                connection_generation:
            mismatches.append(
                f"connection_generation frame="
                f"{operation.connection_generation} authorized="
                f"{session.connection_generation}")
        if operation.session_owner_generation != session. \
                session_owner_generation:
            mismatches.append(
                f"session_owner_generation frame="
                f"{operation.session_owner_generation} authorized="
                f"{session.session_owner_generation}")
        if operation.authorization_revision != binding. \
                authorization_revision:
            mismatches.append(
                f"authorization_revision frame="
                f"{operation.authorization_revision} authorized="
                f"{binding.authorization_revision}")
        if operation.configuration_revision != binding. \
                configuration_revision:
            mismatches.append(
                f"configuration_revision frame="
                f"{operation.configuration_revision} authorized="
                f"{binding.configuration_revision}")
        if mismatches:
            raise ConnectorError(
                "STALE_GENERATION", "wss_grant",
                "the frame's generation/revision expectations diverge "
                "from the session's authorized snapshot: "
                + "; ".join(mismatches),
                action="A new generation/revision must complete the "
                       "Core's public renewal before work is admitted "
                       "under it; the operation produced no effect.")
        return session

    async def submit_remote(self, operation):
        """Apply a Server-initiated turn submit under its operation ID."""
        session = self._resolve_remote_session(operation)
        runtime = self._runtime_of(session)
        context = self._authorized_context(session, "turn.submit")
        receipt = await runtime.submit(
            TurnOperation(operation.operation_id, operation.session_id,
                          str(operation.payload.get("text", "")),
                          expected_turn_id=operation.expected_turn_id),
            context)
        session.last_receipt = receipt
        return receipt

    async def steer_remote(self, operation):
        """Apply a Server-initiated steer preserving the native turn ID."""
        session = self._resolve_remote_session(operation)
        runtime = self._runtime_of(session)
        context = self._authorized_context(session, "turn.steer")
        receipt = await runtime.control(
            ControlOperation(operation.operation_id, operation.session_id,
                             "steer",
                             str(operation.payload.get("text", "")),
                             expected_turn_id=operation.expected_turn_id),
            context)
        session.last_receipt = receipt
        return receipt

    async def interrupt_remote(self, operation):
        """Apply a Server-initiated interrupt under its operation ID."""
        session = self._resolve_remote_session(operation)
        runtime = self._runtime_of(session)
        # Containment of an authorized resource stays available past a
        # lease deadline (Core semantics); it grants nothing new.
        context = self._authorized_context(session, "turn.interrupt",
                                           containment=True)
        receipt = await runtime.control(
            ControlOperation(operation.operation_id, operation.session_id,
                             "interrupt",
                             expected_turn_id=operation.expected_turn_id),
            context)
        session.last_receipt = receipt
        return receipt

    async def close_remote(self, operation):
        """Close through the SERVER's operation id and return ITS receipt.

        CN1/A11 (same-ID receipt) + CN2/N05 classification: FAILED with
        no effect releases only the stop reservation; unknown keeps the
        session under management.
        """
        session = self._resolve_remote_session(operation)
        runtime = self._runtime_of(session)
        session.closing = True
        if session.stop_attempt is None:
            session.stop_attempt = StopAttempt(operation.operation_id)
        session.stop_attempt.phase = "closing"
        context = self._authorized_context(session, "runtime.close",
                                           containment=True)
        try:
            receipt = await runtime.close(
                CloseOperation(operation.operation_id,
                               operation.session_id), context)
        except CoreError as error:
            # CN2/N05: a typed pre-effect refusal releases the stop
            # reservation (retryable); anything else is unknown.
            if error.retry_safe and not error.possible_effect:
                session.closing = False
                session.stop_attempt = None
                raise ConnectorError(error.code, error.stage,
                                     error.message or error.code,
                                     retry_safe=True) from None
            session.stop_attempt.phase = "unknown"
            raise
        self._classify_close_receipt(session, receipt)
        return receipt

    def _classify_close_receipt(self, session: ManagedSession,
                                receipt) -> None:
        """CN2/N05 (b07): typed receipt classification - only a proven
        physical/durable completion removes management; FAILED with no
        effect releases just the stop reservation."""
        stage = receipt.stage if receipt is not None else "OUTCOME_UNKNOWN"
        if stage == "OUTCOME_UNKNOWN":
            session.stop_attempt.phase = "unknown"
            session.stop_attempt.receipt = receipt
            return
        if stage == "FAILED" and not receipt.possible_effect:
            # Proven pre-effect failure: retryable, session stays.
            session.closing = False
            session.stop_attempt = None
            return
        session.stop_attempt.phase = "resolved"
        self._cleanup(session)

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
        if not bindings:
            return {"receipts": [], "snapshots": []}
        namespace_server = server_id or bindings[0].server_id
        namespace_executor = executor_id or bindings[0].executor_id
        # CN2/N04 (b10): durable history comes from the PUBLIC Journal
        # port — a receipt is answerable even after the executable was
        # removed; a missing live handle is unknown ownership, never
        # "operation did not exist".
        # CN3-04 (D04): a COLD host (no runtime ever composed) OPENS
        # the technical journal explicitly before answering — an
        # unopened store is state to RECOVER, not evidence of absence,
        # and a read failure is an error, never an empty success.
        receipts = []
        snapshots = []
        journal = await self._host.ensure_history_journal()
        if journal is not None:
            from nexus_connector_core import OperationKey
            for operation_id in operation_ids:
                durable = await journal.get_receipt(OperationKey(
                    namespace_server, namespace_executor, operation_id))
                if durable is not None:
                    receipts.append({"operation_id": durable.operation_id,
                                     "stage": durable.stage})
            for session_id in session_ids:
                snapshots.append({"session_id": session_id,
                                  "ownership": "unknown"})
        if not session_ids:
            # Live observation only when a live runtime of THIS
            # namespace exists (its own session runtimes included).
            runtime = self._live_runtime_for(state, namespace_server)
            if runtime is not None:
                report: ReconcileReport = await runtime.reconcile(
                    ReconcileRequest(namespace_server, namespace_executor,
                                     operation_ids, session_ids))
                receipts = [{"operation_id": receipt.operation_id,
                             "stage": receipt.stage}
                            for receipt in report.receipts
                            if receipt is not None]
                snapshots = [{"session_id": snapshot.session_id,
                              "ownership": snapshot.ownership}
                             for snapshot in report.snapshots]
        return {"receipts": receipts, "snapshots": snapshots}

    def _live_runtime_for(self, state: ConnectorState,
                          server_id: str):
        """A LIVE runtime of this namespace (binding-level or any
        session-scoped instance); None when nothing is live — history
        still answers via the journal."""
        runtime = None
        for binding in state.bindings:
            if binding.server_id != server_id:
                continue
            runtime = self._host.get(binding.binding_id, binding.server_id)
            if runtime is not None:
                return runtime
        for key, candidate in getattr(self._host, "_runtimes",
                                       {}).items():
            if key.server_id == server_id:
                return candidate
        return runtime

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

    def _mcp_session_home(self, binding: BindingRecord, session_id: str,
                          templates: list) -> Path | None:
        """CN2/N06.1: ephemeral per-session config directory.

        Contains ONLY the generated MCP client entries (codex TOML /
        claude JSON via the Core's public renderers). The harness reads
        its config from HOME (codex: ~/.codex/config.toml; claude:
        ~/.claude.json), so provider_home points at this fresh tree;
        provider credentials flow separately through approved secret
        references, never through this directory.
        """
        if not templates:
            return None
        # CN4-03.01 (P03): the tree name derives from a VERSIONED DIGEST
        # of the canonical ownership tuple — complete IDs survive the
        # bounded name (two 85-char server ids differing at the end are
        # DISTINCT trees; sanitization/truncation can never merge them).
        # A readable prefix is cosmetic only; the digest is the key.
        home = _mcp_home_dir(Path(self._host.root), binding.server_id,
                             binding.executor_id, binding.binding_id,
                             session_id)
        # CN4-03.02: the ownership marker is VALIDATED BEFORE any write
        # — a divergent/foreign marker refuses this execution; the tree
        # is never overwritten ahead of validation.
        import json as _json_owner
        import time as _time_owner
        marker = home / ".owner.json"
        owner_record = {"layout": "v2",
                        "server_id": binding.server_id,
                        "executor_id": binding.executor_id,
                        "binding_id": binding.binding_id,
                        "session_id": session_id,
                        "created_at": _time_owner.strftime(
                            "%Y-%m-%dT%H:%M:%SZ", _time_owner.gmtime())}
        if marker.exists():
            try:
                existing = _json_owner.loads(
                    marker.read_text(encoding="utf-8"))
            except (ValueError, OSError):
                existing = None
            same = isinstance(existing, dict) and all(
                existing.get(field) == owner_record[field]
                for field in ("layout", "server_id", "executor_id",
                              "binding_id", "session_id"))
            if not same:
                raise ConnectorError(
                    "VALIDATION_ERROR", "mcp_home",
                    "config tree exists with a different owner; "
                    "refusing to overwrite it",
                    action="The tree belongs to another execution "
                           "namespace. Inspect it manually or remove "
                           "it before reusing this path.")
        else:
            if home.exists() and any(home.iterdir()):
                raise ConnectorError(
                    "VALIDATION_ERROR", "mcp_home",
                    "config tree exists without an ownership marker; "
                    "refusing to reuse it",
                    action="Unattributed trees are never overwritten "
                           "blindly; reconcile or remove it manually.")
            home.mkdir(parents=True, exist_ok=True)
            _atomic_write(marker, _json_owner.dumps(
                owner_record, indent=1))
            home = home  # marker is the only pre-write side effect
        from nexus_connector_core.harness_config import \
            render_codex_toml_fragment
        import json as _json
        for template in templates:
            if template.adapter_id == "codex_app_server":
                codex_dir = home / ".codex"
                codex_dir.mkdir(exist_ok=True)
                _atomic_write(codex_dir / "config.toml",
                              render_codex_toml_fragment(template))
            elif template.adapter_id == "claude_stream":
                document = {"mcpServers": {
                    template.entry_name: template.entry()}}
                _atomic_write(home / ".claude.json",
                              _json.dumps(document, indent=2))
        return home

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


def _mcp_home_dir(root: Path, server_id: str, executor_id: str,
                  binding_id: str, session_id: str) -> Path:
    """CN4-03.01: bounded, collision-free tree name.

    The digest covers the CANONICAL ownership tuple (JSON with sorted
    keys and fixed separators — no ambiguous concatenation); the same
    readable prefix can never merge two owners because the full digest
    is always part of the name.
    """
    import hashlib as _hashlib
    import json as _json
    canonical = _json.dumps(
        [server_id, executor_id, binding_id, session_id],
        separators=(",", ":"), ensure_ascii=False)
    digest = _hashlib.sha256(canonical.encode("utf-8")
                            ).hexdigest()[:24]
    readable = _safe_segment(f"{server_id}~{binding_id}~{session_id}")[:48]
    return root / "runtime" / "mcp" / f"v2-{readable}-{digest}"


def _safe_segment(value: str) -> str:
    """CN3-06: path-safe, bounded namespace segment (validated)."""
    import re as _re
    cleaned = _re.sub(r"[^A-Za-z0-9_.-]", "_", value)[:80]
    return cleaned or "_"


def _atomic_write(path: Path, content: str) -> None:
    """CN3-04.02: staging + atomic replace under the owning namespace."""
    import os as _os
    import tempfile as _tf
    fd, temp = _tf.mkstemp(dir=str(path.parent), prefix=".cfg-")
    try:
        with _os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(content)
        _os.replace(temp, path)
        temp = None
    finally:
        if temp is not None:
            try:
                _os.unlink(temp)
            except OSError:
                pass


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
