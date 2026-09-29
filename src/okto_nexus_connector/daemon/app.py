"""Daemon composition root: one persistent instance per trust domain.

The daemon owns the technical journal through the Core, the outbound NXL
transports (one per imported Server profile), the managed runtime sessions
and the authenticated IPC endpoint. CLI/TUI clients are observers and
limited controllers: closing them never stops the daemon or unrelated
sessions (TC-09). Event batches are acknowledged to the Core journal only
after the Server's durable-ingress ``event.ack`` (A.10).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import AsyncIterator

from nexus_connector_core import CONTRACT_REVISION
from nexus_connector_core.protocol import PROTOCOL_MAJOR
from nexus_connector_core.models import RuntimeEvent
from nexus_connector_core.protocol import submit_frame_intent_hash

from .. import __version__
from ..errors import ConnectorError
from ..identity.vault import open_vault
from ..ipc.protocol import response_error, response_ok, response_stream
from ..ipc.server import IPCServer
from ..platform import paths
from ..redaction import redact_mapping, redact_text
from ..services.core_host import CoreRuntimeHost
from ..services.runtime_service import RuntimeManager, receipt_nxl_frame
from ..storage.state_store import StateStore
from ..transport.https_client import NexusHTTPClient
from ..transport.wss_client import NXLTransport
from .lock import InstanceLock, Readiness

logger = logging.getLogger(__name__)


class EventBridge:
    """Journal-sourced event publication with bounded memory (CN1/A13).

    RAM holds ONLY cursors and one in-flight batch per stream: the
    pump's publish() is a wake-up signal, and the flusher re-reads
    bounded batches from the durable journal (the Core's replay
    source) starting after the last sequence the Server durably
    ACKed. A long offline period grows the bounded journal, not a
    Python list; ACKs are matched per (server, session, epoch) and
    only a contiguous watermark advances the Core-side ack.
    """

    _MAX_INFLIGHT_PER_STREAM = 128
    _WAKE_COALESCE_SECONDS = 0.05

    def __init__(self, ack_source, core_ack, journal_reader):
        self._ack_source = ack_source    # transport provider by server_id
        self._core_ack = core_ack        # (server, session, epoch, seq)
        self._journal_reader = journal_reader  # bounded replay reader
        # cursor: (server, session, epoch) -> last SENT sequence
        self._sent_through: dict[tuple[str, str, str], int] = {}
        self._acked_through: dict[tuple[str, str, str], int] = {}
        self._wakeups: dict[tuple[str, str, str], asyncio.Event] = {}
        self._tasks: dict[tuple[str, str, str], asyncio.Task] = {}

    def publisher_for(self, server_id: str):
        def publish(event: RuntimeEvent) -> None:
            key = (server_id, event.session_id, event.stream_epoch)
            if key not in self._sent_through:
                self._sent_through[key] = 0
            wake = self._wakeups.get(key)
            if wake is None:
                wake = self._wakeups[key] = asyncio.Event()
            wake.set()
            if key not in self._tasks or self._tasks[key].done():
                self._tasks[key] = asyncio.create_task(
                    self._flush_loop(*key))
        return publish

    async def _flush_loop(self, server_id: str, session_id: str,
                          epoch: str) -> None:
        key = (server_id, session_id, epoch)
        wake = self._wakeups[key]
        while True:
            await wake.wait()
            await asyncio.sleep(self._WAKE_COALESCE_SECONDS)
            wake.clear()
            while True:
                transport = self._ack_source(server_id)
                if transport is None or not transport.online:
                    break  # journal keeps everything; retry on wake
                after = self._sent_through.get(key, 0)
                batch = await self._journal_reader(
                    server_id, session_id, epoch, after,
                    self._MAX_INFLIGHT_PER_STREAM)
                if not batch:
                    break
                sent = await transport.send_events(list(batch))
                if not sent:
                    break  # backpressure: retry without losing events
                self._sent_through[key] = batch[-1].sequence
                watermark = await transport.wait_event_ack(
                    session_id, epoch, timeout=30.0)
                if watermark is None:
                    # No durable ACK yet: the journal stays un-acked so a
                    # reconnect replays from the acked cursor (dedup is
                    # server-side). Stop this round; the next wake retries.
                    break
                await self._core_ack(server_id, session_id, epoch,
                                     watermark)
                self._acked_through[key] = watermark
                if batch[-1].sequence <= watermark:
                    continue
                break

    async def drain(self) -> None:
        for task in list(self._tasks.values()):
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()
        self._wakeups.clear()


class DaemonApp:
    def __init__(self, root: Path):
        self.root = root
        self.store = StateStore(paths.state_file(root))
        state = self.store.load()
        self.vault = open_vault(
            paths.vault_dir(root),
            approved_fallback=bool(state.preferences.get(
                "vault.fallback_file.approved", False)))
        self.host = CoreRuntimeHost(root, self.vault)
        self.bridge = EventBridge(self._transport_for, self._core_ack,
                                  self._journal_batch)
        self.runtimes = RuntimeManager(
            self.store, self.vault, self.host,
            event_publisher=self.bridge.publisher_for,
            connection_generation=self._generation_for)
        self.transports: dict[str, NXLTransport] = {}
        self.ipc = IPCServer(self.dispatch)
        self.lock = InstanceLock(paths.pid_dir(root))
        self.started_at = time.time()
        self._stopped = asyncio.Event()
        self._draining = False
        self._approvals: dict[str, dict[str, object]] = {}

    # -- lifecycle -----------------------------------------------------------

    async def run_forever(self) -> int:
        if not self.lock.acquire():
            live = self.lock.live_owner()
            if live is not None:
                raise ConnectorError(
                    "DAEMON_ALREADY_RUNNING", "lock",
                    f"another daemon owns this state directory (pid "
                    f"{live.pid})")
            raise ConnectorError("DAEMON_ALREADY_RUNNING", "lock",
                                 "lock is held by an unverifiable process")
        try:
            token = self.lock.ensure_token()
            await self.ipc.start(paths.pid_dir(self.root), token)
            identity = _current_identity()
            self.lock.write_readiness(Readiness(
                pid=identity.pid, birth_token=identity.birth_token,
                transport=self.ipc.transport, address=self.ipc.address,
                started_at=_now(), connector_version=__version__))
            self._start_transports()
            logger.info("daemon ready (ipc=%s)", self.ipc.address)
            await self._stopped.wait()
            return await self._shutdown()
        finally:
            self.lock.clear_readiness()
            self.lock.release()

    def request_stop(self) -> None:
        self._stopped.set()

    async def _shutdown(self) -> int:
        # CN1/A08+CN-05.01: draining blocks NEW admissions at the edge
        # FIRST; queries and containment remain available.
        self._draining = True
        self.runtimes.begin_drain()
        reports: list[dict[str, object]] = []
        try:
            reports = await self.runtimes.shutdown()
        except Exception as exc:  # honest reporting, never silent loss
            reports.append({"outcome": "unknown", "error": str(exc)})
        await self.bridge.drain()
        for transport in self.transports.values():
            await transport.stop()
        self.transports.clear()
        await self.ipc.stop()
        outcomes = defaultdict(int)
        pending = 0
        for report in reports:
            outcome = str(report.get("outcome", "unknown"))
            outcomes[outcome] += 1
            if outcome in ("unknown", "error"):
                pending += 1
        logger.info("daemon stopped: %s (pending=%d)", dict(outcomes),
                    pending)
        # CN1/CN-05.04: a nonzero exit reports unresolved ownership —
        # the Core's guardian/containment retains the resources and the
        # next lifecycle call converges them; we never claim success.
        return 1 if pending else 0

    # -- transports ------------------------------------------------------------

    def _start_transports(self) -> None:
        state = self.store.load()
        for server_id, profile in state.servers.items():
            if server_id in self.transports:
                continue
            transport = NXLTransport(
                server_id=server_id,
                executor_id=state.connector_id,
                link_url=profile.link_url_override or
                NexusHTTPClient(profile.base_url).link_url(
                    state.connector_id),
                ticket_provider=self._ticket_provider(server_id),
                on_operation=self._on_remote_operation,
                on_approval=self._on_remote_approval,
                on_reconcile=self._on_reconcile)
            for binding in state.bindings:
                if binding.server_id == server_id:
                    identity = state.identity_for(server_id,
                                                  binding.agent_id)
                    epoch = identity.credential_epoch if identity else 1
                    transport.add_lane(
                        binding.binding_id, binding.agent_id,
                        self._lane_ticket_provider(binding), 
                        credential_epoch=epoch,
                        authorization_revision=binding.authorization_revision)
            transport.start()
            self.transports[server_id] = transport

    async def reload_state(self) -> None:
        """Pick up identities/bindings changed by CLI-side commands."""
        state = self.store.load()
        self._start_transports()
        for server_id, transport in list(self.transports.items()):
            if server_id not in state.servers:
                await transport.stop()
                del self.transports[server_id]
                continue
            wanted = {b.binding_id for b in state.bindings
                      if b.server_id == server_id}
            for binding_id in list(transport.stats.lanes):
                if binding_id not in wanted:
                    transport.remove_lane(binding_id)
            for binding in state.bindings:
                if binding.server_id == server_id and \
                        binding.binding_id not in transport.stats.lanes:
                    identity = state.identity_for(server_id,
                                                  binding.agent_id)
                    transport.add_lane(
                        binding.binding_id, binding.agent_id,
                        self._lane_ticket_provider(binding),
                        credential_epoch=(identity.credential_epoch
                                          if identity else 1),
                        authorization_revision=binding.authorization_revision)

    def _transport_for(self, server_id: str) -> NXLTransport | None:
        return self.transports.get(server_id)

    def _generation_for(self, server_id: str) -> int:
        transport = self.transports.get(server_id)
        return transport.stats.connection_generation if transport else 1

    def _ticket_provider(self, server_id: str):
        async def provide() -> str:
            state = self.store.load()
            profile = state.servers.get(server_id)
            identity = next((i for i in state.identities
                             if i.server_id == server_id), None)
            if profile is None or identity is None:
                raise ConnectorError("AGENT_AUTH_REQUIRED", "ticket",
                                     "no imported identity for this server")
            key = self.vault.resolve(identity.secret_handle)
            async with NexusHTTPClient(profile.base_url) as http:
                transport = self.transports.get(server_id)
                binding_id = (next(iter(transport.stats.lanes), None)
                              if transport else None)
                if binding_id is None:
                    raise ConnectorError("BINDING_NOT_AUTHORIZED", "ticket",
                                         "no binding lane on this server")
                ticket, _expires = await http.binding_ticket(key, binding_id)
            return ticket
        return provide

    def _lane_ticket_provider(self, binding):
        async def provide() -> str:
            state = self.store.load()
            profile = state.servers.get(binding.server_id)
            identity = state.identity_for(binding.server_id,
                                          binding.agent_id)
            if profile is None or identity is None:
                raise ConnectorError("AGENT_AUTH_REQUIRED", "ticket",
                                     "identity missing for lane")
            key = self.vault.resolve(identity.secret_handle)
            async with NexusHTTPClient(profile.base_url) as http:
                ticket, _expires = await http.binding_ticket(
                    key, binding.binding_id)
            return ticket
        return provide

    # -- remote operations over WSS ------------------------------------------

    async def _on_remote_operation(self, frame: dict[str, object]
                                   ) -> dict[str, object] | None:
        """CN1/A11: the full NXL envelope reaches the manager.

        IDs, intent hash, expected_turn_id and payload travel intact;
        remote close uses the SERVER's operation id and returns ITS
        receipt (no second resolution, no locally-minted id). CoreError
        is translated by the transport into a valid error frame — it
        never kills the receiver.
        """
        action = str(frame.get("action", ""))
        operation_id = str(frame.get("operation_id", ""))
        session_id = str(frame.get("session_id", ""))
        binding_id = str(frame.get("binding_id", ""))
        expected_turn_id = frame.get("expected_turn_id")
        payload = frame.get("payload", {})
        text = str(payload.get("text", "")) if isinstance(payload,
                                                            dict) else ""
        if action in ("turn.submit",):
            expected = submit_frame_intent_hash(frame)
            if expected != frame.get("intent_hash"):
                raise ConnectorError("OPERATION_CONFLICT", "wss_submit",
                                     "intent hash mismatch")
        if action == "turn.submit":
            receipt = await self.runtimes.submit_remote(
                session_id, operation_id, text, binding_id,
                expected_turn_id=expected_turn_id)
            return receipt_nxl_frame(
                receipt, server_id=str(frame["server_id"]),
                executor_id=str(frame["executor_id"]))
        if action == "turn.steer":
            receipt = await self.runtimes.steer_remote(
                session_id, operation_id, text, binding_id,
                expected_turn_id=expected_turn_id)
            return receipt_nxl_frame(
                receipt, server_id=str(frame["server_id"]),
                executor_id=str(frame["executor_id"]))
        if action == "turn.interrupt":
            receipt = await self.runtimes.interrupt_remote(
                session_id, operation_id, binding_id,
                expected_turn_id=expected_turn_id)
            return receipt_nxl_frame(
                receipt, server_id=str(frame["server_id"]),
                executor_id=str(frame["executor_id"]))
        if action == "runtime.close":
            receipt = await self.runtimes.close_remote(
                session_id, operation_id, binding_id)
            return receipt_nxl_frame(
                receipt, server_id=str(frame["server_id"]),
                executor_id=str(frame["executor_id"]))
        raise ConnectorError("CAPABILITY_UNSUPPORTED", "wss_operation",
                             f"unsupported action {action!r}",
                             action="Remote open requires the full managed "
                                    "wiring and stays unavailable until "
                                    "the vertical integration gate.")

    async def _on_remote_approval(self, frame: dict[str, object]
                                  ) -> dict[str, object] | None:
        """Surface a pending HITL request; the authority stays server-side.

        The connector never auto-approves with the agent's own key
        (TC-29): the request is queued for the CLI/TUI operator and the
        decision is forwarded through the Server's authorized mechanism.
        """
        request_id = str(frame.get("request_id", ""))
        self._approvals[request_id] = {
            "request_id": request_id,
            "binding_id": frame.get("binding_id"),
            "session_id": frame.get("session_id"),
            "kind": frame.get("kind"),
            "proposal": redact_mapping(frame.get("proposal", {})),
            "received_at": _now(),
            "source": "server",
        }
        return None

    async def _on_reconcile(self, frame: dict[str, object]
                            ) -> dict[str, object] | None:
        """CN1/A09: the ORIGINATING namespace produces the report;
        receipts/snapshots are the typed projections the real NXL codec
        accepts (validated by encode before use in tests).
        """
        server_id = str(frame.get("server_id", ""))
        executor_id = str(frame.get("executor_id", ""))
        report = await self.runtimes.reconcile(
            operation_ids=frame.get("operation_ids", ()),
            session_ids=frame.get("session_ids", ()),
            server_id=server_id, executor_id=executor_id)
        return {
            "protocol_major": PROTOCOL_MAJOR,
            "contract_revision": CONTRACT_REVISION,
            "type": "reconcile.report",
            "server_id": server_id or self._default_namespace()[0],
            "executor_id": executor_id or self._default_namespace()[1],
            "receipts": report.get("receipts", []),
            "snapshots": report.get("snapshots", []),
        }

    def _default_namespace(self) -> tuple[str, str]:
        state = self.store.load()
        if state.connector_id:
            return (next(iter(state.servers), ""), state.connector_id)
        return ("", "")

    async def _core_ack(self, server_id: str, session_id: str, epoch: str,
                        sequence: int) -> None:
        state = self.store.load()
        binding = next((b for b in state.bindings
                        if b.server_id == server_id), None)
        if binding is None:
            return
        runtime = (self.host.get(binding.binding_id, server_id)
                   or next((r for k, r in self.host._runtimes.items()
                            if k.server_id == server_id), None))
        if runtime is None:
            return
        from nexus_connector_core import EventCursor
        await runtime.acknowledge_events(
            EventCursor(server_id, binding.executor_id, session_id, epoch),
            sequence)

    async def _journal_batch(self, server_id: str, session_id: str,
                             epoch: str, after_sequence: int, limit: int):
        """Bounded journal replay for the EventBridge (CN1/A13)."""
        state = self.store.load()
        binding = next((b for b in state.bindings
                        if b.server_id == server_id), None)
        if binding is None:
            return []
        runtime = (self.host.get(binding.binding_id, server_id)
                   or next((r for k, r in self.host._runtimes.items()
                            if k.server_id == server_id), None))
        if runtime is None:
            runtime = await self.host.build(
                binding, environment=_no_environment)
        from nexus_connector_core import EventCursor
        batch = []
        cursor = EventCursor(server_id, binding.executor_id, session_id,
                             epoch, after_sequence)
        async for event in runtime.events(cursor):
            batch.append(event)
            if len(batch) >= limit:
                break
        return batch

    # -- IPC dispatch -----------------------------------------------------------

    async def dispatch(self, request) -> AsyncIterator[dict[str, object]]:
        op = request.op
        params = request.params
        if op == "ping":
            yield response_ok(request.seq, {
                "pong": True, "version": __version__,
                "pid": _current_identity().pid,
                "uptime_seconds": round(time.time() - self.started_at, 1),
                "draining": self._draining})
        elif op == "status":
            state = self.store.load()
            yield response_ok(request.seq, {
                "daemon": {
                    "version": __version__,
                    "pid": _current_identity().pid,
                    "started_at": self.started_at,
                    "draining": self._draining,
                    "ipc_connections": self.ipc.active_connections,
                },
                "transports": {
                    server_id: transport.stats.to_json()
                    for server_id, transport in self.transports.items()},
                "runtimes": self.runtimes.status(),
                "servers": [_profile_json(profile) for profile
                            in state.servers.values()],
                "bindings": len(state.bindings),
                "identities": len(state.identities),
            })
        elif op == "runtime.start":
            yield response_ok(request.seq, await self.runtimes.start(
                alias=str(params.get("alias", "")),
                project=Path(str(params["project"]))
                if params.get("project") else None,
                harness=params.get("harness"),
                new_session=bool(params.get("new_session", False)),
                text=params.get("text")))
        elif op == "runtime.status":
            yield response_ok(request.seq, {"sessions":
                                            self.runtimes.status()})
        elif op == "runtime.inspect":
            yield response_ok(request.seq, await self.runtimes.inspect(
                str(params["session_id"])))
        elif op == "runtime.interrupt":
            yield response_ok(request.seq, await self.runtimes.interrupt(
                str(params["session_id"])))
        elif op == "runtime.stop":
            yield response_ok(request.seq, await self.runtimes.stop(
                str(params["session_id"])))
        elif op == "runtime.submit":
            yield response_ok(request.seq, await self.runtimes.submit(
                str(params["session_id"]), str(params["text"])))
        elif op == "runtime.logs":
            async for record in self.runtimes.logs(
                    str(params["session_id"]),
                    follow=bool(params.get("follow", False))):
                yield response_stream(request.seq, record)
            yield response_ok(request.seq, {"closed": True})
        elif op == "approvals.pending":
            yield response_ok(request.seq, {"approvals": list(
                self._approvals.values())})
        elif op == "approvals.decide":
            decision = await self._decide_approval(params)
            yield response_ok(request.seq, decision)
        elif op == "reconcile":
            yield response_ok(request.seq, await self.runtimes.reconcile(
                operation_ids=params.get("operation_ids", ()),
                session_ids=params.get("session_ids", ())))
        elif op == "state.reload":
            await self.reload_state()
            yield response_ok(request.seq, {"reloaded": True})
        elif op == "shutdown":
            self.request_stop()
            yield response_ok(request.seq, {"stopping": True})
        else:
            yield response_error(request.seq, ConnectorError(
                "CAPABILITY_UNSUPPORTED", "ipc",
                f"unknown op {op!r}"))

    async def _decide_approval(self, params: dict[str, object]
                               ) -> dict[str, object]:
        request_id = str(params.get("request_id", ""))
        decision = str(params.get("decision", ""))
        pending = self._approvals.pop(request_id, None)
        if pending is None:
            raise ConnectorError("VALIDATION_ERROR", "approval",
                                 "unknown or already answered request")
        if decision not in ("approve", "deny"):
            raise ConnectorError("VALIDATION_ERROR", "approval",
                                 "decision must be approve or deny")
        server_id = str(pending.get("binding_id", "")).split(":")[0]
        state = self.store.load()
        binding = next((b for b in state.bindings
                        if b.binding_id == pending.get("binding_id")), None)
        if binding is None:
            raise ConnectorError("VALIDATION_ERROR", "approval",
                                 "binding for this request is gone")
        profile = state.servers.get(binding.server_id)
        identity = state.identity_for(binding.server_id, binding.agent_id)
        if profile is None or identity is None:
            raise ConnectorError("AGENT_AUTH_REQUIRED", "approval")
        key = self.vault.resolve(identity.secret_handle)
        async with NexusHTTPClient(profile.base_url) as http:
            applied = await http.approval_decision(
                key, request_id=request_id, decision=decision,
                cas_token=str(params.get("cas_token", "")),
                response=params.get("response") if isinstance(
                    params.get("response"), dict) else None)
        return {"request_id": request_id, "decision": decision,
                "applied": applied,
                "authority": "server-side CAS; the agent key only "
                             "transports the operator's decision"}


def _error_frame(error: ConnectorError, frame: dict[str, object]
                 ) -> dict[str, object]:
    """Map a connector failure onto a schema-valid NXL error frame.

    The wire ``stage`` vocabulary is the operation-stage enum: a possible
    effect can never be reported as a clean FAILED, and a proven
    pre-effect refusal is FAILED with retry_safe. The component stage
    travels inside the corrective action for diagnostics.
    """
    wire_stage = "OUTCOME_UNKNOWN" if error.possible_effect else "FAILED"
    action = error.action or ""
    if error.stage and error.stage != wire_stage:
        action = f"[stage: {error.stage}] {action}".strip()[:512]
    payload = {
        "protocol_major": PROTOCOL_MAJOR,
        "contract_revision": CONTRACT_REVISION,
        "type": "error",
        "server_id": frame.get("server_id", ""),
        "executor_id": frame.get("executor_id", ""),
        "code": error.code,
        "stage": wire_stage,
        "possible_effect": error.possible_effect,
        "retry_safe": error.retry_safe,
        "operation_id": error.operation_id or frame.get("operation_id"),
    }
    if action:
        payload["corrective_action"] = action
    return payload


async def _no_environment(prepared):
    """Environment for composition-only paths (reconcile/replay): the
    factory that would consume it is never invoked, so nothing here
    reaches a child process."""
    return {}


def _current_identity():
    from ..platform.sysinfo import current_process_identity
    return current_process_identity()


def _profile_json(profile) -> dict[str, object]:
    from dataclasses import asdict
    return asdict(profile)


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
