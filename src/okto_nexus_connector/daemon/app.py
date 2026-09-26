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
    """Batches events per session; Core ACK only after Server ACK."""

    def __init__(self, ack_source, core_ack):
        self._ack_source = ack_source  # transport provider by server_id
        self._core_ack = core_ack      # (session_key, epoch, seq) -> await
        self._pending: dict[tuple[str, str, str], list[RuntimeEvent]] = \
            defaultdict(list)
        self._tasks: dict[str, asyncio.Task] = {}

    def publisher_for(self, server_id: str):
        def publish(event: RuntimeEvent) -> None:
            key = (event.session_id, event.stream_epoch)
            self._pending[(server_id,) + key].append(event)
            task_key = f"{server_id}:{event.session_id}:{event.stream_epoch}"
            if task_key not in self._tasks or self._tasks[task_key].done():
                self._tasks[task_key] = asyncio.create_task(
                    self._flush(server_id, event.session_id,
                                event.stream_epoch))
        return publish

    async def _flush(self, server_id: str, session_id: str, epoch: str
                     ) -> None:
        key = (server_id, session_id, epoch)
        while self._pending.get(key):
            await asyncio.sleep(0.05)  # micro-batch window
            batch = self._pending.get(key, [])[:128]
            if not batch:
                return
            transport = self._ack_source(server_id)
            if transport is None or not transport.online:
                await asyncio.sleep(1.0)
                continue
            sent = await transport.send_events(list(batch))
            if not sent:
                await asyncio.sleep(1.0)
                continue
            watermark = await transport.wait_event_ack(
                session_id, epoch, timeout=30.0)
            if watermark is None:
                # Server did not ACK durably; keep journal un-acked so a
                # reconnect replays (dedup is server-side).
                continue
            await self._core_ack(server_id, session_id, epoch, watermark)
            for event in batch:
                if event.sequence <= watermark and \
                        event in self._pending.get(key, []):
                    self._pending[key].remove(event)
            if not self._pending.get(key):
                return

    async def drain(self) -> None:
        for task in list(self._tasks.values()):
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()
        self._pending.clear()


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
        self.bridge = EventBridge(self._transport_for, self._core_ack)
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
        self._draining = True
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
        for report in reports:
            outcomes[str(report.get("outcome", "unknown"))] += 1
        logger.info("daemon stopped: %s", dict(outcomes))
        return 0

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
        action = str(frame.get("action", ""))
        operation_id = str(frame.get("operation_id", ""))
        session_id = str(frame.get("session_id", ""))
        binding_id = str(frame.get("binding_id", ""))
        try:
            if action == "turn.submit":
                expected = submit_frame_intent_hash(frame)
                if expected != frame.get("intent_hash"):
                    raise ConnectorError("OPERATION_CONFLICT", "wss_submit",
                                         "intent hash mismatch")
                text = str(frame.get("payload", {}).get("text", ""))
                receipt = await self.runtimes.submit_remote(
                    session_id, operation_id, text, binding_id)
                return receipt_nxl_frame(
                    receipt, server_id=str(frame["server_id"]),
                    executor_id=str(frame["executor_id"]))
            if action == "turn.interrupt":
                receipt = await self.runtimes.interrupt_remote(
                    session_id, operation_id, binding_id)
                return receipt_nxl_frame(
                    receipt, server_id=str(frame["server_id"]),
                    executor_id=str(frame["executor_id"]))
            if action == "runtime.close":
                await self.runtimes.stop(session_id)
                return None
            raise ConnectorError("CAPABILITY_UNSUPPORTED", "wss_operation",
                                 f"unsupported action {action!r}")
        except ConnectorError as error:
            return _error_frame(error, frame)

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
        report = await self.runtimes.reconcile(
            operation_ids=frame.get("operation_ids", ()),
            session_ids=frame.get("session_ids", ()))
        return {
            "protocol_major": PROTOCOL_MAJOR,
            "contract_revision": CONTRACT_REVISION,
            "type": "reconcile.report",
            "server_id": frame.get("server_id"),
            "executor_id": frame.get("executor_id"),
            "receipts": report.get("receipts", []),
            "snapshots": report.get("snapshots", []),
        }

    async def _core_ack(self, server_id: str, session_id: str, epoch: str,
                        sequence: int) -> None:
        state = self.store.load()
        binding = next((b for b in state.bindings
                        if b.server_id == server_id), None)
        if binding is None:
            return
        runtime = self.host.get(binding.binding_id)
        if runtime is None:
            return
        from nexus_connector_core import EventCursor
        await runtime.acknowledge_events(
            EventCursor(server_id, binding.executor_id, session_id, epoch),
            sequence)

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


def _current_identity():
    from ..platform.sysinfo import current_process_identity
    return current_process_identity()


def _profile_json(profile) -> dict[str, object]:
    from dataclasses import asdict
    return asdict(profile)


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
