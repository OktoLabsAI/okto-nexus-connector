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
    """Journal-sourced event publication (CN1/A13 + CN2/N03).

    CN2/N03 corrections:

    * The batch reader consumes the PUBLIC Journal port's PAGED,
      FINITE ``events()`` iterator — one persisted event returns
      immediately; zero means end-of-snapshot, never a wait for future
      data (b04).
    * Cursors are separated: the flusher always resumes after the last
      VALIDATED durable ACK, not after the last sent batch — a batch
      whose ACK never arrived is REPLAYED on the next round (b05).
    * ACKs are validated by the transport (namespace/stream/monotonic/
      never-future) before the Core-side acknowledge fires (b05b/b05c);
      the bridge itself only consumes already-validated watermarks.
    * RAM holds cursors and one bounded batch per stream; the journal
      is the replay source.
    """

    _MAX_INFLIGHT_PER_STREAM = 128
    _WAKE_COALESCE_SECONDS = 0.02
    _ACK_RETRY_SECONDS = 5.0

    def __init__(self, ack_source, core_ack, journal_page):
        self._ack_source = ack_source      # transport provider by server
        self._core_ack = core_ack          # validated-ack applier
        self._journal_page = journal_page  # (server, session, epoch,
        #                                 #  after_sequence, limit) ->
        #                                 # finite page of events
        # CN2/N03.2: ONLY the validated-ACK cursor drives replay.
        self._acked_through: dict[tuple[str, str, str], int] = {}
        self._wakeups: dict[tuple[str, str, str], asyncio.Event] = {}
        self._tasks: dict[tuple[str, str, str], asyncio.Task] = {}
        # CN4-02.02: a validated watermark whose CORE application
        # failed stays pending here — the retry re-applies the SAME
        # confirmation without re-reading/re-sending the batch.
        self._pending_core_ack: dict[tuple[str, str, str], int] = {}

    def ack_arrived(self, server_id: str, session_id: str,
                    epoch: str) -> None:
        """CN4-02.01: a NEW validated watermark wakes the parked
        publisher of that stream — a batch-2 wait is satisfied by
        ACK2's arrival, never by re-sending under an old watermark."""
        wake = self._wakeups.get((server_id, session_id, epoch))
        if wake is not None:
            wake.set()
            key = (server_id, session_id, epoch)
            if key not in self._tasks or self._tasks[key].done():
                self._tasks[key] = asyncio.create_task(
                    self._flush_loop(*key))

    def publisher_for(self, server_id: str):
        def publish(event: RuntimeEvent) -> None:
            key = (server_id, event.session_id, event.stream_epoch)
            wake = self._wakeups.get(key)
            if wake is None:
                wake = self._wakeups[key] = asyncio.Event()
            wake.set()
            if key not in self._tasks or self._tasks[key].done():
                self._tasks[key] = asyncio.create_task(
                    self._flush_loop(*key))
        return publish

    def transport_online_again(self, server_id: str) -> None:
        """CN2/N03.2: reconnect wakes every stream of the server —
        pending journal records drain without new native events."""
        for key in list(self._wakeups):
            if key[0] == server_id:
                self._wakeups[key].set()

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
                # CN4-02.02: FIRST drain a confirmation whose Core
                # application failed — same evidence, no re-send.
                pending_watermark = self._pending_core_ack.get(key)
                if pending_watermark is not None:
                    try:
                        await self._core_ack(server_id, session_id,
                                             epoch, pending_watermark)
                    except Exception:
                        self._wake_retry_later(key)
                        break
                    self._acked_through[key] = max(
                        self._acked_through.get(key, 0), pending_watermark)
                    del self._pending_core_ack[key]
                # b05: resume after the VALIDATED ACK, never after sent.
                after = self._acked_through.get(key, 0)
                batch = await self._journal_page(
                    server_id, session_id, epoch, after,
                    self._MAX_INFLIGHT_PER_STREAM)
                if not batch:
                    break  # b04: end of snapshot — no waiting for future
                sent = await transport.send_events(list(batch))
                if not sent:
                    break  # backpressure: retry without losing events
                # CN4-02.01: the wait targets THE BATCH'S OWN last
                # sequence — an old watermark (ACK1) can never satisfy
                # a wait for batch2; the target is fixed for this
                # attempt, never re-derived from a mutable cursor.
                watermark = await transport.wait_event_ack(
                    session_id, epoch, timeout=30.0,
                    target=batch[-1].sequence)
                if watermark is None:
                    # ACK2 absent: EXACTLY ONE attempt was made while
                    # it is missing (no tight re-send loop); the parked
                    # obligation wakes on ACK2's arrival (ack_arrived),
                    # a reconnect or a new event.
                    break
                try:
                    await self._core_ack(server_id, session_id, epoch,
                                         watermark)
                except Exception:
                    # CN4-02.02/ACN4-21: remote progress exists but the
                    # Core application failed — keep the obligation and
                    # retry the SAME confirmation later (bounded),
                    # never the agent's task.
                    self._pending_core_ack[key] = watermark
                    self._wake_retry_later(key)
                    break
                self._acked_through[key] = watermark
                if batch[-1].sequence > watermark:
                    # Partial contiguous ack: replay the remainder.
                    continue
                if len(batch) < self._MAX_INFLIGHT_PER_STREAM:
                    break

    def _wake_retry_later(self, key: tuple[str, str, str]) -> None:
        """CN4-02.02: one coalesced bounded retry per obligation."""
        wake = self._wakeups.get(key)
        if wake is None:
            return
        loop = asyncio.get_event_loop()
        loop.call_later(self._ACK_RETRY_SECONDS, wake.set)

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
                                  self._journal_page)
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
        # CN3/G02: approvals keyed by (server_id, request_id); CN4-01
        # keeps a bounded terminal history so repeats consult the same
        # outcome instead of re-deciding.
        self._approvals: dict[tuple[str, str], dict[str, object]] = {}
        self._approval_history: dict[tuple[str, str], dict] = {}
        self._approval_channel: str | None = None

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

    def _wake_streams_when_online(self, server_id: str) -> None:
        """CN2/N03.2: when the transport becomes READY again, pending
        streams resume from their ACKed cursors without new events."""
        async def _watch():
            transport = self.transports.get(server_id)
            if transport is None:
                return
            was_online = False
            while not transport.stats.state == "stopped":
                online = transport.online
                if online and not was_online:
                    self.bridge.transport_online_again(server_id)
                was_online = online
                await asyncio.sleep(0.5)
        asyncio.create_task(_watch(), name=f"wake-{server_id}")

    def _start_transports(self) -> None:
        state = self.store.load()
        for server_id, profile in state.servers.items():
            if server_id in self.transports:
                continue
            from ..transport.wss_client import validate_link_url
            link_url = (profile.link_url_override or
                        NexusHTTPClient(profile.base_url).link_url(
                            state.connector_id))
            # CN2/N08 (b12): WSS origin validated BEFORE any ticket
            # fetch or credential send — overrides get no bypass.
            validate_link_url(link_url)
            transport = NXLTransport(
                server_id=server_id,
                executor_id=state.connector_id,
                link_url=link_url,
                ticket_provider=self._ticket_provider(server_id),
                on_operation=self._on_remote_operation,
                on_approval=self._on_remote_approval,
                on_reconcile=self._on_reconcile,
                ack_observer=self._on_ack_arrived)
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
            self._wake_streams_when_online(server_id)

    async def reload_state(self) -> None:
        """Pick up identities/bindings changed by CLI-side commands.

        CN4-05.01: lanes are DIFFED against the persisted credentials —
        a validated credential_epoch/authorization_revision change
        rotates the EXISTING lane through add_lane (fence + new provider
        + reattach); a true no-op touches nothing (no epoch bump,
        ACN4-31) and other servers' lanes stay stable.
        """
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
                if binding.server_id != server_id:
                    continue
                identity = state.identity_for(server_id,
                                              binding.agent_id)
                credential_epoch = (identity.credential_epoch
                                    if identity else 1)
                live = transport.lane_info(binding.binding_id)
                if live is None:
                    transport.add_lane(
                        binding.binding_id, binding.agent_id,
                        self._lane_ticket_provider(binding),
                        credential_epoch=credential_epoch,
                        authorization_revision=binding.authorization_revision)
                elif (int(live["credential_epoch"]) != credential_epoch
                      or int(live["authorization_revision"]) !=
                      binding.authorization_revision):
                    # CN4-05.01: persisted rotation applied on the REAL
                    # reload path — the old proof is fenced before any
                    # I/O and the CURRENT provider is installed.
                    transport.add_lane(
                        binding.binding_id, binding.agent_id,
                        self._lane_ticket_provider(binding),
                        credential_epoch=credential_epoch,
                        authorization_revision=binding.authorization_revision)

    def _transport_for(self, server_id: str) -> NXLTransport | None:
        return self.transports.get(server_id)

    def _generation_for(self, server_id: str) -> int:
        transport = self.transports.get(server_id)
        return transport.stats.connection_generation if transport else 1

    def _ticket_provider(self, server_id: str):
        async def provide() -> str:
            # CN2/N07 (b11): pick ONE eligible binding FIRST, then use
            # exactly ITS agent's credential — never the first identity
            # of the Server independently.
            state = self.store.load()
            profile = state.servers.get(server_id)
            transport = self.transports.get(server_id)
            binding_id = (next(iter(transport.stats.lanes), None)
                          if transport else None)
            if binding_id is None:
                binding_id = next(
                    (b.binding_id for b in state.bindings
                     if b.server_id == server_id), None)
            if binding_id is None:
                raise ConnectorError("BINDING_NOT_AUTHORIZED", "ticket",
                                     "no binding lane on this server")
            binding = next((b for b in state.bindings
                             if b.server_id == server_id
                             and b.binding_id == binding_id), None)
            identity = (state.identity_for(server_id, binding.agent_id)
                        if binding is not None else None)
            if profile is None or identity is None:
                raise ConnectorError("AGENT_AUTH_REQUIRED", "ticket",
                                     "the selected binding has no imported "
                                     "identity for its agent")
            key = self.vault.resolve(identity.secret_handle)
            async with NexusHTTPClient(profile.base_url) as http:
                ticket, expires_in = await http.binding_ticket(
                    key, binding_id)
            return ticket, expires_in
        return provide

    def _on_ack_arrived(self, session_id: str, epoch: str,
                        _watermark: int) -> None:
        """CN4-02: a validated watermark wakes the matching publisher
        for EVERY transport of this daemon (the observer is bound per
        transport; the bridge keys streams by server already)."""
        for server_id, transport in list(self.transports.items()):
            if transport.written_through(session_id, epoch) >= \
                    _watermark:
                self.bridge.ack_arrived(server_id, session_id, epoch)
                return

    def _lane_ticket_provider(self, binding):
        async def provide() -> tuple[str, float]:
            state = self.store.load()
            profile = state.servers.get(binding.server_id)
            identity = state.identity_for(binding.server_id,
                                          binding.agent_id)
            if profile is None or identity is None:
                raise ConnectorError("AGENT_AUTH_REQUIRED", "ticket",
                                     "identity missing for lane")
            key = self.vault.resolve(identity.secret_handle)
            async with NexusHTTPClient(profile.base_url) as http:
                ticket, expires_in = await http.binding_ticket(
                    key, binding.binding_id)
            # CN4-05.03: the lane observes the CONTRACT'S expiry — the
            # transport records the local monotonic deadline (single-
            # flight renewal via attach); it never extends the runtime
            # lease by itself.
            return ticket, expires_in
        return provide

    # -- remote operations over WSS ------------------------------------------

    async def _on_remote_operation(self, operation) -> dict | None:
        """CN2/N01: the receiver's ValidatedOperation DTO reaches the
        manager whole — namespace, grant reference and expected turn
        travel with it; sessions resolve ONLY in the wire's namespace.
        CoreError is translated by the transport into a valid error
        frame; it never kills the receiver.
        """
        action = operation.action
        if action == "turn.submit":
            receipt = await self.runtimes.submit_remote(operation)
            return receipt_nxl_frame(
                receipt, server_id=operation.server_id,
                executor_id=operation.executor_id)
        if action == "turn.steer":
            receipt = await self.runtimes.steer_remote(operation)
            return receipt_nxl_frame(
                receipt, server_id=operation.server_id,
                executor_id=operation.executor_id)
        if action == "turn.interrupt":
            receipt = await self.runtimes.interrupt_remote(operation)
            return receipt_nxl_frame(
                receipt, server_id=operation.server_id,
                executor_id=operation.executor_id)
        if action == "runtime.close":
            receipt = await self.runtimes.close_remote(operation)
            return receipt_nxl_frame(
                receipt, server_id=operation.server_id,
                executor_id=operation.executor_id)
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
        # CN3-01.03/G02: approvals are keyed by the FULL authenticated
        # namespace — a foreign frame never reaches this point (the
        # transport validates), and same-text request ids of two
        # Servers never collide.
        request_id = str(frame.get("request_id", ""))
        server_id = str(frame.get("server_id", self._approval_channel or
                                  ""))
        self._approvals[(server_id, request_id)] = {
            "request_id": request_id,
            "server_id": server_id,
            "binding_id": frame.get("binding_id"),
            "agent_id": frame.get("agent_id"),
            "workspace_id": frame.get("workspace_id"),
            "session_id": frame.get("session_id"),
            "kind": frame.get("kind"),
            "proposal": redact_mapping(frame.get("proposal", {})),
            "received_at": _now(),
            "source": "server",
            "phase": "pending",  # CN4-01 state machine starts here
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
            # CN2/N03 + CN3-04: acknowledge through the PUBLIC journal
            # port when no live runtime exists (history-only
            # namespaces); the journal is recovered on demand.
            journal = await self.host.ensure_history_journal()
            from nexus_connector_core import EventCursor
            await journal.acknowledge_events(
                EventCursor(server_id, binding.executor_id, session_id,
                            epoch), sequence)
            return
        from nexus_connector_core import EventCursor
        await runtime.acknowledge_events(
            EventCursor(server_id, binding.executor_id, session_id, epoch),
            sequence)

    async def _journal_page(self, server_id: str, session_id: str,
                             epoch: str, after_sequence: int, limit: int):
        """CN2/N03.1 (b04): a FINITE page from the PUBLIC Journal port.

        The journal's own ``events()`` is paged and terminates at the
        end of the persisted snapshot — one event returns immediately;
        zero means no more data now, never a wait for future events.
        No runtime handle and no binary are required.
        """
        # CN3-04: a cold host OPENS the journal before reading —
        # unopened is state to recover, not an empty snapshot.
        journal = await self.host.ensure_history_journal()
        from nexus_connector_core import EventCursor
        batch = []
        cursor = EventCursor(server_id, self._executor_for(server_id),
                             session_id, epoch, after_sequence)
        async for event in journal.events(cursor):
            batch.append(event)
            if len(batch) >= limit:
                break
        return batch

    def _executor_for(self, server_id: str) -> str:
        state = self.store.load()
        for binding in state.bindings:
            if binding.server_id == server_id:
                return binding.executor_id
        return state.connector_id

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
        elif op == "availability.snapshot":
            # CN4-04.01/G01: the daemon publishes the versioned technical
            # availability snapshot through its real API — over the FULL
            # Core candidates behind the inventory (never re-created
            # from executable paths, which would drop the Pi pair's
            # CLI identity/version/build), and via the SAME service
            # function the CLI uses.
            from ..services.discovery_service import (
                availability_snapshot, inventory_candidates,
            )
            candidates = await inventory_candidates()
            yield response_ok(request.seq, availability_snapshot(candidates))
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
        """CN4-01: the decision flows Server-authority → Core effect.

        Full key to the last resource (ApprovalKey = server/request,
        resolved to binding by server+executor+binding_id with agent/
        workspace compared — never by short binding_id alone and never
        via split()); the FIRST external operation of the permissive
        flow is the CANONICAL Server decision; only a confirmed result
        authorizes the Core application, with the vocabulary translated
        by the Core's public contract (approve→accept, deny→decline).
        Phases: pending → server_pending → server_confirmed →
        native_pending → applied (terminal records go to a bounded
        tombstone history so a repeat consults the same outcome).
        """
        request_id = str(params.get("request_id", ""))
        decision = str(params.get("decision", ""))
        server_hint = str(params.get("server_id", ""))
        if decision not in ("approve", "deny"):
            raise ConnectorError("VALIDATION_ERROR", "approval",
                                 "decision must be approve or deny")
        # CN4-01.01/ACN4-19: a short CLI choice resolves EXACTLY one
        # request — ambiguity is a typed error listing the servers,
        # with ZERO HTTP requests issued.
        matches = [key for key in list(self._approvals) +
                   list(self._approval_history)
                   if key[1] == request_id
                   and (not server_hint or key[0] == server_hint)]
        if not matches:
            raise ConnectorError("VALIDATION_ERROR", "approval",
                                 "unknown request")
        if len(matches) > 1:
            servers = ", ".join(sorted(key[0] for key in matches))
            raise ConnectorError(
                "VALIDATION_ERROR", "approval",
                f"request_id {request_id!r} is pending on multiple "
                f"servers ({servers}); specify server_id")
        approval_key = matches[0]
        history = self._approval_history.get(approval_key)
        if history is not None:
            # ACN4-16/20: a terminal attempt is idempotent — the repeat
            # consults the SAME recorded outcome, never re-decides.
            return dict(history["result"])
        pending = self._approvals.get(approval_key)
        if pending is None:
            raise ConnectorError("VALIDATION_ERROR", "approval",
                                 "unknown request")
        phase = str(pending.get("phase", "pending"))
        if phase in ("server_pending", "server_confirmed",
                     "native_pending"):
            # CN4-01.04/ACN4-15: one attempt owns the request; a second
            # concurrent operator gets an explicit conflict — never a
            # second canonical decision or native application.
            raise ConnectorError(
                "OPERATION_CONFLICT", "approval",
                f"a decision for this request is already in progress "
                f"(phase {phase})")
        if phase == "result_unknown":
            raise ConnectorError(
                "OUTCOME_UNKNOWN", "approval",
                "a previous decision attempt may have reached the "
                "Server; its outcome is being reconciled — do not "
                "re-send a new intent for the same work",
                possible_effect=True,
                action="Query the request by its id on the Server; the "
                       "record stays consultable here.")
        if decision not in ("approve", "deny"):
            raise ConnectorError("VALIDATION_ERROR", "approval",
                                 "decision must be approve or deny")
        # CN4-01.01: resolve the FULL key — the server comes from the
        # AUTHENTICATED request namespace (approval_key), never from
        # split(binding_id); the binding matches server + binding_id
        # and the frame's agent/workspace are compared when present.
        server_id = approval_key[0]
        state = self.store.load()
        binding = next(
            (b for b in state.bindings
             if b.server_id == server_id
             and b.binding_id == pending.get("binding_id")), None)
        if binding is None:
            raise ConnectorError("VALIDATION_ERROR", "approval",
                                 "binding for this request is gone")
        frame_agent = pending.get("agent_id")
        if isinstance(frame_agent, str) and frame_agent and \
                frame_agent != binding.agent_id:
            raise ConnectorError(
                "BINDING_NOT_AUTHORIZED", "approval",
                "request agent does not match the binding in this "
                "namespace")
        profile = state.servers.get(server_id)
        identity = state.identity_for(server_id, binding.agent_id)
        if profile is None or identity is None:
            raise ConnectorError("AGENT_AUTH_REQUIRED", "approval")
        agent_key = self.vault.resolve(identity.secret_handle)
        # CN4-01.02: FIRST EXTERNAL OPERATION = canonical Server
        # decision. The local operator proposal is intent, not
        # authority; nothing native may run before this confirms.
        pending["phase"] = "server_pending"
        try:
            async with NexusHTTPClient(profile.base_url) as http:
                applied = await http.approval_decision(
                    agent_key, request_id=request_id, decision=decision,
                    cas_token=str(params.get("cas_token", "")),
                    response=params.get("response") if isinstance(
                        params.get("response"), dict) else None)
        except ConnectorError as error:
            if getattr(error, "possible_effect", False):
                # CN4-01.05: a possible write is never undone and never
                # restored as a fresh pending invitation.
                pending["phase"] = "result_unknown"
                raise
            # Definitive refusal / not delivered: the Server did not
            # consume the decision; the request stays pending for a
            # corrected retry.
            pending["phase"] = "pending"
            raise
        if not applied:
            # 200-but-not-applied (conflict/already decided): the
            # Server's canonical answer exists — record it as the
            # outcome; NO native application is authorized.
            result = {"request_id": request_id, "decision": decision,
                      "applied": False, "native": None,
                      "authority": "server-side CAS answered; the Core "
                                   "was not touched"}
            self._record_terminal(approval_key, result)
            return result
        pending["phase"] = "server_confirmed"
        # CN4-01.03: translate the CONFIRMED decision through the Core's
        # public vocabulary and apply it only for a LIVE MANAGED
        # session of this namespace; an administrative (Server-side)
        # request ends at the CAS.
        native_result = None
        session_id = pending.get("session_id")
        managed = (isinstance(session_id, str) and session_id
                   and session_id in self.runtimes.session_ids())
        if managed:
            pending["phase"] = "native_pending"
            core_decision = ("accept" if decision == "approve"
                             else "decline")
            try:
                native_result = await self.runtimes.decide_native_approval(
                    session_id, f"op_appr_{request_id}",
                    request=pending.get("proposal", {})
                    if isinstance(pending.get("proposal"), dict) else {},
                    decision=core_decision,
                    kind=pending.get("kind"),
                    response=params.get("response") if isinstance(
                        params.get("response"), dict) else None)
            except ConnectorError as error:
                if getattr(error, "possible_effect", False):
                    pending["phase"] = "result_unknown"
                else:
                    # Pre-effect refusal of the authorized application:
                    # terminal and distinct — never back to pending.
                    pending["phase"] = "native_refused"
                    pending["native_error"] = error.code
                raise
        result = {"request_id": request_id, "decision": decision,
                  "applied": applied,
                  "native": native_result,
                  "authority": "canonical Server decision confirmed "
                               "before any native application"}
        self._record_terminal(approval_key, result)
        return result

    def _record_terminal(self, approval_key: tuple[str, str],
                         result: dict[str, object]) -> None:
        """CN4-01.04/ACN4-20: retire the request BY ITS OWN KEY (the
        agent secret never substitutes the dictionary key) and keep a
        bounded tombstone so repeats consult the same outcome."""
        self._approvals.pop(approval_key, None)
        self._approval_history[approval_key] = {
            "result": result, "at": _now()}
        while len(self._approval_history) > 128:
            self._approval_history.pop(next(iter(
                self._approval_history)))


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
