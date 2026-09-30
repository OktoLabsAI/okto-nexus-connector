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
from ..services.approval_state import PendingApproval
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
    #: CN5-04.02: progressive retry backoff (seconds, capped).
    _RETRY_BACKOFF = (0.25, 0.5, 1.0, 2.0, 5.0)

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
        # CN5-04: the bridge's OWN lifecycle (drain) is separate from
        # transport offline; owned retry timers per stream with
        # progressive backoff.
        self._closed = False
        self._retry_timers: dict[tuple[str, str, str], object] = {}
        self._retry_rounds: dict[tuple[str, str, str], int] = {}

    def _ensure_stream_task(self, key: tuple[str, str, str]) -> None:
        """CN5-04.01: exactly one publisher task per stream — created
        (or re-created after a transient death) by EVERY entry point:
        publish, ack_arrived, transport_online_again. A finished task's
        exception is retrieved ONCE for a redacted diagnostic."""
        if self._closed:
            return
        task = self._tasks.get(key)
        if task is not None and not task.done():
            return
        if task is not None and task.done() and not task.cancelled():
            exc = task.exception()
            if exc is not None:
                logger.warning(
                    "event publisher for %s died (%s); recreating",
                    key, redact_text(repr(exc)))
        wake = self._wakeups.get(key)
        if wake is None:
            wake = self._wakeups[key] = asyncio.Event()
        self._tasks[key] = asyncio.create_task(self._flush_loop(*key))

    def _schedule_stream_retry(self, key: tuple[str, str, str]) -> None:
        """CN5-04.02: ONE owned retry timer per stream with progressive
        backoff 0.25/0.5/1/2/5s (cap 5); reset on progress."""
        if self._closed:
            return
        if self._retry_timers.get(key) is not None:
            return  # coalesced: a timer is already armed
        delay = self._RETRY_BACKOFF[
            min(self._retry_rounds.get(key, 0),
                len(self._RETRY_BACKOFF) - 1)]
        loop = asyncio.get_event_loop()
        wake = self._wakeups.get(key)
        if wake is None:
            return

        def _fire():
            self._retry_timers.pop(key, None)
            if not self._closed:
                wake.set()

        self._retry_timers[key] = loop.call_later(delay, _fire)

    def ack_arrived(self, server_id: str, session_id: str,
                    epoch: str) -> None:
        """CN4-02.01: a NEW validated watermark wakes the parked
        publisher of that stream — a batch-2 wait is satisfied by
        ACK2's arrival, never by re-sending under an old watermark."""
        key = (server_id, session_id, epoch)
        self._ensure_stream_task(key)
        wake = self._wakeups.get(key)
        if wake is not None:
            wake.set()

    def publisher_for(self, server_id: str):
        def publish(event: RuntimeEvent) -> None:
            key = (server_id, event.session_id, event.stream_epoch)
            self._ensure_stream_task(key)
            wake = self._wakeups.get(key)
            if wake is None:
                wake = self._wakeups[key] = asyncio.Event()
            wake.set()
        return publish

    def transport_online_again(self, server_id: str) -> None:
        """CN2/N03.2 + CN5-04.01: reconnect wakes every stream of the
        server — pending journal records drain without new native
        events, and a stream whose publisher died on a TRANSIENT error
        is re-created HERE (the Event alone proves nothing)."""
        for key in list(self._wakeups):
            if key[0] == server_id:
                timer = self._retry_timers.pop(key, None)
                if timer is not None:
                    timer.cancel()  # reconnection anticipates the retry
                self._ensure_stream_task(key)
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
                        self._retry_rounds[key] = (
                            self._retry_rounds.get(key, 0) + 1)
                        self._schedule_stream_retry(key)
                        break
                    self._acked_through[key] = max(
                        self._acked_through.get(key, 0), pending_watermark)
                    del self._pending_core_ack[key]
                # b05: resume after the VALIDATED ACK, never after sent.
                after = self._acked_through.get(key, 0)
                # CN5-04.02: a TRANSIENT page read failure no longer
                # kills the publisher — cursors stay and ONE owned
                # retry is scheduled (ACN5-Q04/ACN5-30).
                try:
                    batch = await self._journal_page(
                        server_id, session_id, epoch, after,
                        self._MAX_INFLIGHT_PER_STREAM)
                except Exception as error:
                    logger.warning(
                        "journal page read failed for %s (%s); retrying",
                        key, redact_text(repr(error)))
                    self._retry_rounds[key] = (
                        self._retry_rounds.get(key, 0) + 1)
                    self._schedule_stream_retry(key)
                    break
                if not batch:
                    break  # b04: end of snapshot — no waiting for future
                try:
                    sent = await transport.send_events(list(batch))
                except Exception as error:
                    logger.warning(
                        "event send failed for %s (%s); retrying",
                        key, redact_text(repr(error)))
                    self._retry_rounds[key] = (
                        self._retry_rounds.get(key, 0) + 1)
                    self._schedule_stream_retry(key)
                    break
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
                    # CN4-02.02/ACN5-31: remote progress exists but the
                    # Core application failed — keep the obligation and
                    # retry the SAME confirmation later (owned,
                    # progressive), never the agent's task.
                    self._pending_core_ack[key] = watermark
                    self._retry_rounds[key] = (
                        self._retry_rounds.get(key, 0) + 1)
                    self._schedule_stream_retry(key)
                    break
                self._acked_through[key] = watermark
                self._retry_rounds[key] = 0  # progress resets backoff
                if batch[-1].sequence > watermark:
                    # Partial contiguous ack: replay the remainder.
                    continue
                if len(batch) < self._MAX_INFLIGHT_PER_STREAM:
                    break

        loop.call_later(self._ACK_RETRY_SECONDS, wake.set)

    async def drain(self) -> None:
        """CN5-04.03: drain closes the BRIDGE (its own lifecycle): the
        timers are cancelled, `_closed` forbids resurrection, and only
        the bridge-owned observers end — runtime operations and the
        journal are never touched."""
        self._closed = True
        for timer in list(self._retry_timers.values()):
            timer.cancel()
        self._retry_timers.clear()
        for task in list(self._tasks.values()):
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()
        self._wakeups.clear()
        self._retry_rounds.clear()


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
        self.r4_controls = {}
        self.r4_executions = {}
        self.ipc = IPCServer(self.dispatch)
        self.lock = InstanceLock(paths.pid_dir(root))
        self.started_at = time.time()
        self._stopped = asyncio.Event()
        self._draining = False
        # CN3/G02: approvals keyed by (server_id, request_id); CN4-01
        # keeps a bounded terminal history so repeats consult the same
        # outcome instead of re-deciding; CN5 stores PendingApproval
        # records (operational copy + redacted display projection).
        self._approvals: dict[tuple[str, str], PendingApproval] = {}
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
        # Startup owns HTTP/state producers. Join them before closing the
        # shared stores; cancellation of an IPC waiter cannot abandon them.
        for control in tuple(self.r4_controls.values()):
            await control.stop()
            if control.status()['recovery_required'] or control.cleanup_pending:
                reports.append({"outcome": "unknown", "source": "r4-startup",
                                "error": "The R4 executor requires recovery or connection cleanup."})
        self.r4_controls.clear()
        # R4 producers finish before the shared Core host/journal shuts down.
        # They must never be cancelled as if they were UI observers.
        for owner in self.r4_executions.values():
            await owner.stop()
            await owner.connection.close()
            if owner.failure is not None:
                reports.append({"outcome": "unknown", "source": "r4-execution",
                                "error": "The R4 connection requires reconciliation."})
        try:
            reports.extend(await self.runtimes.shutdown())
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

    def own_r4_connection(self, connection, *, candidate_provider, launch_provider=None,
                          publish_receipt, native_factory=None, response_resolver=None):
        """Adopt a negotiated authenticated connection into daemon ownership.

        The startup/credential owner supplies host-local composition ports.
        This method never imports a legacy binding or grants R3 authority.
        """
        from ..services.r4_execution import R4ExecutionOwner
        key = (connection.state.server_id, connection.state.executor_id)
        control = self.r4_controls.get(connection.state.server_id)
        if (self._draining or key in self.r4_executions or
                connection.state.server_id in self.transports or
                (control is not None and control.connection is not connection)):
            raise ConnectorError('RUNTIME_DRAINING' if self._draining else 'OPERATION_CONFLICT',
                                 'r4_execution', 'The daemon cannot adopt this connection.')
        if not connection.online or not connection.state.control_ready:
            raise ConnectorError('CONTROL_DISCONNECTED', 'r4_execution',
                                 'The control connection is not ready.')
        if len(self.r4_executions) >= 32:
            raise ConnectorError('CAPACITY_EXCEEDED', 'r4_execution',
                                 'The daemon execution connection capacity is exhausted.')
        owner = R4ExecutionOwner(connection, self.store, self.host,
            candidate_provider=candidate_provider, launch_provider=launch_provider,
            publish_receipt=publish_receipt, native_factory=native_factory,
            response_resolver=response_resolver)
        self.r4_executions[key] = owner
        owner.start()
        return owner

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
        if self._draining:
            return
        state = self.store.load()
        registered = {record.server_id for record in state.execution_executors}
        for server_id, profile in state.servers.items():
            if server_id in registered:
                if server_id in self.transports:
                    # A running legacy transport must drain explicitly. A
                    # reload never silently transfers live resource owners.
                    raise ConnectorError('OPERATION_CONFLICT', 'r4_startup',
                        'Stop the legacy daemon before activating this R4 registration.')
                if server_id not in self.r4_controls:
                    if len(self.r4_controls) >= 32:
                        raise ConnectorError('CAPACITY_EXCEEDED', 'r4_startup',
                                             'The R4 control connection capacity is exhausted.')
                    from .r4_control import R4DaemonControl
                    control = R4DaemonControl(self.store, self.vault, self.host, server_id)
                    self.r4_controls[server_id] = control
                    control.start()
                continue
            if server_id in self.r4_controls:
                # reload_state stops removed registrations before any
                # alternative transport can use this Server.
                continue
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
        registered = {record.server_id for record in state.execution_executors}
        for server_id, control in tuple(self.r4_controls.items()):
            if server_id not in registered or server_id not in state.servers:
                await control.stop()
                del self.r4_controls[server_id]
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

        CN5-01.02: the OPERATIONAL proposal is stored as an immutable
        deep copy — the correlation evidence (request_hash etc.) reaches
        the Core exactly as observed — while ONLY the display projection
        is redacted. The channel's executor namespace is preserved (a
        foreign frame never reaches this point; the transport validates
        the namespace before the callback).
        """
        from ..services.approval_state import (
            ApprovalKey, ApprovalTarget, PendingApproval,
        )
        request_id = str(frame.get("request_id", ""))
        server_id = str(frame.get("server_id", self._approval_channel or
                                  ""))
        executor_id = str(frame.get("executor_id", ""))
        binding_id = str(frame.get("binding_id", "") or "")
        key_tuple = (server_id, request_id)
        target = ApprovalTarget(
            key=ApprovalKey(server_id, executor_id, request_id),
            binding_id=binding_id,
            agent_id=str(frame.get("agent_id", "") or ""),
            workspace_id=str(frame.get("workspace_id", "") or ""),
            session_key=None, kind=str(frame.get("kind", "") or ""))
        incoming = PendingApproval.receive(
            target=target, proposal=frame.get("proposal", {}),
            kind=str(frame.get("kind", "") or "") or None,
            received_at=_now(),
            frame_session_id=(str(frame["session_id"]) if
                              frame.get("session_id") else None))
        existing = self._approvals.get(key_tuple)
        if existing is not None:
            # CN5-01.02: a replay of the SAME request (same key, same
            # operational proposal) keeps the record, its attempt and
            # its phase untouched; a DIFFERENT proposal under the same
            # key is a typed conflict — never an overwrite.
            if existing.proposal_digest() == incoming.proposal_digest():
                return None
            raise ConnectorError(
                "OPERATION_CONFLICT", "approval",
                "a different proposal arrived under the same request "
                "key; the in-flight record was preserved")
        self._approvals[key_tuple] = incoming
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
                "r4_controls": {server_id: control.status()
                                for server_id, control in self.r4_controls.items()},
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
            # CN5-01.01: the ONLY projection served outward is the
            # redacted public dict — never the internal DTO.
            yield response_ok(request.seq, {"approvals": [
                record.to_public_dict()
                for record in self._approvals.values()]})
        elif op == "approvals.decide":
            decision = await self._decide_approval(params)
            yield response_ok(request.seq, decision)
        elif op == "approvals.status":
            # CN5-02.03: local consultation of one attempt — phase,
            # real producer presence and the allowed next action;
            # never proposal bytes or raw sensitive responses.
            yield response_ok(request.seq, self._approval_status(params))
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
        """CN5-01/02: reserve ONE attempt, run it in an OWNED producer.

        The waiter (IPC) never owns the Server POST or the Core call:
        ``_run_decision`` is a task registered on the attempt BEFORE the
        first await, and the waiter ``shield``s it - cancelling the
        waiter does not cancel an accepted Server decision, and a
        repeated identical request SHARES the same producer/result while
        a different decision in flight is a typed conflict. The first
        external operation of the permissive flow remains the canonical
        Server decision; the vocabulary is translated (approve-accept,
        deny-decline) only after a strictly-boolean True confirmation.
        """
        from ..services.approval_state import (
            PENDING, SERVER_REFUSED, SERVER_UNKNOWN, NATIVE_REFUSED,
            NATIVE_UNKNOWN, DecisionAttempt, native_operation_id,
            canonical_proposal_digest,
        )
        request_id = str(params.get("request_id", ""))
        decision = str(params.get("decision", ""))
        server_hint = str(params.get("server_id", ""))
        if decision not in ("approve", "deny"):
            raise ConnectorError("VALIDATION_ERROR", "approval",
                                 "decision must be approve or deny")
        # CN4-01.01/ACN4-19: a short CLI choice resolves EXACTLY one
        # request - ambiguity is a typed error listing the servers,
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
            # ACN4-16/20: a terminal attempt is idempotent - the repeat
            # consults the SAME recorded outcome, never re-decides.
            return dict(history["result"])
        pending = self._approvals.get(approval_key)
        if pending is None:
            raise ConnectorError("VALIDATION_ERROR", "approval",
                                 "unknown request")
        # CN5-02.01: an existing attempt is shared/consulted - never
        # re-created (no second POST, no second native application).
        attempt = pending.attempt
        if attempt is not None:
            producer = attempt.producer_task
            if producer is not None and not producer.done():
                if attempt.local_decision != decision:
                    raise ConnectorError(
                        "OPERATION_CONFLICT", "approval",
                        f"a different decision ({attempt.local_decision}) "
                        "is already in flight for this request")
                return await asyncio.shield(producer)
            if attempt.result is not None:
                if attempt.phase in (SERVER_REFUSED, NATIVE_REFUSED,
                                     SERVER_UNKNOWN, NATIVE_UNKNOWN):
                    raise ConnectorError(
                        attempt.error_code or "OPERATION_CONFLICT",
                        "approval",
                        f"attempt ended in {attempt.phase}"
                        + (f": {attempt.error_stage}" if
                           attempt.error_stage else ""),
                        possible_effect=attempt.phase in (SERVER_UNKNOWN,
                                                          NATIVE_UNKNOWN))
                return dict(attempt.result)
            if attempt.phase != PENDING:
                # A finished producer with no result and no phase change
                # must never claim to be "in progress" again.
                raise ConnectorError(
                    "OPERATION_CONFLICT", "approval",
                    f"previous attempt ended in {attempt.phase} without "
                    "a consultable result")
        # CN5-01.01/03: resolve the FULL target before reserving.
        target = self._resolve_approval_target(pending)
        attempt = DecisionAttempt(
            attempt_id=f"{approval_key[0]}:{approval_key[1]}:{decision}",
            key=target.key, target=target,
            local_decision=decision,
            core_decision=("accept" if decision == "approve"
                           else "decline"),
            intent_digest=canonical_proposal_digest(pending.proposal),
            native_operation_id=native_operation_id(
                target.key, decision))
        pending.attempt = attempt
        # CN5-02.01: the producer is created AND registered BEFORE the
        # first await; the waiter shields it.
        attempt.producer_task = asyncio.create_task(
            self._run_decision(attempt, pending, dict(params)),
            name=f"approval-decision-{approval_key[0]}-"
                 f"{approval_key[1]}")
        return await asyncio.shield(attempt.producer_task)

    def _resolve_approval_target(self, pending):
        """CN5-01.03: the full resource scope, resolved by its key."""
        from nexus_connector_core import SessionKey
        from ..services.approval_state import ApprovalTarget
        key = pending.target.key
        state = self.store.load()
        bindings = [b for b in state.bindings
                    if b.server_id == key.server_id
                    and b.executor_id == key.executor_id
                    and b.binding_id == pending.target.binding_id]
        if not bindings:
            bindings = [b for b in state.bindings
                        if b.server_id == key.server_id
                        and b.binding_id == pending.target.binding_id]
        if not bindings:
            raise ConnectorError("VALIDATION_ERROR", "approval",
                                 "binding for this request is gone")
        if len(bindings) > 1:
            raise ConnectorError("VALIDATION_ERROR", "approval",
                                 "binding id is ambiguous in this "
                                 "namespace")
        binding = bindings[0]
        agent_id = pending.target.agent_id or binding.agent_id
        workspace_id = pending.target.workspace_id or \
            binding.workspace_id
        if pending.target.agent_id and \
                pending.target.agent_id != binding.agent_id:
            raise ConnectorError(
                "BINDING_NOT_AUTHORIZED", "approval",
                "request agent does not match the binding in this "
                "namespace")
        if pending.target.workspace_id and \
                pending.target.workspace_id != binding.workspace_id:
            raise ConnectorError(
                "BINDING_NOT_AUTHORIZED", "approval",
                "request workspace does not match the binding in this "
                "namespace")
        session_key = None
        connection_generation = None
        owner_generation = None
        frame_session = pending.frame_session_id
        if frame_session:
            # CN5-01.03: a NATIVE request carries a session - it must
            # resolve in THIS namespace; absence is a typed refusal,
            # never silently "administrative".
            session_key = SessionKey(key.server_id, key.executor_id,
                                     frame_session)
            try:
                session = self.runtimes.session_by_key(session_key)
            except ConnectorError:
                raise ConnectorError(
                    "BINDING_NOT_AUTHORIZED", "approval",
                    f"native approval targets session {frame_session!r} "
                    "which is not managed in this namespace",
                    action="The request cannot be applied to another "
                           "Server's session; refusing.") from None
            connection_generation = session.connection_generation
            owner_generation = session.session_owner_generation
        return ApprovalTarget(
            key=key, binding_id=binding.binding_id,
            agent_id=agent_id, workspace_id=workspace_id,
            session_key=session_key,
            connection_generation=connection_generation,
            session_owner_generation=owner_generation,
            authorization_revision=binding.authorization_revision,
            configuration_revision=binding.configuration_revision,
            kind=pending.kind)

    async def _run_decision(self, attempt, pending,
                            params: dict[str, object]
                            ) -> dict[str, object]:
        """CN5-02: the OWNED producer of one decision attempt.

        Runs the canonical Server POST and (only after a strictly
        positive confirmation and a re-validated target) the native
        application. Every exit is explicit (the plan's phase table);
        nothing here is cancelled by the waiter that merely awaits.
        """
        from ..services.approval_state import (
            APPLIED, NATIVE_PENDING, NATIVE_REFUSED, NATIVE_UNKNOWN,
            SERVER_CONFIRMED, SERVER_PENDING, SERVER_REFUSED,
            SERVER_UNKNOWN,
        )
        from nexus_connector_core import CoreError
        target = attempt.target
        request_id = target.key.request_id
        decision = attempt.local_decision
        server_id = target.key.server_id
        state = self.store.load()
        profile = state.servers.get(server_id)
        identity = state.identity_for(server_id, target.agent_id)
        if profile is None or identity is None:
            attempt.phase = SERVER_REFUSED
            attempt.error_code = "AGENT_AUTH_REQUIRED"
            attempt.result = None
            raise ConnectorError("AGENT_AUTH_REQUIRED", "approval")
        agent_key = self.vault.resolve(identity.secret_handle)
        # CN5-01.04: target validated before the POST; the client
        # belongs to the PRODUCER, never to the waiter.
        attempt.phase = SERVER_PENDING
        try:
            async with NexusHTTPClient(profile.base_url) as http:
                applied = await http.approval_decision(
                    agent_key, request_id=request_id, decision=decision,
                    cas_token=str(params.get("cas_token", "")),
                    response=params.get("response") if isinstance(
                        params.get("response"), dict) else None)
        except ConnectorError as error:
            if getattr(error, "possible_effect", False):
                # CN5-02.02: possible write - unknown, consultable, no
                # automatic re-POST and no native application.
                attempt.phase = SERVER_UNKNOWN
            else:
                attempt.phase = SERVER_REFUSED
            attempt.error_code = error.code
            attempt.error_stage = error.stage
            attempt.result = None
            raise
        if applied is not True:
            # Strictly-boolean gate upstream; False is the Server's
            # canonical "not applied" - terminal refusal, zero native.
            attempt.phase = SERVER_REFUSED
            attempt.result = {"request_id": request_id,
                              "decision": decision, "applied": False,
                              "native": None,
                              "authority": "server-side CAS answered; "
                                           "the Core was not touched"}
            self._record_terminal((server_id, request_id),
                                  attempt.result)
            return attempt.result
        attempt.server_confirmed = True
        attempt.phase = SERVER_CONFIRMED
        if target.session_key is None:
            # Administrative request: the canonical CAS decision IS the
            # whole authorized effect.
            attempt.phase = APPLIED
            attempt.result = {
                "request_id": request_id, "decision": decision,
                "applied": True, "native": None,
                "authority": "canonical Server decision confirmed "
                             "(administrative request)"}
            self._record_terminal((server_id, request_id),
                                  attempt.result)
            return attempt.result
        # CN5-01.04: re-validate the target AFTER the POST - a target
        # that changed during the confirmation keeps the canonical
        # answer (no second POST) and records a typed refusal with ZERO
        # effect on the new target.
        try:
            current = self._resolve_approval_target(pending)
        except ConnectorError:
            current = None
        target_changed = not (
            current is not None
            and current.binding_id == target.binding_id
            and current.agent_id == target.agent_id
            and current.workspace_id == target.workspace_id
            and current.session_key == target.session_key
            and current.connection_generation ==
            target.connection_generation
            and current.session_owner_generation ==
            target.session_owner_generation
            and current.authorization_revision ==
            target.authorization_revision)
        if target_changed:
            attempt.phase = NATIVE_REFUSED
            attempt.error_code = "STALE_GENERATION"
            attempt.error_stage = ("approval target changed during "
                                   "confirmation")
            attempt.result = None
            raise ConnectorError(
                "STALE_GENERATION", "approval",
                "the approval target changed while the Server "
                "confirmed; the canonical answer is kept and no "
                "native effect was issued",
                action="Do not re-POST: the confirmation is durable; "
                       "reconcile the target before a new request.")
        # CN5-01.03: the native application is MANDATORILY scoped by
        # the full target (SessionKey namespace + captured snapshot).
        attempt.phase = NATIVE_PENDING
        try:
            native_result = await self.runtimes.decide_native_approval(
                target=target,
                operation_id=attempt.native_operation_id,
                request=pending.proposal,
                decision=attempt.core_decision,
                response=params.get("response") if isinstance(
                    params.get("response"), dict) else None)
        except ConnectorError as error:
            if getattr(error, "possible_effect", False):
                attempt.phase = NATIVE_UNKNOWN
            else:
                # CN5-02.02/ACN5-25: a typed pre-effect refusal keeps
                # the canonical confirmation and stays consultable -
                # never a false "in progress", never a re-POST.
                attempt.phase = NATIVE_REFUSED
            attempt.error_code = error.code
            attempt.error_stage = error.stage
            attempt.result = None
            raise
        except CoreError as error:
            # The Core's own contract refusals (hash mismatch, journal,
            # correlation) are typed, terminal and consultable.
            attempt.phase = NATIVE_REFUSED
            attempt.error_code = str(getattr(error, "code",
                                             "CORE_ERROR"))
            attempt.error_stage = "core:" + str(
                getattr(error, "stage", "approval_decide"))
            attempt.result = None
            raise ConnectorError(
                attempt.error_code, "approval",
                f"the Core refused the native application: "
                f"{getattr(error, 'message', error)}",
                action="The Server confirmation is durable; consult "
                       "this attempt via approvals.status.") from None
        attempt.phase = APPLIED
        attempt.result = {
            "request_id": request_id, "decision": decision,
            "applied": True, "native": native_result,
            "authority": "canonical Server decision confirmed before "
                         "any native application"}
        self._record_terminal((server_id, request_id), attempt.result)
        return attempt.result

    def _approval_status(self, params: dict[str, object]
                         ) -> dict[str, object]:
        """CN5-02.03: consult one attempt without side effects."""
        request_id = str(params.get("request_id", ""))
        server_hint = str(params.get("server_id", ""))
        matches = [key for key in list(self._approvals) +
                   list(self._approval_history)
                   if key[1] == request_id
                   and (not server_hint or key[0] == server_hint)]
        if len(matches) != 1:
            raise ConnectorError("VALIDATION_ERROR", "approval",
                                 "unknown or ambiguous request")
        key = matches[0]
        history = self._approval_history.get(key)
        if history is not None:
            return {"request_id": request_id, "phase": "terminal",
                    "producer": False,
                    "result": history["result"],
                    "allowed": "consult (terminal outcome recorded)"}
        pending = self._approvals[key]
        attempt = pending.attempt
        if attempt is None:
            return {"request_id": request_id, "phase": "pending",
                    "producer": False,
                    "allowed": "decide (awaiting operator)"}
        producer_alive = (attempt.producer_task is not None
                          and not attempt.producer_task.done())
        allowed = ("share/await the running attempt" if producer_alive
                   else "consult the recorded outcome" if
                   attempt.result is not None else
                   f"inspect (ended in {attempt.phase})")
        return {"request_id": request_id, "phase": attempt.phase,
                "producer": producer_alive,
                "result": attempt.result,
                "error": {"code": attempt.error_code,
                          "stage": attempt.error_stage}
                if attempt.error_code else None,
                "allowed": allowed}

    def _record_terminal(self, approval_key: tuple[str, str],
                         result: dict[str, object]) -> None:
        """CN4-01.04/ACN5-02.04: retire the request BY ITS OWN KEY (the
        agent secret never substitutes the dictionary key) and keep a
        bounded tombstone so repeats consult the same outcome."""
        from ..services.approval_state import TOMBSTONE_LIMIT
        self._approvals.pop(approval_key, None)
        self._approval_history[approval_key] = {
            "result": result, "at": _now()}
        while len(self._approval_history) > TOMBSTONE_LIMIT:
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
