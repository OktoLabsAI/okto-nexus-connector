"""Outbound NXL WSS control client (plan C04, Anexo A.8/A.5).

CN1 corrections applied here (audit A02/A04/A05/A12):

* Lossless priority queues — two consumers woken together can never
  drop an already-dequeued item (A04).
* Explicit connection state machine — SOCKET_OPEN → NEGOTIATING →
  RECONCILING → READY; the Server's ``connection_generation`` from
  welcome is ADOPTED (a local reconnect counter is never authority);
  operations are refused before negotiation+reconciliation complete
  and outside a live lane scope (A02).
* The receiver validates each inbound operation's FULL envelope
  (server/executor/binding/agent/generation) against the negotiated
  scope before any dispatch; the handler runs on a bounded scheduler
  so a blocked submit never blocks interrupts/ACKs (A05).
* A monotonic watchdog with its own deadline keeps absence detection
  alive even while sender/receiver/heartbeat tasks are pending (A05).
* Lanes attach asynchronously and are only READY after the attach
  frame was actually written; a full urgent queue never fakes success
  (A12).
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Mapping

import websockets

from nexus_connector_core import CONTRACT_REVISION
from nexus_connector_core.frame_codec import decode_frame, encode_frame
from nexus_connector_core.models import CoreError, RuntimeEvent
from nexus_connector_core.protocol import PROTOCOL_MAJOR

from ..errors import ConnectorError
from ..redaction import redact_text

logger = logging.getLogger(__name__)

HEARTBEAT_SECONDS = 15.0
ABSENCE_DETECT_SECONDS = 45.0
BACKOFF_MIN = 0.5
BACKOFF_MAX = 30.0
URGENT_BUDGET = 8
NORMAL_LIMIT = 256
#: CN1/A05: inbound productive operations run on a bounded scheduler;
#: control frames (ACK, heartbeat, reconcile, approvals) never wait
#: behind a stuck submit.
OPERATION_CONCURRENCY = 4

#: Transport states (CN1/A02/A12). ``online`` (legacy view) is true
#: only in READY.
ST_STOPPED = "stopped"
ST_CONNECTING = "connecting"
ST_SOCKET_OPEN = "socket_open"
ST_NEGOTIATING = "negotiating"
ST_RECONCILING = "reconciling"
ST_READY = "ready"
ST_BACKOFF = "backoff"
ST_DEGRADED = "degraded"


@dataclass(slots=True)
class LaneState:
    binding_id: str
    agent_id: str
    ticket_provider: Callable[[], Awaitable[str]]
    #: PENDING → ATTACHING (frame written) → READY. CN1/A12: enqueue
    #: alone is never proof of admission.
    state: str = "pending"
    ticket_epoch: int = 1
    expires_at: float | None = None
    authorization_revision: int = 1
    credential_epoch: int = 1


@dataclass(slots=True)
class TransportStats:
    state: str = ST_STOPPED
    connection_generation: int = 0
    connected_at: float | None = None
    last_frame_at: float | None = None
    frames_sent: int = 0
    frames_received: int = 0
    reconnects: int = 0
    lanes: dict[str, bool] = field(default_factory=dict)

    def to_json(self) -> dict[str, object]:
        return {
            "state": self.state,
            "connection_generation": self.connection_generation,
            "connected_at": self.connected_at,
            "last_frame_at": self.last_frame_at,
            "frames_sent": self.frames_sent,
            "frames_received": self.frames_received,
            "reconnects": self.reconnects,
            "lanes": dict(self.lanes),
        }


class PriorityQueues:
    """Two finite queues with an urgent budget — lossless (CN1/A04).

    ``get`` selects from one condition-guarded store: an item taken
    from a queue is either delivered to the caller or explicitly kept
    for the next call — two simultaneous puts can never cause a lost
    message (test_08).
    """

    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._urgent: list = []
        self._normal: list = []
        self.urgent_capacity = URGENT_BUDGET
        self.normal_capacity = NORMAL_LIMIT
        self.dropped_normal = 0

    async def put(self, frame: Mapping[str, object], *, urgent: bool = False
                  ) -> bool:
        async with self._condition:
            if urgent:
                if len(self._urgent) >= self.urgent_capacity:
                    return False  # caller must keep its obligation
                self._urgent.append(frame)
            else:
                if len(self._normal) >= self.normal_capacity:
                    self.dropped_normal += 1
                    return False
                self._normal.append(frame)
            self._condition.notify_all()
            return True

    def _take(self) -> Mapping[str, object] | None:
        if self._urgent:
            return self._urgent.pop(0)
        if self._normal:
            return self._normal.pop(0)
        return None

    async def get(self) -> Mapping[str, object]:
        async with self._condition:
            while True:
                item = self._take()
                if item is not None:
                    return item
                await self._condition.wait()

    def pending(self) -> int:
        return len(self._urgent) + len(self._normal)


class NXLTransport:
    """One Server's outbound control channel with lanes per binding."""

    def __init__(self, *, server_id: str, executor_id: str, link_url: str,
                 ticket_provider: Callable[[], Awaitable[str]],
                 on_operation: Callable[[dict[str, object]],
                                        Awaitable[dict[str, object] | None]],
                 on_approval: Callable[[dict[str, object]],
                                       Awaitable[dict[str, object] | None]],
                 on_reconcile: Callable[[dict[str, object]],
                                        Awaitable[dict[str, object] | None]]
                 | None = None):
        self.server_id = server_id
        self.executor_id = executor_id
        self.link_url = link_url
        self._ticket_provider = ticket_provider
        self._on_operation = on_operation
        self._on_approval = on_approval
        self._on_reconcile = on_reconcile
        self.stats = TransportStats()
        self.queues = PriorityQueues()
        self._lanes: dict[str, LaneState] = {}
        self._websocket = None
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._online = asyncio.Event()
        #: CN1/A02: the negotiated scope. server/executor are fixed by
        #: the link identity; ``connection_generation`` is the value the
        #: SERVER authorized in welcome (adopted verbatim); ``reconciled``
        #: gates productive admissions.
        self._negotiated = False
        self._reconciled = False
        self._server_generation: int | None = None
        self._ack_source: dict[tuple[str, str], int] = {}
        self._ack_waiters: dict[tuple[str, str], asyncio.Event] = {}
        self._op_semaphore = asyncio.Semaphore(OPERATION_CONCURRENCY)
        self._inflight: set[asyncio.Task] = set()
        #: CN1/A05: activity marker in MONOTONIC time (wall-clock stats
        #: never gate the watchdog — different clocks never compare).
        self._last_activity_monotonic: float | None = None

    # -- lifecycle ---------------------------------------------------------

    def add_lane(self, binding_id: str, agent_id: str,
                 ticket_provider: Callable[[], Awaitable[str]],
                 *, credential_epoch: int = 1,
                 authorization_revision: int = 1) -> None:
        self._lanes[binding_id] = LaneState(
            binding_id, agent_id, ticket_provider,
            credential_epoch=credential_epoch,
            authorization_revision=authorization_revision)
        self.stats.lanes[binding_id] = False

    def remove_lane(self, binding_id: str) -> None:
        self._lanes.pop(binding_id, None)
        self.stats.lanes.pop(binding_id, None)

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="nxl-transport")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self._websocket is not None:
            await self._websocket.close()
            self._websocket = None
        self.stats.state = ST_STOPPED
        self._online.clear()

    async def wait_online(self, timeout: float = 30.0) -> bool:
        try:
            await asyncio.wait_for(self._online.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    @property
    def online(self) -> bool:
        return self._online.is_set()

    # -- sending -------------------------------------------------------------

    async def send_frame(self, frame: Mapping[str, object], *,
                         urgent: bool = False) -> bool:
        return await self.queues.put(frame, urgent=urgent)

    async def send_events(self, events: list) -> bool:
        from ._batches import _contiguous_batches
        if not events:
            return True
        sent = True
        for batch in _contiguous_batches(events):
            from nexus_connector_core.event_reducer import event_batch_frame
            sent = await self.queues.put(event_batch_frame(batch)) and sent
        key = (events[-1].session_id, events[-1].stream_epoch)
        self._ack_waiters.setdefault(key, asyncio.Event()).clear()
        return sent

    async def wait_event_ack(self, session_id: str, stream_epoch: str,
                             timeout: float = 30.0) -> int | None:
        key = (session_id, stream_epoch)
        waiter = self._ack_waiters.setdefault(key, asyncio.Event())
        try:
            await asyncio.wait_for(waiter.wait(), timeout)
        except asyncio.TimeoutError:
            return None
        return self._ack_source.get(key)

    # -- main loop -------------------------------------------------------------

    async def _run(self) -> None:
        backoff = BACKOFF_MIN
        while not self._stop.is_set():
            self.stats.state = ST_CONNECTING
            try:
                ticket = await self._ticket_provider()
                headers = [("Authorization", f"Bearer {ticket}")]
                async with websockets.connect(
                        self.link_url, subprotocols=["nxl.v1"],
                        additional_headers=headers,
                        max_size=1024 * 1024, open_timeout=15,
                        ping_interval=None) as websocket:
                    self._websocket = websocket
                    self._negotiated = False
                    self._reconciled = False
                    self.stats.connected_at = time.time()
                    self._last_activity_monotonic = time.monotonic()
                    self.stats.reconnects += 1
                    self.stats.state = ST_SOCKET_OPEN
                    backoff = BACKOFF_MIN
                    await self._session(websocket)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("nxl transport error: %s",
                             redact_text(repr(exc)))
            finally:
                self._websocket = None
                self._online.clear()
                self._negotiated = False
                self._reconciled = False
                for lane in self._lanes.values():
                    lane.state = "pending"
                self.stats.lanes.update({key: False for key in self._lanes})
            if self._stop.is_set():
                break
            self.stats.state = ST_BACKOFF
            await asyncio.sleep(backoff * (0.5 + random.random()))
            backoff = min(backoff * 2, BACKOFF_MAX)
        self.stats.state = ST_STOPPED

    async def _session(self, websocket) -> None:
        hello = {
            "protocol_major": PROTOCOL_MAJOR,
            "contract_revision": CONTRACT_REVISION,
            "type": "hello",
            "server_id": self.server_id,
            "executor_id": self.executor_id,
            "core_version": _core_version(),
            "event_types": ["lifecycle", "turn_state", "text_delta",
                            "tool_activity", "approval_request", "metrics",
                            "system_warning", "error", "native_unknown"],
            "limits": {"max_frame_bytes": 1048576,
                       "max_events_per_batch": 128},
            "capabilities": [],
        }
        await websocket.send(encode_frame(hello))
        self.stats.frames_sent += 1
        self.stats.state = ST_NEGOTIATING
        sender = asyncio.create_task(self._sender(websocket))
        receiver = asyncio.create_task(self._receiver(websocket))
        heartbeat = asyncio.create_task(self._heartbeat(websocket))
        # CN1/A05: the watchdog has its OWN periodic deadline — it fires
        # even while sender/receiver/heartbeat are pending, and valid
        # inbound traffic refreshes the activity marker.
        watchdog = asyncio.create_task(self._watchdog())
        try:
            done, _pending = await asyncio.wait(
                {sender, receiver, heartbeat, watchdog},
                return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                exc = task.exception()
                if exc is not None and not isinstance(exc,
                                                      asyncio.CancelledError):
                    raise exc
        finally:
            for task in (sender, receiver, heartbeat, watchdog):
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
            for task in list(self._inflight):
                task.cancel()
            for task in list(self._inflight):
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
            self._inflight.clear()
            self.stats.state = ST_DEGRADED

    async def _sender(self, websocket) -> None:
        while True:
            frame = await self.queues.get()
            await websocket.send(encode_frame(frame))
            self.stats.frames_sent += 1
            self.stats.last_frame_at = time.time()
            kind = frame.get("type")
            if kind == "binding.attach":
                # CN1/A12: the lane is READY only once the attach frame
                # was actually WRITTEN to the socket — enqueue alone is
                # never proof of admission.
                lane = self._lanes.get(str(frame.get("binding_id")))
                if lane is not None and lane.state == "attaching":
                    lane.state = "ready"
                    self.stats.lanes[lane.binding_id] = True

    async def _receiver(self, websocket) -> None:
        async for raw in websocket:
            self.stats.frames_received += 1
            self.stats.last_frame_at = time.time()
            self._last_activity_monotonic = time.monotonic()
            try:
                frame = decode_frame(raw)
            except CoreError as error:
                logger.warning("invalid frame from server: %s", error.code)
                continue
            # CN1/A05: dispatch NEVER awaits a productive handler on the
            # receiver loop — control frames (ACK, heartbeat, reconcile,
            # approvals, errors) are handled inline; operations run on a
            # bounded scheduler with owned tasks.
            kind = frame.get("type")
            if kind == "operation.submit":
                # CN1/A02: envelope validation happens HERE — an invalid
                # scope never reaches the handler, never spawns a task.
                if self._validate_operation_scope(frame) is None:
                    self._reject_premature(frame)
                    continue
                task = asyncio.create_task(self._run_operation(frame))
                self._inflight.add(task)
                task.add_done_callback(self._inflight.discard)
            elif kind == "welcome":
                task = asyncio.create_task(self._complete_handshake(frame))
                self._inflight.add(task)
                task.add_done_callback(self._inflight.discard)
            else:
                await self._handle(frame)

    async def _run_operation(self, frame: dict) -> None:
        async with self._op_semaphore:
            try:
                receipt = await self._on_operation(frame)
            except ConnectorError as error:
                from ..daemon.app import _error_frame
                receipt = _error_frame(error, frame)
            except CoreError as error:
                from ..daemon.app import _error_frame
                receipt = _error_frame(ConnectorError(
                    error.code, error.stage, error.message or error.code,
                    possible_effect=error.possible_effect,
                    retry_safe=error.retry_safe,
                    operation_id=error.operation_id), frame)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("operation handler failed")
                from ..daemon.app import _error_frame
                receipt = _error_frame(ConnectorError(
                    "UNKNOWN", "wss_operation",
                    "internal error; see daemon log"), frame)
            if receipt is not None:
                if isinstance(receipt, dict):
                    await self.queues.put(receipt)
                else:
                    from nexus_connector_core.receipt_reducer import \
                        receipt_frame as _rf
                    from nexus_connector_core.models import OperationReceipt
                    if isinstance(receipt, OperationReceipt):
                        await self.queues.put(_rf(
                            receipt, server_id=self.server_id,
                            executor_id=self.executor_id))

    async def _heartbeat(self, websocket) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_SECONDS)
            frame = {
                "protocol_major": PROTOCOL_MAJOR,
                "contract_revision": CONTRACT_REVISION,
                "type": "heartbeat",
                "server_id": self.server_id,
                "executor_id": self.executor_id,
                "connection_generation": self.stats.connection_generation,
            }
            await websocket.send(encode_frame(frame))
            self.stats.frames_sent += 1

    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(ABSENCE_DETECT_SECONDS)
            last = self._last_activity_monotonic
            if last is None or \
                    time.monotonic() - last > ABSENCE_DETECT_SECONDS:
                # No valid inbound traffic within the window: the peer is
                # absent at the application level even with the socket
                # open and sibling tasks alive (test_09).
                raise ConnectorError(
                    "EXECUTOR_OFFLINE", "wss_watchdog",
                    "peer application absence detected",
                    retry_safe=True)

    # -- inbound handling ---------------------------------------------------

    async def _handle(self, frame: dict[str, object]) -> None:
        kind = frame.get("type")
        if kind == "welcome":
            await self._complete_handshake(frame)
        elif kind == "operation.submit":
            # Only reached pre-negotiation (receiver validates scope).
            self._reject_premature(frame)
        elif kind == "approval.request":
            decision = await self._on_approval(frame)
            if decision is not None:
                await self.queues.put(decision, urgent=True)
        elif kind == "reconcile.request":
            if self._on_reconcile is not None:
                report = await self._on_reconcile(frame)
                if report is not None:
                    await self.queues.put(report)
        elif kind == "event.ack":
            key = (str(frame.get("session_id")),
                   str(frame.get("stream_epoch")))
            self._ack_source[key] = int(frame.get("sequence", 0))
            waiter = self._ack_waiters.get(key)
            if waiter is not None:
                waiter.set()
        elif kind == "goaway":
            logger.info("server sent goaway: %s",
                        redact_text(str(frame.get("reason", ""))))
            self._stop.set()
        elif kind == "error":
            logger.warning("server error frame: %s %s",
                           frame.get("code"), redact_text(
                               str(frame.get("stage", ""))))
        elif kind == "binding.detach":
            binding_id = str(frame.get("binding_id"))
            lane = self._lanes.get(binding_id)
            if lane is not None:
                lane.state = "detached"
                self.stats.lanes[binding_id] = False
        elif kind == "heartbeat":
            pass  # activity already recorded by the receiver

    async def _complete_handshake(self, frame: dict) -> None:
        """Adopt the SERVER's generation, attach lanes, reconcile."""
        server_generation = int(frame.get("connection_generation", 0))
        frame_server = str(frame.get("server_id", ""))
        frame_executor = str(frame.get("executor_id", ""))
        if frame_server != self.server_id or \
                (frame_executor and frame_executor != self.executor_id):
            raise ConnectorError("BINDING_NOT_AUTHORIZED", "welcome",
                                 "welcome scope does not match the "
                                 "link identity")
        # CN1/A02 (test_06): the SERVER's generation is adopted verbatim;
        # the local reconnect counter is bookkeeping only.
        if self._server_generation is not None and \
                server_generation < self._server_generation:
            raise ConnectorError("STALE_GENERATION", "welcome",
                                 "server offered a stale generation")
        self._server_generation = server_generation
        self.stats.connection_generation = server_generation
        self._negotiated = True
        self.stats.state = ST_RECONCILING
        await self._attach_lanes()
        # CN1/A02 (test_07): reconciliation precedes admissions. The
        # executor declares its durable state with a reconcile.report
        # immediately after attach; READY only after it is written.
        await self._send_initial_reconcile()
        self._reconciled = True
        self.stats.state = ST_READY
        self._online.set()

    def _reject_premature(self, frame: dict[str, object]) -> None:
        logger.warning("operation %s refused: transport not ready (%s)",
                       frame.get("operation_id"), self.stats.state)
        from ..daemon.app import _error_frame
        error = ConnectorError(
            "EXECUTOR_OFFLINE", "wss_admission",
            "operation arrived before handshake/reconciliation completed",
            retry_safe=True,
            action="The Server must wait for reconcile.report before "
                   "submitting work.")
        asyncio.ensure_future(self.queues.put(_error_frame(error, frame)))

    def _validate_operation_scope(self, frame: dict[str, object]
                                  ) -> LaneState | None:
        """CN1/A02 (tests 05/22): full envelope check before dispatch.

        server_id, executor_id, agent_id, binding_id and the connection
        generation must match the negotiated scope and a live lane; a
        valid intent hash is NOT a credential.
        """
        if not self._negotiated or not self._reconciled:
            return None
        if str(frame.get("server_id")) != self.server_id:
            return None
        if str(frame.get("executor_id")) != self.executor_id:
            return None
        generation = frame.get("connection_generation")
        if generation is None or int(generation) != \
                self.stats.connection_generation:
            return None
        lane = self._lanes.get(str(frame.get("binding_id")))
        if lane is None or lane.state != "ready":
            return None
        if str(frame.get("agent_id")) != lane.agent_id:
            return None
        authorization_revision = int(frame.get("authorization_revision", 0))
        if authorization_revision < lane.authorization_revision:
            return None
        return lane

    async def _run_operation_guarded(self, frame: dict) -> None:
        lane = self._validate_operation_scope(frame)
        if lane is None:
            self._reject_premature(frame)
            return
        await self._run_operation(frame)

    async def _send_initial_reconcile(self) -> None:
        report = {
            "protocol_major": PROTOCOL_MAJOR,
            "contract_revision": CONTRACT_REVISION,
            "type": "reconcile.report",
            "server_id": self.server_id,
            "executor_id": self.executor_id,
            "receipts": [],
            "snapshots": [],
        }
        # The daemon refines this projection via on_reconcile when live.
        if self._on_reconcile is not None:
            try:
                refined = await self._on_reconcile({
                    "server_id": self.server_id,
                    "executor_id": self.executor_id,
                    "operation_ids": [],
                    "session_ids": []})
                if refined is not None:
                    report = refined
            except Exception:
                logger.exception("initial reconcile projection failed")
        await self.queues.put(report)

    async def _attach_lanes(self) -> None:
        for lane in list(self._lanes.values()):
            if lane.state in ("ready", "attaching"):
                continue
            try:
                ticket = await lane.ticket_provider()
            except Exception:
                logger.warning("ticket fetch failed for lane %s",
                               lane.binding_id)
                continue
            # The lane may have been removed while the ticket was being
            # fetched; attaching it anyway would resurrect a revoked lane.
            if self._lanes.get(lane.binding_id) is not lane:
                continue
            frame = {
                "protocol_major": PROTOCOL_MAJOR,
                "contract_revision": CONTRACT_REVISION,
                "type": "binding.attach",
                "server_id": self.server_id,
                "executor_id": self.executor_id,
                "binding_id": lane.binding_id,
                "agent_id": lane.agent_id,
                "authorization_revision": lane.authorization_revision,
                "credential_epoch": lane.credential_epoch,
                "ticket": ticket,
            }
            # CN1/A12: a refused put is an obligation kept by the caller
            # — the lane stays PENDING, never falsely attached; the
            # SENDER flips it to ready once the frame is written.
            if not await self.queues.put(frame, urgent=True):
                lane.state = "pending"
                self.stats.lanes[lane.binding_id] = False
                continue
            lane.state = "attaching"

    # -- compatibility view ------------------------------------------------


def _core_version() -> str:
    try:
        import nexus_connector_core
        return nexus_connector_core.__version__
    except Exception:  # pragma: no cover
        return "unknown"
