"""Outbound NXL WSS control client (plan C04, Anexo A.8/A.5).

The connector only ever *dials out* to the Server's link endpoint; no
listener is opened on the harness host (TC-15). Frames are encoded and
decoded with the Core's bundled NXL r3 codec; the revision gate is exact
during development. Each binding multiplexes as a lane authenticated by
its own short-lived ticket (never in URLs or logs). Heartbeats, bounded
priority queues, generation fencing and reconcile-before-admit semantics
follow Anexo A.10–A.12.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Mapping

import websockets

from nexus_connector_core import CONTRACT_REVISION
from nexus_connector_core.protocol import PROTOCOL_MAJOR
from nexus_connector_core.event_reducer import event_batch_frame
from nexus_connector_core.frame_codec import decode_frame, encode_frame
from nexus_connector_core.models import CoreError, RuntimeEvent

from ..errors import ConnectorError
from ..redaction import redact_text

logger = logging.getLogger(__name__)

HEARTBEAT_SECONDS = 15.0
ABSENCE_DETECT_SECONDS = 45.0
BACKOFF_MIN = 0.5
BACKOFF_MAX = 30.0
URGENT_BUDGET = 8
NORMAL_LIMIT = 256


@dataclass(slots=True)
class LaneState:
    binding_id: str
    agent_id: str
    ticket_provider: Callable[[], Awaitable[str]]
    attached: bool = False
    credential_epoch: int = 1
    authorization_revision: int = 1


@dataclass(slots=True)
class TransportStats:
    state: str = "stopped"  # stopped|connecting|online|degraded|backoff
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
    """Two finite queues with an urgent budget (plan A.10, TC-19)."""

    def __init__(self) -> None:
        self._urgent: asyncio.Queue = asyncio.Queue(maxsize=URGENT_BUDGET)
        self._normal: asyncio.Queue = asyncio.Queue(maxsize=NORMAL_LIMIT)
        self.dropped_normal = 0

    async def put(self, frame: Mapping[str, object], *, urgent: bool = False
                  ) -> bool:
        queue = self._urgent if urgent else self._normal
        try:
            queue.put_nowait(frame)
            return True
        except asyncio.QueueFull:
            if not urgent:
                self.dropped_normal += 1
            return False

    async def get(self) -> Mapping[str, object]:
        while True:
            if not self._urgent.empty():
                return self._urgent.get_nowait()
            urgent_wait = asyncio.create_task(self._urgent.get())
            normal_wait = asyncio.create_task(self._normal.get())
            try:
                done, pending = await asyncio.wait(
                    {urgent_wait, normal_wait},
                    return_when=asyncio.FIRST_COMPLETED)
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except (asyncio.CancelledError, Exception):
                        pass
                for task in done:
                    return task.result()
            except asyncio.CancelledError:
                urgent_wait.cancel()
                normal_wait.cancel()
                raise

    def pending(self) -> int:
        return self._urgent.qsize() + self._normal.qsize()


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
        self._acked: dict[tuple[str, str], int] = {}
        self._ack_waiters: dict[tuple[str, str], asyncio.Event] = {}

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
        self.stats.state = "stopped"
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

    async def send_events(self, events: list[RuntimeEvent]) -> bool:
        if not events:
            return True
        # Batches must be contiguous per contract; split at gaps so the
        # peer sees an explicit watermark stop rather than an error.
        sent = True
        for batch in _contiguous_batches(events):
            sent = await self.queues.put(event_batch_frame(batch)) and sent
        if not events:
            return sent
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
        return self._acked.get(key)

    # -- main loop -------------------------------------------------------------

    async def _run(self) -> None:
        backoff = BACKOFF_MIN
        while not self._stop.is_set():
            self.stats.state = "connecting"
            try:
                ticket = await self._ticket_provider()
                headers = [("Authorization", f"Bearer {ticket}")]
                async with websockets.connect(
                        self.link_url, subprotocols=["nxl.v1"],
                        additional_headers=headers,
                        max_size=1024 * 1024, open_timeout=15,
                        ping_interval=None) as websocket:
                    self._websocket = websocket
                    self.stats.connection_generation += 1
                    self.stats.connected_at = time.time()
                    self.stats.state = "online"
                    self.stats.reconnects += 1
                    self._online.set()
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
                for lane in self._lanes.values():
                    lane.attached = False
                self.stats.lanes.update({key: False for key in self._lanes})
            if self._stop.is_set():
                break
            self.stats.state = "backoff"
            await asyncio.sleep(backoff * (0.5 + random.random()))
            backoff = min(backoff * 2, BACKOFF_MAX)
        self.stats.state = "stopped"

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
        sender = asyncio.create_task(self._sender(websocket))
        receiver = asyncio.create_task(self._receiver(websocket))
        heartbeat = asyncio.create_task(self._heartbeat(websocket))
        last_receive = time.monotonic()
        try:
            while not self._stop.is_set():
                done, pending = await asyncio.wait(
                    {sender, receiver, heartbeat},
                    return_when=asyncio.FIRST_COMPLETED)
                if receiver in done:
                    break
                for task in done:
                    exc = task.exception()
                    if exc is not None:
                        raise exc
                if time.monotonic() - last_receive > ABSENCE_DETECT_SECONDS:
                    raise ConnectorError("EXECUTOR_OFFLINE", "wss",
                                         "peer absence detected",
                                         retry_safe=True)
                await asyncio.sleep(1.0)
        finally:
            for task in (sender, receiver, heartbeat):
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
            self.stats.state = "degraded"

    async def _sender(self, websocket) -> None:
        while True:
            frame = await self.queues.get()
            await websocket.send(encode_frame(frame))
            self.stats.frames_sent += 1
            self.stats.last_frame_at = time.time()

    async def _receiver(self, websocket) -> None:
        async for raw in websocket:
            self.stats.frames_received += 1
            self.stats.last_frame_at = time.time()
            try:
                frame = decode_frame(raw)
            except CoreError as error:
                logger.warning("invalid frame from server: %s", error.code)
                continue
            await self._handle(frame)

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

    # -- inbound handling ---------------------------------------------------

    async def _handle(self, frame: dict[str, object]) -> None:
        kind = frame.get("type")
        if kind == "welcome":
            self._check_generation(int(frame.get("connection_generation", 0)))
            await self._attach_lanes()
        elif kind == "operation.submit":
            from nexus_connector_core.models import OperationReceipt
            receipt = await self._on_operation(frame)
            if isinstance(receipt, OperationReceipt):
                from nexus_connector_core.receipt_reducer import receipt_frame
                receipt = receipt_frame(
                    receipt, server_id=self.server_id,
                    executor_id=self.executor_id)
            if receipt is not None:
                await self.queues.put(receipt)
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
            self._acked[key] = int(frame.get("sequence", 0))
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
                lane.attached = False
                self.stats.lanes[binding_id] = False

    def _check_generation(self, server_generation: int) -> None:
        if server_generation < self.stats.connection_generation:
            raise ConnectorError("STALE_GENERATION", "welcome",
                                 "server offered a stale generation")

    async def _attach_lanes(self) -> None:
        for lane in list(self._lanes.values()):
            ticket = await lane.ticket_provider()
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
            await self.queues.put(frame, urgent=True)
            lane.attached = True
            self.stats.lanes[lane.binding_id] = True


def _core_version() -> str:
    try:
        import nexus_connector_core
        return nexus_connector_core.__version__
    except Exception:  # pragma: no cover
        return "unknown"


def _contiguous_batches(events: list) -> list[list]:
    """Split a sequence list into contract-valid contiguous batches."""
    batches: list[list] = []
    current: list = []
    expected: int | None = None
    for event in events:
        if expected is not None and event.sequence != expected:
            batches.append(current)
            current = []
        current.append(event)
        expected = event.sequence + 1
    if current:
        batches.append(current)
    return batches
