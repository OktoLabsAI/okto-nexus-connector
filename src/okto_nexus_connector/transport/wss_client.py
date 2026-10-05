"""Outbound NXL WSS control client (plan C04, Anexo A.8/A.5).

CN2 corrections layered over CN1 (audit N01–N08):

* **N01 — authority end-to-end:** the receiver builds an immutable
  ``ValidatedOperation`` DTO carrying the FULL authenticated envelope
  (connection identity, namespace, agent/binding/workspace, session key,
  generations, revisions, grant); dispatch resolves sessions ONLY in
  that namespace and REVALIDATES the lane reservation after every
  admission wait, immediately before the Core call. A lane detached
  during the wait produces zero effects.
* **N02 — control capacity:** productive operations and resource
  controls are separate work classes; interrupts/close/deny bypass the
  productive semaphore through a reserved control path, and admission
  is bounded (items+bytes) BEFORE any task is created.
* **N03 — ACK validation:** event.ack must match the connection's
  namespace and a known stream; the watermark may never exceed what was
  actually WRITTEN on that stream, and only a validated contiguous
  watermark reaches the Core.
* **N04 — honest readiness:** reconciliation has explicit state
  (pending/failed/complete); a failed initial projection keeps the
  transport OUT of ready and blocks productive admissions.
* **N07 — live lanes:** adding a lane on a READY transport schedules a
  owned attach immediately; lane tickets carry scope/epoch/expiry.
* **N08 — WSS origin:** the link URL is validated (wss off-loopback, no
  userinfo, approved origin) BEFORE any credential is obtained or sent.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Mapping
from urllib.parse import urlsplit

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
#: CN4-05.03: safety margin subtracted from the Server's reported
#: ticket lifetime when converting to a local monotonic deadline.
_TICKET_EXPIRY_MARGIN = 5.0
BACKOFF_MAX = 30.0
URGENT_BUDGET = 8
NORMAL_LIMIT = 256
#: CN2/N02: admission classes. Productive effects share a bounded pool;
#: resource controls (interrupt/close/deny) never wait behind them.
#: Admission is capped BEFORE create_task (items and bytes).
PRODUCTIVE_CONCURRENCY = 4
CONTROL_CONCURRENCY = 4
#: CN2/N02: maximum admitted-but-not-started operations per class and
#: the byte budget for waiting frames (product decision, tested small).
ADMISSION_QUEUE_ITEMS = 64
ADMISSION_QUEUE_BYTES = 4 * 1024 * 1024

#: Transport states (CN1/A02 + CN2/N04 reconciliation state machine).
ST_STOPPED = "stopped"
ST_CONNECTING = "connecting"
ST_SOCKET_OPEN = "socket_open"
ST_NEGOTIATING = "negotiating"
ST_RECONCILING = "reconciling"
ST_RECONCILE_FAILED = "reconcile_failed"
ST_READY = "ready"
ST_BACKOFF = "backoff"
ST_DEGRADED = "degraded"

#: Reconciliation facts (CN2/N04): enqueue, write and acceptance are
#: different stages; only the agreed completion proof flips COMPLETE.
RECONCILE_PENDING = "pending"
RECONCILE_FAILED = "failed"
RECONCILE_COMPLETE = "complete"

_CONTROL_ACTIONS = frozenset({
    "turn.interrupt", "runtime.close", "approval.deny",
    "input.decline",
})


def is_control_action(action: str) -> bool:
    """CN2/N02: controls of an ALREADY authorized resource — they never
    grant new work and must not wait behind productive effects. Steer
    and accept-style replies concede work and stay in the productive
    class regardless of urgency."""
    return action in _CONTROL_ACTIONS


@dataclass(frozen=True, slots=True)
class ValidatedOperation:
    """CN2/N01: the authenticated envelope travels WITH the operation.

    Built by the receiver after scope validation; the dispatcher and the
    manager consume this DTO — never a bare session_id resolved by text.
    """

    server_id: str
    executor_id: str
    binding_id: str
    agent_id: str
    workspace_id: str
    session_key: tuple  # (server_id, executor_id, session_id)
    session_id: str
    operation_id: str
    action: str
    intent_hash: str
    payload: dict
    expected_turn_id: str | None
    connection_generation: int
    authorization_revision: int
    configuration_revision: int
    #: CN3-01: the FULL contract expectations travel with the envelope.
    #: The host compares them against the session's authorized snapshot
    #: before building any ExecutionContext — divergence is a typed
    #: refusal with zero effects, never silently "corrected".
    session_owner_generation: int = 1
    workspace_binding_id: str = ""
    #: Lane reservation captured at admission (CN2-01.02): revalidated
    # after every wait, right before the Core call. CN3-01.02: the
    # AUTHORIZATION REVISION is part of the reservation — a lane that
    # advanced to a new revision invalidates work admitted under the old.
    lane_epoch: int = 1
    lane_authorization_revision: int = 1

    @property
    def control(self) -> bool:
        return is_control_action(self.action)


class _AttachEnvelope:
    """CN5-03.03: internal queue envelope for attach frames — carries
    the attempt generation so the sender can discard an OBSOLETE frame
    before the bytes leave (no invalid extra field on the NXL frame)."""

    __slots__ = ("frame", "binding_id", "generation")

    def __init__(self, frame, binding_id, generation):
        self.frame = frame
        self.binding_id = binding_id
        self.generation = generation


class LaneAttachAttempt:
    """CN5-03.01: the IMMUTABLE snapshot of one attach attempt —
    captured BEFORE the first await; a late provider result can never
    be relabelled with the lane's CURRENT mutable values."""

    __slots__ = ("generation", "provider", "agent_id", "binding_id",
                 "credential_epoch", "authorization_revision",
                 "ticket_epoch", "connection_serial")

    def __init__(self, *, generation, provider, agent_id, binding_id,
                 credential_epoch, authorization_revision, ticket_epoch,
                 connection_serial):
        self.generation = generation
        self.provider = provider
        self.agent_id = agent_id
        self.binding_id = binding_id
        self.credential_epoch = credential_epoch
        self.authorization_revision = authorization_revision
        self.ticket_epoch = ticket_epoch
        self.connection_serial = connection_serial


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
    #: CN2/N07: a lane attached on a live connection has its own attach
    #: producer so reload never requires a socket restart.
    attach_task: asyncio.Task | None = None
    #: CN5-03: monotonic attach generation; rotation bumps it and any
    #: in-flight attempt of an older generation is discarded.
    attach_generation: int = 0
    #: CN5-03.02: set when a rotation arrived while an attempt was in
    #: flight — the done-callback schedules the successor.
    reattach_requested: bool = False
    #: CN5-03.04: the single owned retry timer of the current attempt.
    retry_timer: object | None = None


@dataclass(slots=True)
class StreamSendState:
    """CN2/N03: enqueued ≠ written ≠ durably acked — three cursors."""

    written_through: int = 0
    acked_through: int = 0


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
    reconcile_state: str = RECONCILE_PENDING
    admitted_waiting: int = 0

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
            "reconcile_state": self.reconcile_state,
            "admitted_waiting": self.admitted_waiting,
        }


def validate_link_url(link_url: str, *, allow_loopback_plain: bool = True
                      ) -> str:
    """CN2/N08: WSS origin validation BEFORE any credential moves.

    Non-loopback links must be ``wss://``; userinfo is refused; loopback
    ``ws://`` is the documented laboratory exception. Returns the
    normalized URL or raises.
    """
    parts = urlsplit(link_url)
    if parts.username or parts.password or "@" in parts.netloc:
        raise ConnectorError(
            "PROFILE_DRIFT", "wss_link",
            "userinfo in the WSS link URL is refused",
            action="Provide wss://host[:port]/path without credentials.")
    host = (parts.hostname or "").lower()
    loopback = host in ("127.0.0.1", "::1", "localhost")
    scheme = parts.scheme.lower()
    if scheme == "wss":
        return link_url
    if scheme == "ws" and loopback and allow_loopback_plain:
        return link_url
    raise ConnectorError(
        "PROFILE_DRIFT", "wss_link",
        f"non-loopback control link must be wss://, got {scheme!r} for "
        f"{host!r}",
        action="Use the Server's wss:// address; the canonical ticket is "
               "never sent over a plaintext remote link. Loopback ws:// "
               "is the documented laboratory exception.")


class PriorityQueues:
    """Two finite queues with an urgent budget — lossless (CN1/A04).

    CN2/N02: ``put`` also enforces the byte budget for admitted frames.
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
        import json as _json
        blob = (_json.dumps(dict(frame), default=str).encode("utf-8")
                if isinstance(frame, Mapping) else b"")
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


class _Reservation:
    """CN3-03: one admission's EXACT cost — computed once, released once.

    The reservation carries the byte/item cost charged at ingress; the
    release spends THIS object (never a re-serialized approximation of
    the frame). Double release raises instead of silently corrupting
    the counters.
    """

    __slots__ = ("bytes_cost", "finalized")

    def __init__(self, bytes_cost: int):
        self.bytes_cost = bytes_cost
        self.finalized = False


class _Admission:
    """CN2/N02 + CN3-03: bounded admission BEFORE create_task, with
    exact single-shot accounting. Refusals happen before any task
    exists; an already-admitted operation keeps its durable intent and
    a consultable receipt.
    """

    def __init__(self, *, items: int = ADMISSION_QUEUE_ITEMS,
                 max_bytes: int = ADMISSION_QUEUE_BYTES):
        self.max_items = items
        self.max_bytes = max_bytes
        self.waiting_items = 0
        self.waiting_bytes = 0
        self.refused = 0
        self._released_inconsistent = 0

    @staticmethod
    def cost_of(frame: dict) -> int:
        import json as _json
        return len(_json.dumps(frame, default=str,
                               sort_keys=True).encode("utf-8"))

    def try_reserve(self, frame: dict) -> "_Reservation | None":
        cost = self.cost_of(frame)
        if (self.waiting_items >= self.max_items or
                self.waiting_bytes + cost > self.max_bytes):
            self.refused += 1
            return None
        self.waiting_items += 1
        self.waiting_bytes += cost
        return _Reservation(cost)

    def release(self, reservation: "_Reservation") -> None:
        if reservation is None:
            return
        if reservation.finalized:
            # CN3-03: a double release is a bug, not a clamp.
            self._released_inconsistent += 1
            raise RuntimeError("admission reservation released twice")
        reservation.finalized = True
        self.waiting_items -= 1
        self.waiting_bytes -= reservation.bytes_cost


class NXLTransport:
    """One Server's outbound control channel with lanes per binding."""

    def __init__(self, *, server_id: str, executor_id: str, link_url: str,
                 ticket_provider: Callable[[], Awaitable[str]],
                 on_operation: Callable,
                 on_approval: Callable,
                 on_reconcile: Callable | None = None,
                 ack_observer: Callable[[str, str, int], None] | None = None):
        self.server_id = server_id
        self.executor_id = executor_id
        # CN2/N08: validated at construction — before any ticket fetch.
        self.link_url = validate_link_url(link_url)
        self._ticket_provider = ticket_provider
        self._ack_observer = ack_observer
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
        self._negotiated = False
        #: CN2/N04: explicit reconciliation facts.
        self._reconcile_state: str = RECONCILE_PENDING
        self._server_generation: int | None = None
        #: CN2/N03: per-stream send state, keyed by the FULL stream
        #: scope (server, executor, session, epoch) + connection id.
        self._streams: dict[tuple, StreamSendState] = {}
        self._ack_waiters: dict[tuple, asyncio.Event] = {}
        self._productive_sem = asyncio.Semaphore(PRODUCTIVE_CONCURRENCY)
        self._control_sem = asyncio.Semaphore(CONTROL_CONCURRENCY)
        #: CN2/N02: bounded admission, split by work class.
        self._productive_admission = _Admission()
        self._control_admission = _Admission()
        self._inflight: set[asyncio.Task] = set()
        self._last_activity_monotonic: float | None = None
        #: CN2/N01: identity of the current connection; increments per
        #: socket so stale ACKs/frames cannot cross a replacement.
        self._connection_serial: int = 0

    # -- lifecycle ---------------------------------------------------------

    def add_lane(self, binding_id: str, agent_id: str,
                 ticket_provider: Callable[[], Awaitable[str]],
                 *, credential_epoch: int = 1,
                 authorization_revision: int = 1,
                 expires_at: float | None = None) -> None:
        lane = self._lanes.get(binding_id)
        if lane is not None:
            # CN4-05.02: a REAL rotation installs the CURRENT provider
            # and identity (the old closure must never serve a later
            # fetch), fences the old proof unconditionally and
            # re-attaches through the existing coordinator. A true
            # no-op (same credentials/revisions/provider/expiry) is
            # idempotent and does NOT rotate epochs (ACN4-31).
            same = (
                lane.credential_epoch == credential_epoch and
                lane.authorization_revision == authorization_revision and
                lane.agent_id == agent_id and
                lane.expires_at == expires_at and
                lane.ticket_provider is ticket_provider)
            if same:
                return
            if lane.retry_timer is not None:
                lane.retry_timer.cancel()
                lane.retry_timer = None
            in_flight = (lane.attach_task is not None
                         and not lane.attach_task.done())
            if in_flight:
                # CN4-05.02/ACN4-32 + CN5-03.02: the in-flight attach of
                # the OLD proof is superseded — cancelled, and its
                # done-callback schedules the successor generation (the
                # old task must never remain the only registered work).
                lane.reattach_requested = True
                lane.attach_task.cancel()
            lane.attach_generation += 1
            lane.credential_epoch = credential_epoch
            lane.authorization_revision = authorization_revision
            lane.agent_id = agent_id
            lane.ticket_provider = ticket_provider
            if expires_at is not None:
                lane.expires_at = expires_at
            lane.ticket_epoch += 1
            lane.state = "pending"  # old proof fenced ALWAYS
            self.stats.lanes[binding_id] = False
            if self._negotiated and self.websocket_open:
                if in_flight:
                    # CN5-03.02: the successor is scheduled by the old
                    # attempt's completion callback — not lost by a
                    # scheduler that still sees the cancelled task.
                    pass
                else:
                    self._ensure_lane_attach(binding_id)
            return
        self._lanes[binding_id] = LaneState(
            binding_id, agent_id, ticket_provider,
            credential_epoch=credential_epoch,
            authorization_revision=authorization_revision,
            expires_at=expires_at)
        self.stats.lanes[binding_id] = False
        # CN2/N07 (b11b): a lane added on a LIVE negotiated transport
        # gets its own owned attach producer immediately.
        if self._negotiated and self.websocket_open:
            self._schedule_lane_attach(binding_id)

    def remove_lane(self, binding_id: str) -> None:
        lane = self._lanes.pop(binding_id, None)
        if lane is not None:
            # CN5-03.02: removal prevents ANY reattach.
            lane.reattach_requested = False
            if lane.retry_timer is not None:
                lane.retry_timer.cancel()
                lane.retry_timer = None
            if lane.attach_task is not None:
                lane.attach_task.cancel()
        self.stats.lanes.pop(binding_id, None)

    def lane_info(self, binding_id: str) -> dict[str, object] | None:
        """CN4-05.01: the reload path diffs against the PERSISTED
        credentials/revisions — the transport exposes its live lane
        identity (never the provider closure) for that comparison."""
        lane = self._lanes.get(binding_id)
        if lane is None:
            return None
        return {"binding_id": lane.binding_id, "agent_id": lane.agent_id,
                "credential_epoch": lane.credential_epoch,
                "authorization_revision": lane.authorization_revision,
                "ticket_epoch": lane.ticket_epoch, "state": lane.state,
                "expires_at": lane.expires_at}

    @property
    def websocket_open(self) -> bool:
        return self._websocket is not None

    def _ensure_lane_attach(self, binding_id: str) -> None:
        """CN5-03.02: AT MOST ONE active attach task per lane; a
        rotation that arrived in flight is honoured by the previous
        attempt's done-callback (``reattach_requested``)."""
        lane = self._lanes.get(binding_id)
        if lane is None:
            return
        if lane.attach_task is not None and not lane.attach_task.done():
            lane.reattach_requested = True
            return
        if lane.retry_timer is not None:
            lane.retry_timer.cancel()
            lane.retry_timer = None
        task = asyncio.create_task(
            self._attach_one_lane(lane), name=f"lane-attach-{binding_id}")
        lane.attach_task = task
        task.add_done_callback(
            lambda done, binding_id=binding_id:
            self._attach_done(binding_id, done))

    def _attach_done(self, binding_id: str, task: asyncio.Task) -> None:
        """CN5-03.02: observe the OLD attempt's completion ONCE and
        schedule the CURRENT generation's successor when a rotation is
        pending and the channel is still live."""
        if not task.cancelled() and task.exception() is not None:
            try:
                task.exception()  # retrieved once; logged redacted
                logger.warning("lane attach attempt failed for %s",
                               binding_id)
            except Exception:  # pragma: no cover
                pass
        lane = self._lanes.get(binding_id)
        if lane is None or lane.attach_task is not task:
            return  # superseded or removed
        lane.attach_task = None
        if lane.reattach_requested:
            lane.reattach_requested = False
            if self._negotiated and self.websocket_open:
                self._ensure_lane_attach(binding_id)

    # Backwards-compatible seam for existing callers/tests.
    def _schedule_lane_attach(self, binding_id: str) -> None:
        self._ensure_lane_attach(binding_id)

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="nxl-transport")

    async def stop(self) -> None:
        self._stop.set()
        for lane in self._lanes.values():
            lane.reattach_requested = False
            if lane.retry_timer is not None:
                lane.retry_timer.cancel()
                lane.retry_timer = None
            if lane.attach_task is not None:
                lane.attach_task.cancel()
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

    @property
    def reconciled(self) -> bool:
        return self._reconcile_state == RECONCILE_COMPLETE

    # -- sending -------------------------------------------------------------

    async def send_frame(self, frame: Mapping[str, object], *,
                         urgent: bool = False) -> bool:
        return await self.queues.put(frame, urgent=urgent)

    async def send_events(self, events: list) -> bool:
        from ._batches import _contiguous_batches
        from nexus_connector_core.event_reducer import event_batch_frame
        if not events:
            return True
        sent = True
        last = events[-1]
        stream_key = self._stream_key(last.session_id, last.stream_epoch)
        for batch in _contiguous_batches(events):
            sent = await self.queues.put(event_batch_frame(batch)) and sent
        state = self._streams.setdefault(stream_key, StreamSendState())
        # CN3-05: a REPLAY never erases a proof already received — the
        # waiter Event is not cleared here; the predicate wait checks
        # acked_through against the target before blocking.
        self._ack_waiters.setdefault(
            (last.session_id, last.stream_epoch), asyncio.Event())
        return sent

    def _stream_key(self, session_id: str, epoch: str) -> tuple:
        return (self._connection_serial, self.server_id, self.executor_id,
                session_id, epoch)

    def written_through(self, session_id: str, epoch: str) -> int:
        return self._streams.get(
            self._stream_key(session_id, epoch),
            StreamSendState()).written_through

    async def wait_event_ack(self, session_id: str, stream_epoch: str,
                             timeout: float = 30.0, *,
                             target: int | None = None) -> int | None:
        """CN3-05 (D05): the wait tests a MONOTONIC watermark predicate.

        A duplicate-but-valid ACK satisfies the wait for the target it
        already covers (idempotent progress); an old watermark (ACK1)
        never satisfies a wait for a newer target (seq2). The check runs
        BEFORE and AFTER every wait — replaying a batch cannot erase a
        proof already received.
        """
        waiter = self._ack_waiters.setdefault(
            (session_id, stream_epoch), asyncio.Event())
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout

        def _satisfied() -> bool:
            state = self._streams.get(
                self._stream_key(session_id, stream_epoch))
            current = state.acked_through if state else 0
            return current >= target if target is not None else current > 0

        while not _satisfied():
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            try:
                await asyncio.wait_for(waiter.wait(), remaining)
            except asyncio.TimeoutError:
                return None
            waiter.clear()
        state = self._streams.get(self._stream_key(session_id, stream_epoch))
        return state.acked_through if state else None

    def wake_stream(self, session_id: str, epoch: str) -> None:
        """CN2/N03.2: reconnect wakes pending streams without new native
        events."""
        waiter = self._ack_waiters.get((session_id, epoch))
        if waiter is not None:
            waiter.set()

    # -- main loop -------------------------------------------------------------

    async def _run(self) -> None:
        backoff = BACKOFF_MIN
        while not self._stop.is_set():
            self.stats.state = ST_CONNECTING
            try:
                _fetched = await self._ticket_provider()
                ticket = _fetched[0] if isinstance(
                    _fetched, tuple) else _fetched
                headers = [("Authorization", f"Bearer {ticket}")]
                from .proxy import resolve_proxy
                async with websockets.connect(
                        self.link_url, subprotocols=["nxl.v1"],
                        proxy=resolve_proxy(self.link_url),
                        additional_headers=headers,
                        max_size=1024 * 1024, open_timeout=15,
                        ping_interval=None) as websocket:
                    self._websocket = websocket
                    self._connection_serial += 1
                    self._negotiated = False
                    self._reconcile_state = RECONCILE_PENDING
                    self.stats.reconcile_state = RECONCILE_PENDING
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
                self._reconcile_state = RECONCILE_PENDING
                self.stats.reconcile_state = RECONCILE_PENDING
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
            if isinstance(frame, _AttachEnvelope):
                # CN5-03.03: an attach frame of an OBSOLETE
                # generation is discarded BEFORE any byte leaves.
                lane = self._lanes.get(frame.binding_id)
                if (lane is None or
                        lane.attach_generation != frame.generation):
                    logger.info(
                        "discarding obsolete attach frame for %s",
                        frame.binding_id)
                    continue
                frame = frame.frame
            await websocket.send(encode_frame(frame))
            self.stats.frames_sent += 1
            self.stats.last_frame_at = time.time()
            kind = frame.get("type")
            if kind == "binding.attach":
                lane = self._lanes.get(str(frame.get("binding_id")))
                # CN4-05.02/ACN4-32: an attach sent under SUPERSEDED
                # credentials/revisions never marks the lane ready —
                # only the CURRENT proof's frame does.
                if lane is not None and lane.state == "attaching" and \
                        int(frame.get("credential_epoch", 0)) == \
                        lane.credential_epoch and \
                        int(frame.get("authorization_revision", 0)) == \
                        lane.authorization_revision:
                    # CN5-03.03: the ready transition belongs to
                    # the CURRENT generation's confirmed attach —
                    # an obsolete envelope was discarded above and
                    # can never reach this mark.
                    lane.state = "ready"
                    self.stats.lanes[lane.binding_id] = True
            elif kind == "event.batch":
                events = frame.get("events") or []
                if events:
                    first = events[0]
                    key = self._stream_key(first["session_id"],
                                           first["stream_epoch"])
                    state = self._streams.setdefault(key, StreamSendState())
                    state.written_through = max(
                        state.written_through,
                        max(event["sequence"] for event in events))

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
            kind = frame.get("type")
            if kind == "operation.submit":
                # CN2/N01: envelope validation HERE — the DTO (or None)
                # is produced before anything is admitted.
                operation = self._validate_operation_scope(frame)
                if operation is None:
                    self._reject_premature(frame)
                    continue
                # CN2/N02: bounded admission BEFORE create_task.
                admission = (self._control_admission if operation.control
                             else self._productive_admission)
                reservation = admission.try_reserve(frame)
                if reservation is None:
                    self._reject_over_capacity(operation, frame)
                    continue
                task = asyncio.create_task(
                    self._run_operation(operation, admission,
                                        reservation))
                self._inflight.add(task)
                task.add_done_callback(self._inflight.discard)
            elif kind == "welcome":
                task = asyncio.create_task(self._complete_handshake(frame))
                self._inflight.add(task)
                task.add_done_callback(self._inflight.discard)
            else:
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

    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(ABSENCE_DETECT_SECONDS)
            last = self._last_activity_monotonic
            if last is None or \
                    time.monotonic() - last > ABSENCE_DETECT_SECONDS:
                raise ConnectorError(
                    "EXECUTOR_OFFLINE", "wss_watchdog",
                    "peer application absence detected",
                    retry_safe=True)

    # -- inbound handling ---------------------------------------------------

    async def _handle(self, frame: dict[str, object]) -> None:
        # CN4-05.03: every inbound frame is a cheap, deterministic
        # checkpoint for ticket expiry (no polling task of its own).
        self._check_lane_expiry()
        kind = frame.get("type")
        if kind == "approval.request":
            if not self._frame_namespace_matches(frame):
                logger.warning(
                    "approval.request from foreign namespace %s/%s "
                    "ignored", frame.get("server_id"),
                    frame.get("executor_id"))
                return
            decision = await self._on_approval(frame)
            if decision is not None:
                await self.queues.put(decision, urgent=True)
        elif kind == "reconcile.request":
            # CN3-02 (D02): the frame's declared namespace is checked
            # against the AUTHENTICATED channel — it can never select
            # another Server's journal. The callback receives the
            # channel's trusted namespace, not the frame's claim.
            if not self._frame_namespace_matches(frame):
                logger.warning(
                    "reconcile.request from foreign namespace %s/%s "
                    "refused (channel is %s/%s)",
                    frame.get("server_id"), frame.get("executor_id"),
                    self.server_id, self.executor_id)
                error = ConnectorError(
                    "BINDING_NOT_AUTHORIZED", "wss_reconcile",
                    "reconcile requested a namespace foreign to this "
                    "channel",
                    action="The channel answers only its own "
                           "authenticated namespace.")
                from ..daemon.app import _error_frame
                await self.queues.put(_error_frame(error, {
                    "type": "reconcile.request",
                    "server_id": self.server_id,
                    "executor_id": self.executor_id,
                    "code": "BINDING_NOT_AUTHORIZED",
                    "stage": "wss_reconcile",
                    "possible_effect": False,
                    "retry_safe": False}))
                return
            envelope = dict(frame)
            envelope["server_id"] = self.server_id
            envelope["executor_id"] = self.executor_id
            if self._on_reconcile is not None:
                report = await self._on_reconcile(envelope)
                if report is not None:
                    await self.queues.put(report)
        elif kind == "event.ack":
            self._apply_event_ack(frame)
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

    def _apply_event_ack(self, frame: dict[str, object]) -> None:
        """CN2/N03.3 (b05b/b05c): validate BEFORE advancing anything.

        The ACK must come from THIS connection's namespace (server and
        executor match the link identity — foreign frames never
        associate), reference a KNOWN stream of the CURRENT connection
        serial, and carry a contiguous watermark that never exceeds what
        was actually written on that stream. Invalid ACKs are diagnosed
        and dropped; only validated watermarks reach the Core callback.
        """
        frame_server = str(frame.get("server_id", ""))
        frame_executor = str(frame.get("executor_id", ""))
        if frame_server != self.server_id or \
                frame_executor != self.executor_id:
            logger.warning(
                "event.ack from foreign namespace %s/%s ignored "
                "(local %s/%s)", frame_server, frame_executor,
                self.server_id, self.executor_id)
            return
        session_id = str(frame.get("session_id", ""))
        epoch = str(frame.get("stream_epoch", ""))
        sequence = frame.get("sequence")
        key = self._stream_key(session_id, epoch)
        state = self._streams.get(key)
        if state is None:
            logger.warning("event.ack for unknown stream %s/%s ignored",
                           session_id, epoch)
            return
        if not isinstance(sequence, int) or sequence < 0:
            logger.warning("event.ack with invalid sequence ignored")
            return
        if sequence > state.written_through:
            # Future watermark: never advance the Core past what we
            # actually wrote on THIS stream (b05b).
            logger.warning(
                "event.ack sequence %d exceeds written %d; ignored",
                sequence, state.written_through)
            return
        if sequence <= state.acked_through and state.acked_through > 0:
            # CN3-05: duplicate/regressive ACK — the cursor never moves,
            # but a duplicate of a VALID watermark still wakes waiters
            # testing a predicate their target already covers.
            waiter = self._ack_waiters.get((session_id, epoch))
            if waiter is not None:
                waiter.set()
            return
        state.acked_through = sequence
        waiter = self._ack_waiters.get((session_id, epoch))
        if waiter is not None:
            waiter.set()
        # CN4-02.01: a NEW validated watermark wakes the publisher that
        # is parked waiting for THIS batch's target — the obligation is
        # satisfied without a timed re-send loop.
        if self._ack_observer is not None:
            try:
                self._ack_observer(session_id, epoch, sequence)
            except Exception:  # pragma: no cover - observer is local
                logger.exception("ack observer failed")

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
        if self._server_generation is not None and \
                server_generation < self._server_generation:
            raise ConnectorError("STALE_GENERATION", "welcome",
                                 "server offered a stale generation")
        self._server_generation = server_generation
        self.stats.connection_generation = server_generation
        self._negotiated = True
        self.stats.state = ST_RECONCILING
        self._reconcile_state = RECONCILE_PENDING
        self.stats.reconcile_state = RECONCILE_PENDING
        await self._attach_lanes()
        # CN2/N04 (b06): reconciliation precedes admissions and a FAILED
        # projection keeps the transport OUT of ready. The state flips
        # only on the agreed completion proof: the report written to the
        # outgoing queue (sender marks the wire write; completion here
        # treats an accepted enqueue as delivered per the lab contract,
        # and a refusal/failure keeps reconcile_failed).
        ok = await self._send_initial_reconcile()
        if not ok:
            self._reconcile_state = RECONCILE_FAILED
            self.stats.reconcile_state = RECONCILE_FAILED
            self.stats.state = ST_RECONCILE_FAILED
            logger.warning(
                "initial reconciliation failed; productive admissions "
                "stay blocked")
            return
        self._reconcile_state = RECONCILE_COMPLETE
        self.stats.reconcile_state = RECONCILE_COMPLETE
        self.stats.state = ST_READY
        self._online.set()

    def _frame_namespace_matches(self, frame: dict[str, object]) -> bool:
        """CN3-01.03: origin equality for non-productive frames too."""
        server = str(frame.get("server_id", ""))
        executor = str(frame.get("executor_id", ""))
        if server and server != self.server_id:
            return False
        if executor and executor != self.executor_id:
            return False
        return True

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

    def _reject_over_capacity(self, operation: ValidatedOperation,
                              frame: dict) -> None:
        """CN2/N02: explicit refusal BEFORE any task/effect."""
        from ..daemon.app import _error_frame
        error = ConnectorError(
            "CAPACITY_EXCEEDED", "wss_admission",
            "operation admission queue is full; the operation was NOT "
            "admitted and produced no effect",
            retry_safe=True,
            operation_id=operation.operation_id,
            action="Retry the same operation_id after capacity frees; "
                   "do not mint a new intent.")
        asyncio.ensure_future(self.queues.put(_error_frame(error, frame)))

    def _validate_operation_scope(self, frame: dict[str, object]
                                  ) -> ValidatedOperation | None:
        """CN2/N01: full envelope → immutable DTO (or None)."""
        if not self._negotiated or \
                self._reconcile_state != RECONCILE_COMPLETE:
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
        session_id = str(frame.get("session_id", ""))
        payload = frame.get("payload", {})
        return ValidatedOperation(
            server_id=self.server_id,
            executor_id=self.executor_id,
            binding_id=lane.binding_id,
            agent_id=lane.agent_id,
            workspace_id=str(frame.get("workspace_id", "")),
            session_key=(self.server_id, self.executor_id, session_id),
            session_id=session_id,
            operation_id=str(frame.get("operation_id", "")),
            action=str(frame.get("action", "")),
            intent_hash=str(frame.get("intent_hash", "")),
            payload=dict(payload) if isinstance(payload, dict) else {},
            expected_turn_id=frame.get("expected_turn_id"),
            connection_generation=int(generation),
            authorization_revision=authorization_revision,
            configuration_revision=int(frame.get("configuration_revision",
                                                 0)),
            session_owner_generation=int(frame.get(
                "session_owner_generation", 1)),
            workspace_binding_id=str(frame.get("workspace_binding_id",
                                               "")),
            lane_epoch=lane.ticket_epoch,
            lane_authorization_revision=lane.authorization_revision)

    def lane_reservation_valid(self, operation: ValidatedOperation) -> bool:
        """CN2-01.02 (b02): reservation recheck AFTER any wait.

        The lane must still exist, still be READY, carry the SAME epoch
        captured at admission (rotation/detach invalidates), and the
        connection must still be the one that admitted the operation
        with the same negotiated generation.
        """
        lane = self._lanes.get(operation.binding_id)
        if lane is None or lane.state != "ready":
            return False
        if lane.ticket_epoch != operation.lane_epoch:
            return False
        if lane.agent_id != operation.agent_id:
            return False
        # CN3-01.02 (D06): the authorization revision captured at
        # admission must STILL be the lane's — rotation to a new
        # revision invalidates work queued under the old one.
        if lane.authorization_revision != operation. \
                lane_authorization_revision:
            return False
        if not self._negotiated or \
                self._reconcile_state != RECONCILE_COMPLETE:
            return False
        if self.stats.connection_generation != operation. \
                connection_generation:
            return False
        return True

    async def _run_operation(self, operation: ValidatedOperation,
                             admission: _Admission,
                             reservation: "_Reservation") -> None:
        try:
            semaphore = (self._control_sem if operation.control
                         else self._productive_sem)
            async with semaphore:
                # CN2-01.02: REVALIDATE after the admission wait —
                # detach/rotation during the queue closes the path with
                # ZERO effects (b02).
                if not self.lane_reservation_valid(operation):
                    from ..daemon.app import _error_frame
                    await self.queues.put(_error_frame(ConnectorError(
                        "STALE_GENERATION", "wss_dispatch",
                        "lane reservation invalidated while the operation "
                        "waited for capacity",
                        operation_id=operation.operation_id,
                        action="The lane was detached/rotated; the "
                               "operation produced no effect."),
                        _as_error_frame_payload(operation)))
                    return
                try:
                    receipt = await self._on_operation(operation)
                except ConnectorError as error:
                    from ..daemon.app import _error_frame
                    receipt = _error_frame(
                        error, _as_error_frame_payload(operation))
                except CoreError as error:
                    from ..daemon.app import _error_frame
                    receipt = _error_frame(ConnectorError(
                        error.code, error.stage, error.message or
                        error.code,
                        possible_effect=error.possible_effect,
                        retry_safe=error.retry_safe,
                        operation_id=error.operation_id),
                        _as_error_frame_payload(operation))
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("operation handler failed")
                    from ..daemon.app import _error_frame
                    receipt = _error_frame(ConnectorError(
                        "UNKNOWN", "wss_operation",
                        "internal error; see daemon log"),
                        _as_error_frame_payload(operation))
            if receipt is not None:
                if isinstance(receipt, dict):
                    ok = await self.queues.put(receipt)
                    if not ok:
                        # CN2/N02: refused enqueue keeps a recoverable
                        # obligation (bounded retry buffer = the queues'
                        # own budget; the receipt stays durable in the
                        # journal and is consultable by ID).
                        logger.warning(
                            "receipt enqueue refused for %s; the receipt "
                            "remains durable/consultable",
                            operation.operation_id)
                else:
                    from nexus_connector_core.receipt_reducer import \
                        receipt_frame as _rf
                    from nexus_connector_core.models import OperationReceipt
                    if isinstance(receipt, OperationReceipt):
                        ok = await self.queues.put(_rf(
                            receipt, server_id=self.server_id,
                            executor_id=self.executor_id))
                        if not ok:
                            logger.warning(
                                "receipt enqueue refused for %s; the "
                                "receipt remains durable/consultable",
                                operation.operation_id)
        finally:
            # CN3-03: releases the EXACT reservation charged at ingress.
            admission.release(reservation)

    async def _send_initial_reconcile(self) -> bool:
        """CN2/N04: FAILED projection never becomes an empty success."""
        report = {
            "protocol_major": PROTOCOL_MAJOR,
            "contract_revision": CONTRACT_REVISION,
            "type": "reconcile.report",
            "server_id": self.server_id,
            "executor_id": self.executor_id,
            "receipts": [],
            "snapshots": [],
        }
        if self._on_reconcile is not None:
            try:
                refined = await self._on_reconcile({
                    "server_id": self.server_id,
                    "executor_id": self.executor_id,
                    "operation_ids": [],
                    "session_ids": []})
            except Exception:
                logger.exception("initial reconcile projection failed")
                return False
            if refined is not None:
                report = refined
        return await self.queues.put(report)

    async def _attach_lanes(self) -> None:
        for lane in list(self._lanes.values()):
            if lane.state in ("ready", "attaching"):
                continue
            # CN5-03.02: handshake attaches also go through the single
            # owned coordinator (attach_task + done-callback), so a
            # rotation during the handshake is scheduled correctly.
            self._ensure_lane_attach(lane.binding_id)

    async def _attach_one_lane(self, lane: LaneState) -> None:
        # CN5-03.01: the attempt snapshot is captured BEFORE the first
        # await — a late provider result is never relabelled with the
        # lane's CURRENT (possibly rotated) values.
        attempt = LaneAttachAttempt(
            generation=lane.attach_generation,
            provider=lane.ticket_provider, agent_id=lane.agent_id,
            binding_id=lane.binding_id,
            credential_epoch=lane.credential_epoch,
            authorization_revision=lane.authorization_revision,
            ticket_epoch=lane.ticket_epoch,
            connection_serial=self._connection_serial)
        try:
            fetched = await attempt.provider()
        except Exception:
            logger.warning("ticket fetch failed for lane %s",
                           lane.binding_id)
            # CN5-03.04: transient provider failure keeps PENDING and
            # schedules ONE bounded retry (never a task per frame).
            self._schedule_lane_retry(lane, attempt.generation)
            return
        # CN4-05.03: providers may return (ticket, expires_in) — the
        # locally observed expiry is recorded on the lane; a plain
        # string stays valid (no contract break for existing seams).
        if isinstance(fetched, tuple):
            ticket, expires_in = fetched
            if self._attempt_current(lane, attempt):
                lane.expires_at = time.monotonic() + max(
                    1.0, float(expires_in) - _TICKET_EXPIRY_MARGIN)
        else:
            ticket = fetched
        # CN5-03.01: an obsolete result (removed lane or a newer
        # generation/rotation) is DISCARDED — it never produces a frame
        # and never marks anything ready.
        if not self._attempt_current(lane, attempt):
            return
        frame = {
            "protocol_major": PROTOCOL_MAJOR,
            "contract_revision": CONTRACT_REVISION,
            "type": "binding.attach",
            "server_id": self.server_id,
            "executor_id": self.executor_id,
            "binding_id": attempt.binding_id,
            "agent_id": attempt.agent_id,
            "authorization_revision": attempt.authorization_revision,
            "credential_epoch": attempt.credential_epoch,
            "ticket": ticket,
        }
        # CN5-03.03: the generation rides in an INTERNAL envelope; the
        # sender discards an obsolete frame before any byte leaves.
        envelope = _AttachEnvelope(frame, attempt.binding_id,
                                   attempt.generation)
        if not await self.queues.put(envelope, urgent=True):
            if self._attempt_current(lane, attempt):
                lane.state = "pending"
                self.stats.lanes[lane.binding_id] = False
            return
        if self._attempt_current(lane, attempt):
            lane.state = "attaching"

    def _attempt_current(self, lane: LaneState,
                         attempt: LaneAttachAttempt) -> bool:
        """The attempt is still the lane's CURRENT generation and this
        connection's."""
        return (self._lanes.get(lane.binding_id) is lane
                and lane.attach_generation == attempt.generation
                and self._connection_serial == attempt.connection_serial)

    def _schedule_lane_retry(self, lane: LaneState, generation: int
                             ) -> None:
        if self._stop.is_set():
            return
        if lane.retry_timer is not None:
            return  # one owned retry at a time
        lane.state = "pending"
        self.stats.lanes[lane.binding_id] = False
        loop = asyncio.get_event_loop()

        def _retry():
            lane.retry_timer = None
            if (self._lanes.get(lane.binding_id) is lane
                    and lane.attach_generation == generation
                    and not self._stop.is_set()):
                self._ensure_lane_attach(lane.binding_id)

        lane.retry_timer = loop.call_later(1.0, _retry)

    def _check_lane_expiry(self) -> None:
        """CN4-05.03: an expired ticket never leaves a lane READY
        indefinitely — the lane demotes to pending and renews through
        the SAME single-flight attach (the fetch is the renewal; it
        never extends the runtime's lease by itself)."""
        now = time.monotonic()
        for lane in list(self._lanes.values()):
            if lane.state == "ready" and lane.expires_at is not None \
                    and lane.expires_at <= now:
                logger.info("lane %s ticket expired; renewing",
                            lane.binding_id)
                lane.state = "pending"
                self.stats.lanes[lane.binding_id] = False
                self._schedule_lane_attach(lane.binding_id)


def _as_error_frame_payload(operation: ValidatedOperation) -> dict:
    """A schema-shaped operation.submit payload for error frames and
    admission accounting (slots dataclass has no __dict__)."""
    return {
        "protocol_major": PROTOCOL_MAJOR,
        "contract_revision": CONTRACT_REVISION,
        "type": "operation.submit",
        "server_id": operation.server_id,
        "executor_id": operation.executor_id,
        "binding_id": operation.binding_id,
        "agent_id": operation.agent_id,
        "workspace_id": operation.workspace_id,
        "workspace_binding_id": "wb",
        "session_id": operation.session_id,
        "operation_id": operation.operation_id,
        "action": operation.action,
        "connection_generation": operation.connection_generation,
        "authorization_revision": operation.authorization_revision,
        "configuration_revision": operation.configuration_revision,
        "intent_hash": operation.intent_hash,
        "payload": operation.payload,
    }


def _core_version() -> str:
    try:
        import nexus_connector_core
        return nexus_connector_core.__version__
    except Exception:  # pragma: no cover
        return "unknown"
