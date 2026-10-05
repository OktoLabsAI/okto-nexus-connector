"""One reader, correlated control requests and bounded R4 operation inboxes.

This owner starts after control negotiation. It never invokes a native runtime
operation: consumers retain the exact received reservation until their durable
producer completes, and revalidate it before building the Core context.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import secrets
import time

from nexus_connector_core import (
    CoreError, R4AttachAttempt, R4LaneProjection, R4_PREVIEW_REVISION, decode_r4_frame,
    encode_r4_frame, r4_lease_renew_frame, reduce_r4_binding_attached,
)

from ..errors import ConnectorError
from .wss_r4 import R4ControlState


_CONTROL = frozenset({'turn.steer', 'turn.interrupt', 'runtime.close',
                      'approval.decide', 'input.provide'})


@dataclass(frozen=True, slots=True)
class ReceivedOperation:
    """Immutable encoded input and the exact reservation charged at ingress."""

    token: str
    encoded: bytes
    control: bool
    lane_attempt_id: str

    @property
    def frame(self) -> dict:
        return decode_r4_frame(self.encoded)


class R4Connection:
    def __init__(self, websocket, state: R4ControlState, *, boot_id: str,
                 regular_items=32, control_items=8, regular_bytes=256 * 1024,
                 control_bytes=128 * 1024, max_requests=16, max_lanes=256,
                 request_timeout=15.0, send_timeout=5.0, heartbeat_seconds=15.0, initial_lanes=None):
        limits = (regular_items, control_items, regular_bytes, control_bytes, max_requests, max_lanes)
        if (not state.control_ready or not boot_id or
                any(type(n) is not int or n <= 0 for n in limits) or
                min(request_timeout, send_timeout, heartbeat_seconds) <= 0):
            raise ValueError('Invalid R4 connection settings.')
        self.websocket, self.state, self.boot_id = websocket, state, boot_id
        self.limits = {False: (regular_items, regular_bytes), True: (control_items, control_bytes)}
        self.max_requests = max_requests
        self.max_lanes = max_lanes
        self.request_timeout, self.send_timeout = request_timeout, send_timeout
        self.heartbeat_seconds = heartbeat_seconds
        self._write_lock = asyncio.Lock()
        self._wake = {False: asyncio.Event(), True: asyncio.Event()}
        self._queues = {False: [], True: []}
        self._used = {False: [0, 0], True: [0, 0]}
        self._reservations: dict[str, ReceivedOperation] = {}
        self._lanes = {}
        self._lane_attempts = {}
        self._pending = {}
        self._event_written = {}
        self._producers = set()
        self._reader = self._heartbeat = self._closer = None
        self._closed = False
        self.failure = None
        self.close_error = None
        for binding_id,lane in (initial_lanes or {}).items():
            if (len(self._lanes)>=max_lanes or not isinstance(lane,R4LaneProjection) or
                    binding_id!=lane.binding_id or lane.boot_id!=boot_id or
                    any(getattr(lane,k)!=getattr(state,k) for k in
                        ('server_id','executor_id','connection_id','connection_generation')) or
                    time.monotonic()>=lane.deadline_monotonic):
                raise CoreError('BINDING_NOT_AUTHORIZED','r4_recovery_transfer')
            self._lanes[binding_id]=lane
            self._lane_attempts[binding_id]=lane.attach_request_id

    @property
    def online(self):
        return self._reader is not None and not self._closed

    @property
    def usage(self):
        return {key: tuple(value) for key, value in self._used.items()}

    def start(self):
        if self._reader is not None or self._closed:
            raise RuntimeError('The R4 connection owner has already started.')
        self._reader = asyncio.create_task(self._read(), name='r4-control-reader')
        self._heartbeat = asyncio.create_task(self._beat(), name='r4-heartbeat')

    def _require_online(self):
        if not self.online:
            raise ConnectorError('CONTROL_DISCONNECTED', 'r4_link',
                                 'The R4 control connection is unavailable.')

    def _base(self):
        s = self.state
        return dict(protocol_major=1, contract_revision=R4_PREVIEW_REVISION,
                    server_id=s.server_id, executor_id=s.executor_id,
                    connection_id=s.connection_id, connection_generation=s.connection_generation)

    async def send(self, raw):
        """Serialized writer compatible with the existing lease helper."""
        self._require_online()
        async with self._write_lock:
            self._require_online()
            try:
                await asyncio.wait_for(self.websocket.send(raw), self.send_timeout)
            except Exception as error:
                self._fence(error)
                raise

    def _fence(self, error):
        if self._closed:
            return
        self.failure, self._closed = error, True
        self._lanes.clear()
        for future, _ in self._pending.values():
            if not future.done():
                future.set_exception(ConnectorError('CONTROL_DISCONNECTED', 'r4_link',
                    'The control connection closed before the request completed.'))
        # Queued frames have not been handed to a consumer. Taken reservations
        # belong to that consumer until its producer is observed and released.
        for control, queue in self._queues.items():
            for item in queue:
                self.release_operation(item)
            queue.clear()
            self._wake[control].set()
        if self._closer is None:
            self._closer = asyncio.create_task(self._close_socket(), name='r4-socket-close')

    async def _close_socket(self):
        try:
            await asyncio.wait_for(self.websocket.close(), self.send_timeout)
        except Exception as error:
            # Failure is retained; socket cleanup cannot grant authority.
            self.close_error = error

    async def close(self):
        self._fence(ConnectorError('CONTROL_DISCONNECTED', 'r4_link', 'The control connection was closed.'))
        # Cancel only observers. Request/install producers remain owned and
        # are shielded even when the caller waiting for close is cancelled.
        for task in (self._reader, self._heartbeat):
            if task is not None and not task.done():
                task.cancel()
        tasks = [t for t in (self._reader, self._heartbeat, self._closer) if t is not None]
        tasks.extend(self._producers)
        if tasks:
            await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))

    async def _beat(self):
        try:
            while self.online:
                await asyncio.sleep(self.heartbeat_seconds)
                await self.send(encode_r4_frame(dict(**self._base(), type='heartbeat')).decode())
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fence(error)

    async def _read(self):
        try:
            while self.online:
                raw = await self.websocket.recv()
                encoded = raw.encode('utf-8') if isinstance(raw, str) else raw
                frame = decode_r4_frame(encoded)
                kind = frame['type']
                scope = frame['scope'] if kind == 'lease.granted' else frame
                if any(scope.get(k) != self._base()[k] for k in ('server_id', 'executor_id')):
                    raise CoreError('SCOPE_MISMATCH', 'r4_link')
                if kind != 'lease.granted' and any(frame.get(k) != self._base()[k]
                        for k in ('connection_id', 'connection_generation')):
                    raise CoreError('STALE_GENERATION', 'r4_link')
                if kind == 'operation.submit':
                    self._admit(frame, encoded)
                elif kind in ('lease.granted', 'binding.attached'):
                    field = 'request_id' if kind == 'lease.granted' else 'attach_request_id'
                    pending = self._pending.get((kind, frame[field]))
                    if pending is None or pending[0].done():
                        raise CoreError('STALE_GENERATION', 'r4_link')
                    future, validate = pending
                    # A lane ACK is validated/installed before the next frame
                    # can be admitted, including immediate Server dispatch.
                    if validate is not None:
                        validate(frame)
                    future.set_result(frame)
                elif kind == 'event.ack':
                    stream = tuple(frame[k] for k in ('binding_id','agent_id','session_id','stream_epoch'))
                    if frame['sequence'] > self._event_written.get(stream, 0):
                        raise CoreError('EVENT_GAP', 'r4_link')
                    pending = self._pending.get(('event.ack',stream))
                    if pending is not None and not pending[0].done():
                        future, validate = pending
                        if validate(frame):
                            future.set_result(frame)
                elif kind == 'error':
                    # This wire error has no request ID. It cannot safely be
                    # assigned to one of several in-flight lease requests.
                    raise ConnectorError(frame['code'], 'r4_link', 'The Server rejected a control request.')
                elif kind != 'heartbeat':
                    raise CoreError('VALIDATION_ERROR', 'r4_link')
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fence(error)

    async def _request(self, frame, *, reply_type, request_id, validate=None, finish=None, fence_on_error=True, before_send=None):
        self._require_online()
        key = (reply_type, request_id)
        if key in self._pending or len(self._producers) >= self.max_requests:
            raise ConnectorError('CAPACITY_EXCEEDED', 'r4_link', 'The control request capacity is exhausted.')
        encoded = encode_r4_frame(frame).decode('utf-8')
        future = asyncio.get_running_loop().create_future()
        self._pending[key] = (future, validate)

        async def produce():
            try:
                if before_send is not None:
                    before_send()
                await self.send(encoded)
                reply = await asyncio.wait_for(asyncio.shield(future), self.request_timeout)
                return await finish(reply) if finish is not None else reply
            except Exception as error:
                if fence_on_error:
                    self._fence(error)
                raise
            finally:
                self._pending.pop(key, None)
                if not future.done():
                    future.cancel()
                elif not future.cancelled():
                    future.exception()

        task = asyncio.create_task(produce(), name='r4-control-request')
        self._producers.add(task)
        def done(completed):
            self._producers.discard(completed)
            if not completed.cancelled():
                completed.exception()
        task.add_done_callback(done)
        return await asyncio.shield(task)

    async def publish_events(self, *, binding_id, agent_id, session_id, stream_epoch, events):
        self._require_online()
        lane = self._lanes.get(binding_id)
        if lane is None or lane.agent_id != agent_id or time.monotonic() >= lane.deadline_monotonic:
            raise CoreError('BINDING_NOT_AUTHORIZED', 'r4_event')
        stream = (binding_id,agent_id,session_id,stream_epoch)
        if stream not in self._event_written and len(self._event_written) >= 4096:
            raise ConnectorError('CAPACITY_EXCEEDED','r4_event','The event stream capacity is exhausted.')
        frame = dict(**self._base(),type='event.batch',binding_id=binding_id,agent_id=agent_id,
                     session_id=session_id,stream_epoch=stream_epoch,events=events)
        parsed = decode_r4_frame(encode_r4_frame(frame))
        if any(any(event[k] != parsed[k] for k in ('server_id','executor_id','session_id','stream_epoch'))
               for event in parsed['events']):
            raise CoreError('EVENT_SESSION_MISMATCH','r4_event')
        target = max(event['sequence'] for event in parsed['events'])
        if ('event.ack',stream) in self._pending:
            raise ConnectorError('CAPACITY_EXCEEDED','r4_event','The event stream already has an in-flight batch.')
        def prepare_send():
            if self._lanes.get(binding_id) is not lane or time.monotonic() >= lane.deadline_monotonic:
                raise CoreError('BINDING_NOT_AUTHORIZED','r4_event')
            self._event_written[stream] = max(target,self._event_written.get(stream,0))
        def validate(reply):
            if self._lanes.get(binding_id) is not lane or time.monotonic() >= lane.deadline_monotonic:
                raise CoreError('BINDING_NOT_AUTHORIZED','r4_event')
            return reply['sequence'] >= target
        return await self._request(parsed,reply_type='event.ack',request_id=stream,
                                   validate=validate,fence_on_error=False,before_send=prepare_send)

    async def attach_binding(self, *, binding_id, agent_id, ticket, credential_epoch,
                             authorization_revision, configuration_revision):
        self._require_online()
        if binding_id not in self._lane_attempts and len(self._lane_attempts) >= self.max_lanes:
            raise ConnectorError('CAPACITY_EXCEEDED', 'r4_link', 'The control lane capacity is exhausted.')
        request_id = 'attach_' + secrets.token_hex(16)
        # Invalidate the old lane before the first await. Late ACKs retain
        # their original attempt and cannot restore a rotated credential.
        self._lanes.pop(binding_id, None)
        self._lane_attempts[binding_id] = request_id
        attempt = R4AttachAttempt(request_id, self.state.server_id, self.state.executor_id,
            binding_id, agent_id, credential_epoch, authorization_revision, configuration_revision,
            self.state.connection_id, self.state.connection_generation, self.boot_id, time.monotonic())
        def validate(reply):
            projection = reduce_r4_binding_attached(attempt, reply, received_at_monotonic=time.monotonic())
            if self._lane_attempts.get(binding_id) == request_id:
                self._lanes[binding_id] = projection
        base = self._base()
        base['expected_connection_generation'] = base.pop('connection_generation')
        reply = await self._request(dict(**base, type='binding.attach',
            attach_request_id=request_id, binding_id=binding_id, agent_id=agent_id, ticket=ticket,
            credential_epoch=credential_epoch, authorization_revision=authorization_revision,
            configuration_revision=configuration_revision), reply_type='binding.attached',
            request_id=request_id, validate=validate)
        if self._lane_attempts.get(binding_id) != request_id:
            raise CoreError('STALE_GENERATION', 'r4_link')
        return reply

    async def apply_lease(self, runtime, *, scope, grant_id, purpose='initial',
                          require_current=None, fence_on_error=True):
        self._require_online()
        if scope.get('server_id') != self.state.server_id or scope.get('executor_id') != self.state.executor_id:
            raise CoreError('SCOPE_MISMATCH', 'r4_link')
        if require_current is not None:
            await require_current()
        attempt = await runtime.begin_r4_lease_request(scope=scope, grant_id=grant_id,
            connection_id=self.state.connection_id, connection_generation=self.state.connection_generation,
            purpose=purpose)
        if require_current is not None:
            await require_current()
        async def finish(grant):
            self._require_online()
            if require_current is not None:
                await require_current()
            application = await runtime.install_r4_lease(attempt, grant)
            # HTTP receipts may overtake lease.applied on another channel.
            # Replaying this SAME request after the ACK is an ordered Server
            # commit barrier, not a renewal or a new local deadline. Keep the
            # original producer and correlation slot through confirmation.
            confirmation = asyncio.get_running_loop().create_future()
            key = ('lease.granted', frame['request_id'])
            self._pending[key] = (confirmation, None)
            try:
                await self.send(encode_r4_frame(application.acknowledgement).decode())
                await self.send(encode_r4_frame(frame).decode())
                confirmed = await asyncio.wait_for(asyncio.shield(confirmation), self.request_timeout)
                if encode_r4_frame(confirmed) != encode_r4_frame(grant):
                    raise CoreError('STALE_GENERATION', 'r4_link')
            finally:
                if not confirmation.done():
                    confirmation.cancel()
                elif not confirmation.cancelled():
                    confirmation.exception()
            return application
        frame = r4_lease_renew_frame(attempt)
        return await self._request(frame, reply_type='lease.granted', request_id=frame['request_id'],
                                   finish=finish, fence_on_error=fence_on_error)

    def _lane(self, frame):
        lane = self._lanes.get(frame['binding_id'])
        if lane is None or time.monotonic() >= lane.deadline_monotonic or any(
                frame[k] != getattr(lane, k) for k in ('agent_id', 'credential_epoch',
                    'authorization_revision', 'configuration_revision')):
            raise CoreError('BINDING_NOT_AUTHORIZED', 'r4_link')
        return lane

    def is_attached(self, *, binding_id, agent_id, credential_epoch, authorization_revision, configuration_revision):
        try:
            self._lane(dict(binding_id=binding_id,agent_id=agent_id,credential_epoch=credential_epoch,
                authorization_revision=authorization_revision,configuration_revision=configuration_revision))
            return self.online
        except CoreError:
            return False

    def _admit(self, frame, encoded):
        lane = self._lane(frame)
        control = frame['action'] in _CONTROL
        used, limit = self._used[control], self.limits[control]
        if used[0] >= limit[0] or used[1] + len(encoded) > limit[1]:
            raise ConnectorError('CAPACITY_EXCEEDED', 'r4_link', 'The operation inbox capacity is exhausted.')
        item = ReceivedOperation(secrets.token_hex(16), encoded, control, lane.attach_request_id)
        used[0] += 1
        used[1] += len(encoded)
        self._reservations[item.token] = item
        self._queues[control].append(item)
        self._wake[control].set()

    async def receive_operation(self, *, control=False):
        while True:
            self._require_online()
            if self._queues[control]:
                return self._queues[control].pop(0)
            self._wake[control].clear()
            await self._wake[control].wait()

    def require_current(self, item: ReceivedOperation):
        self._require_online()
        if self._reservations.get(item.token) is not item:
            raise CoreError('STALE_GENERATION', 'r4_link')
        frame = item.frame
        if self._lane(frame).attach_request_id != item.lane_attempt_id:
            raise CoreError('STALE_GENERATION', 'r4_link')
        return frame

    def release_operation(self, item: ReceivedOperation):
        if self._reservations.get(item.token) is not item:
            raise RuntimeError('The operation reservation has already been released or belongs to another owner.')
        del self._reservations[item.token]
        self._used[item.control][0] -= 1
        self._used[item.control][1] -= len(item.encoded)
