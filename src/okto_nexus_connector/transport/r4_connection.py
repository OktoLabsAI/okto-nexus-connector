"""One reader, correlated control requests and bounded R4 operation inboxes.

This owner starts after control negotiation. It never invokes a native runtime
operation: consumers retain the exact received reservation until their durable
producer completes, and revalidate it before building the Core context.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, replace
import secrets
import time

from nexus_connector_core import (
    CoreError, R4AttachAttempt, R4LaneProjection, R4_PREVIEW_REVISION, decode_r4_frame,
    encode_r4_frame, r4_lease_renew_frame, reduce_r4_binding_attached,
    CAPACITY_CONTRACT_REVISION, CAPACITY_REQUEST_TYPES, CapacityRequest,
    decode_control_frame, encode_capacity_frame, decode_capacity_frame, capacity_denial_frame,
)

from ..errors import ConnectorError
from .lease_denial import LeaseRequestDenied
from .wss_r4 import R4ControlState
from nexus_connector_core.session_release import (
    SESSION_RELEASE_REVISION, encode_session_release, decode_session_release,
)


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
                 request_timeout=15.0, send_timeout=5.0, heartbeat_seconds=15.0, initial_lanes=None, resume=None,
                 capacity_handler=None, max_capacity_requests=8, max_capacity_recovery=4, max_capacity_replies=64):
        limits = (regular_items, control_items, regular_bytes, control_bytes, max_requests, max_lanes,
                  max_capacity_requests, max_capacity_recovery, max_capacity_replies)
        if (not state.control_ready or not boot_id or
                any(type(n) is not int or n <= 0 for n in limits) or
                min(request_timeout, send_timeout, heartbeat_seconds) <= 0):
            raise ValueError('Invalid R4 connection settings.')
        capacity_negotiated = CAPACITY_CONTRACT_REVISION in state.control_capabilities
        if capacity_negotiated and not callable(capacity_handler):
            raise ValueError('Negotiated capacity requires an installed handler.')
        self.websocket, self.state, self.boot_id = websocket, state, boot_id
        self._capacity_handler = capacity_handler if capacity_negotiated else None
        self._capacity_limits = {False: max_capacity_requests, True: max_capacity_recovery}
        self._capacity_tasks = {}
        self._capacity_replies = asyncio.Queue(maxsize=max_capacity_replies)
        self._capacity_writer = None
        self._resume = resume
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
        self._release_replies = {}
        self._event_written = {}
        self._producers = set()
        self._reader = self._heartbeat = self._closer = None
        self._closed = False
        self.failure = None
        self.close_error = None
        self.last_received_at = None
        self._last_received_monotonic = time.monotonic()
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
        if self._capacity_handler is not None:
            self._capacity_writer = asyncio.create_task(self._write_capacity(), name='r4-capacity-writer')

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

    async def publish_capacity_change(self, *, binding_id, boot_id, revision, reason, valid_for_ms):
        """Publish a scoped hint through the existing bounded socket writer."""
        if self._capacity_handler is None:
            raise CoreError('CAPACITY_PROTOCOL_UNSUPPORTED', 'capacity_publication')
        self._require_online()
        lane = self._lanes.get(binding_id)
        if lane is None or time.monotonic() >= lane.deadline_monotonic:
            raise CoreError('NOT_AUTHORIZED', 'capacity_publication')
        s = self.state
        frame = dict(contract_revision=CAPACITY_CONTRACT_REVISION, type='capacity.changed',
            server_id=s.server_id, executor_id=s.executor_id, connection_id=s.connection_id,
            connection_generation=s.connection_generation, binding_id=binding_id, boot_id=boot_id,
            capacity_revision=revision, reason=reason, valid_for_ms=valid_for_ms)
        await self.send(encode_capacity_frame(frame).decode())

    async def publish_capacity_dispatch_state(self, item, frame):
        if self._capacity_handler is None:
            raise CoreError('CAPACITY_PROTOCOL_UNSUPPORTED', 'capacity_dispatch')
        original = self.require_current(item)
        if frame['type'] != 'capacity.dispatch_state' or any(frame[k] != original[k] for k in (
                'server_id', 'executor_id', 'connection_id', 'connection_generation', 'binding_id',
                'agent_id', 'session_id', 'operation_id', 'intent_hash', 'action')):
            raise CoreError('SCOPE_MISMATCH', 'capacity_dispatch')
        await self.send(encode_capacity_frame(frame).decode())

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
            self.close_error = None
        except Exception as error:
            self.close_error = error
            # A lost peer can fail the graceful close handshake. Fence the
            # underlying transport and observe connection_lost before allowing
            # a fresh authenticated control connection. Never drop an owner
            # whose physical socket closure cannot be established.
            transport = getattr(self.websocket, 'transport', None)
            wait_closed = getattr(self.websocket, 'wait_closed', None)
            if transport is not None and callable(getattr(transport, 'abort', None)) and callable(wait_closed):
                try:
                    transport.abort()
                    await asyncio.wait_for(wait_closed(), self.send_timeout)
                except Exception as abort_error:
                    self.close_error = abort_error
                else:
                    self.close_error = None
                    logging.getLogger(__name__).warning(
                        'Control socket graceful close failed; transport closure confirmed after abort.')

    async def close(self):
        self._fence(ConnectorError('CONTROL_DISCONNECTED', 'r4_link', 'The control connection was closed.'))
        if self._closer is not None and self._closer.done() and self.close_error is not None:
            self._closer = asyncio.create_task(self._close_socket(), name='r4-socket-close-retry')
        # Cancel only observers. Request/install producers remain owned and
        # are shielded even when the caller waiting for close is cancelled.
        for task in (self._reader, self._heartbeat, self._capacity_writer):
            if task is not None and not task.done():
                task.cancel()
        tasks = [t for t in (self._reader, self._heartbeat, self._closer, self._capacity_writer) if t is not None]
        tasks.extend(self._producers)
        tasks.extend(item[0] for item in self._capacity_tasks.values())
        if tasks:
            await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))

    async def _beat(self):
        try:
            while self.online:
                await asyncio.sleep(self.heartbeat_seconds)
                if ('heartbeat_ack_v1' in self.state.control_capabilities and
                        time.monotonic() - self._last_received_monotonic > self.heartbeat_seconds * 2):
                    if not await self._try_resume(self.websocket):
                        raise ConnectorError('CONTROL_DISCONNECTED', 'r4_heartbeat', 'The Server heartbeat deadline expired.')
                await self.send(encode_r4_frame(dict(**self._base(), type='heartbeat')).decode())
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fence(error)

    async def _read(self):
        try:
            while self.online:
                socket = self.websocket
                try:
                    raw = await socket.recv()
                except Exception as error:
                    from websockets.exceptions import ConnectionClosed
                    code = getattr(getattr(error, 'rcvd', None), 'code', None)
                    if (isinstance(error, (OSError, ConnectionClosed)) and code in (None, 1000, 1001, 1006)
                            and await self._try_resume(socket)):
                        continue
                    raise
                encoded = raw.encode('utf-8') if isinstance(raw, str) else raw
                frame = decode_control_frame(encoded, capabilities=self.state.control_capabilities)
                kind = frame['type']
                release_reply = kind in ('session.release.accepted', 'session.release.deferred')
                envelope = frame['request'] if kind == 'lease.denied' or release_reply else frame
                scope = envelope['scope'] if kind in ('lease.granted', 'lease.denied') or release_reply else frame
                if any(scope.get(k) != self._base()[k] for k in ('server_id', 'executor_id')):
                    raise CoreError('SCOPE_MISMATCH', 'r4_link')
                if kind != 'lease.granted' and any(envelope.get(k) != self._base()[k]
                        for k in ('connection_id', 'connection_generation')):
                    raise CoreError('STALE_GENERATION', 'r4_link')
                self.last_received_at = time.time()
                self._last_received_monotonic = time.monotonic()
                if kind in CAPACITY_REQUEST_TYPES:
                    self._admit_capacity(frame, encoded)
                elif kind == 'operation.submit':
                    self._admit(frame, encoded)
                elif release_reply:
                    pending = self._pending.get(('session.release.accepted', envelope['release_id']))
                    if pending is None:
                        if self._release_replies.get(envelope['release_id']) != envelope:
                            raise CoreError('STALE_GENERATION', 'r4_session_release')
                        continue  # Duplicate reply after a retry/lost ACK.
                    future, validate = pending
                    if validate is None:
                        raise CoreError('SCOPE_MISMATCH', 'r4_session_release')
                    validate(frame)
                    if not future.done():
                        future.set_result(frame)
                elif kind in ('event.artifact.ack','event.artifact.rejected'):
                    pending = self._pending.get(('event.artifact.ack',frame['request_id']))
                    # A late transfer ACK has no event/operation effect. It
                    # must not disconnect healthy lanes after a timed-out upload.
                    if pending is not None and not pending[0].done():
                        future, validate = pending
                        if kind == 'event.artifact.rejected':
                            normalized = {k:v for k,v in frame.items() if k!='code'}
                            validate(dict(normalized,type='event.artifact.ack'))
                            future.set_exception(CoreError(frame['code'],'event_artifact',possible_effect=True))
                        else:
                            validate(frame)
                            future.set_result(frame)
                elif kind in ('lease.granted', 'lease.denied', 'binding.attached', 'connection.renewed'):
                    field = 'attach_request_id' if kind == 'binding.attached' else 'request_id'
                    pending = self._pending.get(('lease.granted' if kind == 'lease.denied' else kind, envelope[field]))
                    if pending is None or pending[0].done():
                        raise CoreError('STALE_GENERATION', 'r4_link')
                    future, validate = pending
                    # A lane ACK is validated/installed before the next frame
                    # can be admitted, including immediate Server dispatch.
                    if validate is not None:
                        validate(frame)
                    elif kind == 'lease.denied':
                        raise CoreError('SCOPE_MISMATCH', 'r4_lease_denied')
                    if kind == 'lease.denied':
                        future.set_exception(LeaseRequestDenied(envelope))
                    else:
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

    async def _try_resume(self, socket):
        # An uncertain request must use durable reconciliation. A quiet wire
        # may resume while native work continues under its existing lease.
        if not self._resume or self._pending or self._capacity_tasks or self._closed:
            return False
        async with self._write_lock:
            if self.websocket is not socket:
                return True
            if self._pending or self._capacity_tasks or self._closed:
                return False
            try:
                await asyncio.wait_for(socket.close(), self.send_timeout)
                replacement = await asyncio.wait_for(self._resume(self.state), 8)
            except Exception:
                return False
            if self._closed:
                await replacement.close()
                return False
            self.websocket = replacement
            self._last_received_monotonic = time.monotonic()
            logging.getLogger(__name__).info('Control transport resumed; runtime sessions and lease ownership retained.')
            return True

    def _queue_capacity_reply(self, frame):
        if not self.online:
            return
        try:
            self._capacity_replies.put_nowait(encode_capacity_frame(frame).decode('utf-8'))
        except asyncio.QueueFull:
            # A peer that does not consume replies cannot retain unlimited
            # memory. Durable mutations remain owned and are queried on reconnect.
            self._fence(CoreError('CAPACITY_EXCEEDED', 'capacity_replies'))

    async def _write_capacity(self):
        try:
            while self.online:
                reply = await self._capacity_replies.get()
                await self.send(reply)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fence(error)

    def _admit_capacity(self, frame, encoded):
        if self._capacity_handler is None:
            raise CoreError('CAPACITY_PROTOCOL_UNSUPPORTED', 'capacity_control')
        encoded = encode_capacity_frame(frame)
        request_id = frame['request_id']
        prior = self._capacity_tasks.get(request_id)
        if prior is not None:
            if prior[2] != encoded:
                raise CoreError('RESERVATION_CONFLICT', 'capacity_request_id')
            return  # One retained producer and one result for this wire request.
        recovery = frame['type'] in ('capacity.get', 'capacity.release')
        if sum(item[1] == recovery for item in self._capacity_tasks.values()) >= self._capacity_limits[recovery]:
            self._queue_capacity_reply(capacity_denial_frame(frame, 'STALE_OBSERVATION'))
            return
        # No await in the reader. Mutations keep an owner until their durable
        # result is known, including when the socket or its caller disappears.
        task = asyncio.create_task(self._serve_capacity(encoded), name='r4-capacity-request')
        self._capacity_tasks[request_id] = (task, recovery, encoded)
        def done(completed):
            if self._capacity_tasks.get(request_id, (None,))[0] is completed:
                self._capacity_tasks.pop(request_id, None)
            if not completed.cancelled():
                completed.exception()
        task.add_done_callback(done)

    async def _serve_capacity(self, encoded):
        frame = decode_capacity_frame(encoded)
        request_hash = CapacityRequest.from_dict(frame['request']).request_hash
        try:
            result = await self._capacity_handler(decode_capacity_frame(encoded))
            result = decode_capacity_frame(encode_capacity_frame(result))
            if (result['type'] != 'capacity.result' or result['request_hash'] != request_hash
                    or any(result[key] != frame[key] for key in ('server_id', 'executor_id',
                        'connection_id', 'connection_generation', 'request_id'))):
                raise CoreError('SCOPE_MISMATCH', 'capacity_response')
        except Exception:
            # Storage/handler failures are uncertain, never fabricated free
            # capacity. Do not leak exception text, credentials or local paths.
            logging.getLogger(__name__).warning('Capacity request requires durable reconciliation.')
            result = capacity_denial_frame(frame, 'NEEDS_RECONCILIATION')
        self._queue_capacity_reply(result)

    async def _request(self, frame, *, reply_type, request_id, validate=None, finish=None, fence_on_error=True,
                       before_send=None, encode=encode_r4_frame, retry_timeouts=False,
                       stop_event=None, on_retire=None):
        self._require_online()
        key = (reply_type, request_id)
        if key in self._pending or len(self._producers) >= self.max_requests:
            raise ConnectorError('CAPACITY_EXCEEDED', 'r4_link', 'The control request capacity is exhausted.')
        encoded = encode(frame).decode('utf-8')
        future = asyncio.get_running_loop().create_future()
        self._pending[key] = (future, validate)

        async def produce():
            stopping = asyncio.create_task(stop_event.wait()) if stop_event is not None else None
            try:
                if before_send is not None:
                    before_send()
                while True:
                    if stop_event is not None and stop_event.is_set():
                        raise ConnectorError('PUBLICATION_INTERRUPTED', 'r4_link',
                                             'The durable publication remains pending.')
                    await self.send(encoded)
                    try:
                        if stopping is None:
                            reply = await asyncio.wait_for(asyncio.shield(future), self.request_timeout)
                        else:
                            await asyncio.wait((future, stopping), timeout=self.request_timeout,
                                               return_when=asyncio.FIRST_COMPLETED)
                            if future.done():
                                reply = future.result()
                            elif stopping.done():
                                raise ConnectorError('PUBLICATION_INTERRUPTED', 'r4_link',
                                                     'The durable publication remains pending.')
                            else:
                                raise asyncio.TimeoutError
                        break
                    except asyncio.TimeoutError:
                        if not retry_timeouts:
                            raise
                        # Retain the original ID, bytes and observer through
                        # retry; late ACKs cannot land in another request.
                        self._require_online()
                return await finish(reply) if finish is not None else reply
            except Exception as error:
                if fence_on_error and not isinstance(error, LeaseRequestDenied):
                    self._fence(error)
                raise
            finally:
                if on_retire is not None:
                    on_retire()
                self._pending.pop(key, None)
                if not future.done():
                    future.cancel()
                elif not future.cancelled():
                    future.exception()
                if stopping is not None:
                    stopping.cancel()
                    await asyncio.gather(stopping, return_exceptions=True)

        task = asyncio.create_task(produce(), name='r4-control-request')
        self._producers.add(task)
        def done(completed):
            self._producers.discard(completed)
            if not completed.cancelled():
                completed.exception()
        task.add_done_callback(done)
        return await asyncio.shield(task)

    async def publish_session_release(self, request, *, stop_event=None):
        if SESSION_RELEASE_REVISION not in self.state.control_capabilities:
            raise CoreError('VERSION_INCOMPATIBLE', 'r4_session_release')
        request = decode_session_release(encode_session_release(request))
        if request['type'] != 'session.release' or any(request[k] != self._base()[k] for k in
                ('server_id','executor_id','connection_id','connection_generation')):
            raise CoreError('SCOPE_MISMATCH', 'r4_session_release')
        def validate(reply):
            if reply['request'] != request:
                raise CoreError('SCOPE_MISMATCH', 'r4_session_release')
        def retire():
            if len(self._release_replies) >= 256:
                self._release_replies.pop(next(iter(self._release_replies)))
            self._release_replies[request['release_id']] = request
        return await self._request(request, reply_type='session.release.accepted',
            request_id=request['release_id'], validate=validate, on_retire=retire,
            encode=encode_session_release, retry_timeouts=True, stop_event=stop_event,
            fence_on_error=False)

    async def publish_event_artifact(self, scope, event, raw):
        from nexus_connector_core.event_artifacts import EVENT_ARTIFACT_REVISION, artifact_chunks, artifact_ack, encode_event_artifact
        if EVENT_ARTIFACT_REVISION not in self.state.control_capabilities:
            raise CoreError('VERSION_INCOMPATIBLE','event_artifact')
        def current():
            self._require_online()
            lane = self._lanes.get(scope['binding_id'])
            if lane is None or lane.agent_id != scope['agent_id'] or time.monotonic() >= lane.deadline_monotonic:
                raise CoreError('BINDING_NOT_AUTHORIZED','event_artifact')
        base = dict(self._base(),binding_id=scope['binding_id'],agent_id=scope['agent_id'])
        for chunk in artifact_chunks(base,event,raw):
            current()
            expected = artifact_ack(chunk)
            def validate(reply):
                current()
                if reply != expected:
                    raise CoreError('EVENT_CONFLICT','event_artifact_ack')
                return True
            await self._request(chunk,reply_type='event.artifact.ack',request_id=chunk['request_id'],
                validate=validate,encode=encode_event_artifact,fence_on_error=False,before_send=current)

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
            current = self._lanes.get(binding_id)
            if current is None or replace(current, deadline_monotonic=lane.deadline_monotonic) != lane or time.monotonic() >= current.deadline_monotonic:
                raise CoreError('BINDING_NOT_AUTHORIZED','r4_event')
            self._event_written[stream] = max(target,self._event_written.get(stream,0))
        def validate(reply):
            current = self._lanes.get(binding_id)
            if current is None or replace(current, deadline_monotonic=lane.deadline_monotonic) != lane or time.monotonic() >= current.deadline_monotonic:
                raise CoreError('BINDING_NOT_AUTHORIZED','r4_event')
            return reply['sequence'] >= target
        return await self._request(parsed,reply_type='event.ack',request_id=stream,
                                   validate=validate,fence_on_error=False,before_send=prepare_send)

    async def renew_connection(self):
        """Extend the current proofs; retain socket, lane attempts and native owners."""
        if 'connection_renewal_v1' not in self.state.control_capabilities:
            raise ConnectorError('VERSION_INCOMPATIBLE', 'r4_renewal', 'The Server does not support connection continuity.')
        started = time.monotonic()
        request_id = 'renew_' + secrets.token_hex(16)
        def validate(reply):
            deadline = started + reply['expires_in']
            if deadline <= time.monotonic() or set(reply['binding_ids']) != set(self._lanes):
                raise CoreError('STALE_GENERATION', 'r4_renewal')
            for binding_id, lane in self._lanes.items():
                self._lanes[binding_id] = replace(lane, deadline_monotonic=deadline)
        reply = await self._request(dict(**self._base(), type='connection.renew', request_id=request_id),
            reply_type='connection.renewed', request_id=request_id, validate=validate)
        return started + reply['expires_in']

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
        frame = r4_lease_renew_frame(attempt)
        def validate_reply(reply):
            if reply['type'] == 'lease.denied' and reply['request'] != frame:
                raise CoreError('SCOPE_MISMATCH', 'r4_lease_denied')
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
            self._pending[key] = (confirmation, validate_reply)
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
        return await self._request(frame, reply_type='lease.granted', request_id=frame['request_id'],
                                   validate=validate_reply, finish=finish, fence_on_error=fence_on_error)

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
