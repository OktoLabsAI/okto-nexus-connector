"""Execution ownership with real Core journals and a synthetic native peer."""

import asyncio
import hashlib
import secrets

import pytest
from nexus_connector_core import CoreError, RuntimeEvent, R4_PREVIEW_REVISION, encode_r4_frame, r4_submit_intent_hash
from nexus_connector_core.protocol import canonical_json

from okto_nexus_connector.services.core_host import CoreRuntimeHost
from okto_nexus_connector.services.execution_selection import acknowledge_execution_binding
from okto_nexus_connector.services.r4_execution import R4ExecutionOwner, R4LaunchSetup
from okto_nexus_connector.platform.paths import state_dir
from okto_nexus_connector.transport.r4_connection import ReceivedOperation
from okto_nexus_connector.transport.wss_r4 import R4ControlState
from tests.unit.test_execution_selection import selection


class Connection:
    def __init__(self, frame):
        self.state = R4ControlState(frame['server_id'], frame['executor_id'],
            frame['connection_id'], frame['connection_generation'], True)
        self.queues = {False: asyncio.Queue(), True: asyncio.Queue()}
        self.reservations = {}
        self.online = True
        self.lease_count = 0

    async def receive_operation(self, *, control=False):
        return await self.queues[control].get()

    def require_current(self, item):
        if not self.online or self.reservations.get(item.token) is not item:
            raise CoreError('STALE_GENERATION', 'test_connection')
        return item.frame

    def release_operation(self, item):
        assert self.reservations.pop(item.token) is item

    async def close(self):
        self.online = False

    async def apply_lease(self, runtime, *, scope, grant_id):
        self.lease_count += 1
        attempt = await runtime.begin_r4_lease_request(scope=scope, grant_id=grant_id,
            connection_id=self.state.connection_id, connection_generation=self.state.connection_generation,
            purpose='initial')
        return await runtime.install_r4_lease(attempt, dict(protocol_major=1,
            contract_revision=R4_PREVIEW_REVISION, type='lease.granted',
            request_id=attempt.request_id, grant_id=grant_id, scope=scope, lease_id='lease',
            lease_serial=1, valid_for_ms=60000,
            allowed_actions=['runtime.open', 'turn.submit', 'turn.steer', 'turn.interrupt',
                             'runtime.close', 'approval.decide', 'input.provide']))

    async def emit(self, frame):
        frame['intent_hash'] = r4_submit_intent_hash(frame)
        control = frame['action'] not in ('runtime.open', 'turn.submit')
        item = ReceivedOperation(secrets.token_hex(8), encode_r4_frame(frame), control, 'lane')
        self.reservations[item.token] = item
        await self.queues[control].put(item)


class Native:
    native_id = 'synthetic-native'
    active_turn_id = 'native-turn'

    def __init__(self):
        self.sent = []
        self.events_queue = asyncio.Queue()
        self.stopped = False
        self.observed = asyncio.Event()
        self.decisions = []

    async def send(self, verb, payload, operation_id, *, expected_turn_id=None):
        self.sent.append((verb, payload, operation_id, expected_turn_id))

    async def events(self):
        while (event := await self.events_queue.get()) is not None:
            yield event
            self.observed.set()

    async def reply_native_approval(self, request, decision, response):
        self.decisions.append((request, decision, response))

    async def close(self):
        self.stopped = True
        await self.events_queue.put(None)
        return 'graceful'

    async def observe(self):
        return ('STOPPED' if self.stopped else 'RUNNING', 'IDLE')


class Factory:
    def __init__(self):
        self.native = Native()
        self.opened = []

    async def open(self, prepared, session_id, context, *, stream_epoch):
        self.opened.append(prepared)
        self.scope = (context.server_id, context.executor_id, session_id, stream_epoch)
        return self.native


@pytest.fixture
async def execution(selection, tmp_path):
    store, candidate, binding, opening, *_ = selection
    acknowledge_execution_binding(store, binding=binding)
    connection = Connection(opening)
    host = CoreRuntimeHost(state_dir(tmp_path / 'runtime'), None)
    factory = Factory()
    receipts = asyncio.Queue()
    async def candidates(frame):
        return [candidate]
    async def environment(_):
        return {}
    async def launch(frame):
        return R4LaunchSetup(environment)
    owner = R4ExecutionOwner(connection, store, host, candidate_provider=candidates,
        launch_provider=launch, publish_receipt=receipts.put, native_factory=factory)
    owner.start()
    try:
        yield owner, connection, factory, receipts, opening
    finally:
        await owner.stop()
        await connection.close()
        await host.shutdown_all()


def operation(opening, action, payload, **extra):
    return {**opening, 'operation_id': action, 'action': action, 'payload': payload,
            'expected_turn_id': None, **extra}


async def observed(queue, owner):
    async with asyncio.timeout(3):
        while queue.empty():
            if owner.failure is not None:
                raise owner.failure
            await asyncio.sleep(0)
        return queue.get_nowait()


async def failed(owner):
    async with asyncio.timeout(3):
        while owner.failure is None or owner.pending_count:
            await asyncio.sleep(0)


async def test_control_and_cancelled_stop_do_not_abandon_receipt_producer(execution):
    owner, connection, factory, receipts, opening = execution
    await connection.emit(opening)
    await observed(receipts, owner)
    entered, release = asyncio.Event(), asyncio.Event()
    async def publish(frame):
        if frame['operation_id'] == 'turn.submit':
            entered.set()
            await release.wait()
        await receipts.put(frame)
    owner.publish_receipt = publish
    try:
        await connection.emit(operation(opening, 'turn.submit', {'text': 'Hello'}))
        await asyncio.wait_for(entered.wait(), 3)
        await connection.emit(operation(opening, 'turn.interrupt', {'reason': 'Operator requested stop.'}))
        assert (await observed(receipts, owner))['operation_id'] == 'turn.interrupt'
        stopping = asyncio.create_task(owner.stop())
        await asyncio.sleep(0)
        stopping.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopping
        assert owner.pending_count == 1 and len(connection.reservations) == 1
        assert [row[0] for row in factory.native.sent] == ['send_turn', 'interrupt']
        release.set()
        assert (await observed(receipts, owner))['operation_id'] == 'turn.submit'
        await owner.stop()
        assert owner.pending_count == 0 and not connection.reservations
    finally:
        release.set()


async def test_lane_invalidated_during_launch_wait_never_opens(execution):
    owner, connection, factory, receipts, opening = execution
    entered, release = asyncio.Event(), asyncio.Event()
    launch = owner.launch_provider
    async def blocked(frame):
        entered.set()
        await release.wait()
        return await launch(frame)
    owner.launch_provider = blocked
    try:
        await connection.emit(opening)
        await asyncio.wait_for(entered.wait(), 3)
        await connection.close()
        release.set()
        await failed(owner)
        assert not factory.opened and connection.lease_count == 0 and receipts.empty()
        assert owner.failure.code == 'STALE_GENERATION'
    finally:
        release.set()


async def test_publication_failure_fences_without_repeating_native_open(execution):
    owner, connection, factory, _, opening = execution
    published = []
    async def rejected(frame):
        published.append(frame)
        raise OSError('Receipt storage is unavailable.')
    owner.publish_receipt = rejected
    await connection.emit(opening)
    await failed(owner)
    assert not connection.online and len(factory.opened) == 1
    assert len(published) == 1 and published[0]['operation_id'] == opening['operation_id']
    assert not connection.reservations


async def test_frozen_open_selection_and_typed_control_projection(execution):
    owner, connection, factory, receipts, opening = execution
    candidates = owner.candidate_provider
    async def mutating(frame):
        frame['payload']['mode'] = 'attach'
        return await candidates(frame)
    owner.candidate_provider = mutating
    opening['payload']['model'] = 'approved-model'
    await connection.emit(opening)
    assert (await observed(receipts, owner))['operation_id'] == 'open'
    assert factory.opened[0].intent.mode == 'managed'
    assert factory.opened[0].intent.model == 'approved-model'
    await connection.emit(operation(opening, 'turn.steer', {'text': 'Continue.'},
                                    expected_turn_id='native-turn'))
    assert (await observed(receipts, owner))['operation_id'] == 'turn.steer'
    assert factory.native.sent[-1][3] == 'native-turn'
    await connection.emit(operation(opening, 'runtime.close',
        {'reason': 'Finished.', 'drain_seconds': 1, 'interrupt_seconds': 1}))
    assert (await observed(receipts, owner))['operation_id'] == 'runtime.close'
    assert factory.native.stopped


async def test_duplicate_open_never_requests_a_second_lease(execution):
    owner, connection, factory, receipts, opening = execution
    await connection.emit(opening)
    await observed(receipts, owner)
    await connection.emit(opening)
    await failed(owner)
    assert owner.failure.code == 'OPERATION_CONFLICT'
    assert len(factory.opened) == connection.lease_count == 1


async def test_cross_binding_control_cannot_borrow_a_runtime(execution):
    owner, connection, factory, receipts, opening = execution
    await connection.emit(opening)
    await observed(receipts, owner)
    await connection.emit(operation(opening, 'turn.interrupt', {'reason': 'Stop.'}, binding_id='other'))
    await failed(owner)
    assert owner.failure.code == 'SESSION_UNKNOWN'
    assert not factory.native.sent


@pytest.mark.parametrize('action', ['approval.decide', 'input.provide'])
async def test_native_decision_uses_observed_request_and_same_receipt(execution, action):
    owner, connection, factory, receipts, opening = execution
    await connection.emit(opening)
    await observed(receipts, owner)
    request = dict(schema_version=1, request_id=7, request_hash='a' * 64,
        method='item/tool/requestUserInput' if action == 'input.provide' else 'item/commandExecution/requestApproval',
        params={'threadId': 'thread', 'turnId': 'native-turn', 'itemId': 'item'})
    await factory.native.events_queue.put(RuntimeEvent(*factory.scope, 0, 'approval',
        request['method'], {'native_approval': request}))
    await asyncio.wait_for(factory.native.observed.wait(), 3)
    payload = dict(canonical_request_id='request', decision_id='decision', decision_revision=1,
                   decision='accept', request=request, response_digest=None)
    response = None
    if action == 'input.provide':
        response = {'answers': {'question': {'answers': ['Synthetic answer']}}}
        payload.update(response_ref='protected-response',
            response_digest='sha256:' + hashlib.sha256(canonical_json(response)).hexdigest())
        async def resolve(frame):
            assert frame['payload']['response_ref'] == 'protected-response'
            return response
        owner.response_resolver = resolve
    await connection.emit(operation(opening, action, payload))
    receipt = await observed(receipts, owner)
    assert receipt['operation_id'] == action and receipt['stage'] == 'SUBMITTED'
    assert len(factory.native.decisions) == 1
    applied, decision, answer = factory.native.decisions[0]
    assert applied['request_id'] == 7 and applied['request_hash'] == 'a' * 64
    assert decision == 'accept' and answer == response


async def test_native_factory_requires_pi_before_native_open(execution):
    owner, connection, factory, receipts, opening = execution
    original = owner.launch_provider
    called = []
    async def launch(frame):
        setup = await original(frame)
        return R4LaunchSetup(setup.environment, native_action_factory=lambda runtime: called.append(runtime))
    owner.launch_provider = launch
    await connection.emit(opening)
    await failed(owner)
    assert owner.failure.code == "VALIDATION_ERROR"
    assert not called and not factory.opened and not connection.lease_count


async def test_close_fences_native_ingress_before_native_close(execution, monkeypatch):
    owner, connection, factory, receipts, opening = execution
    await connection.emit(opening)
    await observed(receipts, owner)
    fenced = []
    original_fence = owner.host.close_native_actions
    original_close = factory.native.close
    async def fence(key, **kwargs):
        fenced.append(key)
        return await original_fence(key, **kwargs)
    async def close():
        assert len(fenced) == 1 and fenced[0].session_id == opening["session_id"]
        return await original_close()
    monkeypatch.setattr(owner.host, "close_native_actions", fence)
    monkeypatch.setattr(factory.native, "close", close)
    await connection.emit(operation(opening, "runtime.close",
        {"reason":"Done.", "drain_seconds":1, "interrupt_seconds":1}))
    assert (await observed(receipts, owner))["operation_id"] == "runtime.close"
    assert factory.native.stopped
