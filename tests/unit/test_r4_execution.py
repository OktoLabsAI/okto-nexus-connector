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
from tests.unit.async_diagnostics import pending_task_locations


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

    async def publish_events(self, **values):
        return dict(protocol_major=1,contract_revision=R4_PREVIEW_REVISION,type='event.ack',
            server_id=self.state.server_id,executor_id=self.state.executor_id,
            connection_id=self.state.connection_id,connection_generation=self.state.connection_generation,
            **{k:values[k] for k in ('binding_id','agent_id','session_id','stream_epoch')},
            sequence=values['events'][-1]['sequence'])

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
    # This is a deadlock watchdog, not a receipt-latency requirement. The
    # installed Core opens durable journals in worker threads; hosted Windows
    # runners can spend over three seconds there before native execution.
    # Runtime lease/close deadlines remain independently enforced and tested.
    observation = asyncio.timeout(15)
    try:
        async with observation:
            while queue.empty():
                if owner.failure is not None:
                    raise owner.failure
                # Producers perform journal and filesystem work in worker threads.
                # A zero-delay spin contends for the GIL without observing any
                # additional state.
                await asyncio.sleep(.005)
            return queue.get_nowait()
    except TimeoutError as error:
        if not observation.expired():
            raise
        raise AssertionError(
            "Receipt exceeded the fifteen-second test watchdog.\n"
            + pending_task_locations()) from error


@pytest.mark.parametrize('warm_count', [2, 4, 8])
async def test_parallel_openings_do_not_block_ready_session_or_controls(execution, warm_count):
    owner, connection, factory, receipts, opening = execution
    await connection.emit(opening)
    await observed(receipts, owner)
    entered, release = asyncio.Event(), asyncio.Event()
    blocked = []

    async def slow(prepared, session_id, context, *, stream_epoch):
        blocked.append(session_id)
        if len(blocked) == warm_count:
            entered.set()
        await release.wait()
        return Native()

    factory.open = slow
    try:
        for index in range(warm_count):
            await connection.emit({**opening, 'operation_id': f'open-{index}', 'session_id': f'cold-{index}'})
        await asyncio.wait_for(entered.wait(), 15)
        await connection.emit(operation(opening, 'turn.submit', {'text': 'Ready while replenishing'}))
        receipt = await observed(receipts, owner)
        assert receipt['operation_id'] == 'turn.submit'
        assert receipt['stage'] == 'SUBMITTED'
        assert not release.is_set() and len(factory.native.sent) == 1
        await connection.emit(operation(opening, 'turn.interrupt', {'reason': 'Independent control'}))
        assert (await observed(receipts, owner))['operation_id'] == 'turn.interrupt'
        assert owner.failure is None
    finally:
        release.set()
    for _ in range(warm_count):
        assert (await observed(receipts, owner))['operation_id'].startswith('open-')


async def test_preparing_sessions_count_against_host_capacity(execution, monkeypatch):
    from okto_nexus_connector.errors import ConnectorError
    owner, connection, _, _, opening = execution
    owner.max_sessions = 1
    entered, release = asyncio.Event(), asyncio.Event()

    async def preparing(item):
        entered.set()
        await release.wait()
        return 'prepared'

    monkeypatch.setattr(owner, '_execute_owned', preparing)
    monkeypatch.setattr(connection, 'require_current', lambda item: item.frame)
    first = ReceivedOperation('a', encode_r4_frame(opening), False, 'lane')
    second_frame = {**opening, 'session_id': 'another', 'operation_id': 'another'}
    second_frame['intent_hash'] = r4_submit_intent_hash(second_frame)
    second = ReceivedOperation('b', encode_r4_frame(second_frame), False, 'lane')
    producer = asyncio.create_task(owner._execute(first))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        with pytest.raises(ConnectorError, match='capacity'):
            await owner._execute(second)
        with pytest.raises(CoreError, match='OPERATION_CONFLICT'):
            await owner._execute(first)
    finally:
        release.set()
        assert await producer == 'prepared'
    assert not owner._opening_sessions


async def failed(owner):
    try:
        # Includes cold journal/ledger initialization before the injected
        # fault. Match the receipt watchdog on loaded Windows CI runners;
        # this assertion verifies containment and durability, not latency.
        async with asyncio.timeout(15):
            while owner.failure is None or owner.pending_count:
                await asyncio.sleep(.005)
    except TimeoutError as error:
        error.add_note(
            f"Failure observation: failure_present={owner.failure is not None}, "
            f"pending_count={owner.pending_count}\n" + pending_task_locations())
        raise


@pytest.mark.parametrize('rejected', [False, True])
async def test_failed_native_open_keeps_connection_and_next_session_working(execution, rejected):
    from nexus_connector_core.models import CoreError, EffectRejected
    owner, connection, factory, receipts, opening = execution
    original = factory.open
    async def fail(*args, **kwargs):
        if rejected:
            raise EffectRejected('Handshake rejected after confirmed stop.', failure_code='NATIVE_PROTOCOL_INCOMPATIBLE')
        raise CoreError('NATIVE_VERSION_UNQUALIFIED', 'open', retry_safe=True)
    factory.open = fail
    await connection.emit(opening)
    receipt = await observed(receipts, owner)
    assert receipt['stage'] == 'FAILED'
    assert owner.failure is None and connection.online
    assert owner.execution_errors[opening['binding_id']] == receipt['error_code']
    factory.open = original
    second = {**opening, 'operation_id': 'second-open', 'session_id': 'second-session'}
    await connection.emit(second)
    receipt = await observed(receipts, owner)
    assert receipt['stage'] in ('SUBMITTED', 'SUCCEEDED')
    assert owner.failure is None and not owner.execution_errors


async def test_quota_refusals_are_durable_without_consuming_close_capacity(execution):
    from dataclasses import replace
    from nexus_connector_core import OperationKey
    from okto_nexus_connector.storage.r4_publications import R4PublicationStore
    owner, connection, factory, receipts, opening = execution
    await connection.emit(opening)
    await observed(receipts, owner)
    journal = await owner.host.ensure_journal()
    limits = journal.limits
    # Only open occupies a row; keep the remaining row reserved for close.
    journal.limits = replace(limits, max_operation_rows=2, reserved_operation_rows=1)
    for index in range(4):
        op_id = f'quota-refused-{index}'
        await connection.emit(operation(opening, 'turn.submit', {'text': 'Must not run'}, operation_id=op_id))
        refused = await observed(receipts, owner)
        assert refused['stage'] == 'FAILED' and refused['error_code'] == 'JOURNAL_FULL'
        assert refused['retry_safe'] and not refused['possible_effect']
        assert owner.failure is None and connection.online and not factory.native.stopped
        assert factory.native.sent == []
        assert await journal.get_receipt(OperationKey(opening['server_id'], opening['executor_id'], op_id)) is None
        # A fresh host store can recover exactly the refusal before any
        # transport acknowledgment; it needs no invented Core journal row.
        persisted = R4PublicationStore(owner.publications.path).lookup(
            opening['server_id'], opening['executor_id'], [op_id])
        assert persisted[op_id]['receipt'] == refused
    await connection.emit(operation(opening, 'runtime.close',
        {'reason': 'Close while productive capacity is exhausted.', 'drain_seconds': 0, 'interrupt_seconds': 1}))
    closed = await observed(receipts, owner)
    assert closed['stage'] == 'SUCCEEDED' and factory.native.stopped
    assert owner.failure is None and connection.online


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
        # Publication callback completion precedes the durable local ACK.
        async with asyncio.timeout(3):
            while any(item.frame['action'] == 'turn.interrupt' for item in connection.reservations.values()):
                await asyncio.sleep(.01)
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
    opening['payload']['mcp_preset'] = [dict(name='docs', enabled=True, transport='http',
        url='https://example.test/mcp', header_refs={})]
    opening['payload']['harness_settings'] = {'effort':'low', 'approval_policy':'on-request', 'user_input':'enabled'}
    await connection.emit(opening)
    assert (await observed(receipts, owner))['operation_id'] == 'open'
    assert factory.opened[0].intent.mode == 'managed'
    assert factory.opened[0].intent.model == 'approved-model'
    assert list(factory.opened[0].intent.mcp_preset) == opening['payload']['mcp_preset']
    assert factory.opened[0].intent.harness_settings.effort == 'low'
    assert factory.opened[0].intent.harness_settings.approval_policy == 'on-request'
    assert factory.opened[0].intent.harness_settings.user_input == 'enabled'
    await connection.emit(operation(opening, 'turn.steer', {'text': 'Continue.'},
                                    expected_turn_id='native-turn'))
    assert (await observed(receipts, owner))['operation_id'] == 'turn.steer'
    assert factory.native.sent[-1][3] == 'native-turn'
    await connection.emit(operation(opening, 'runtime.close',
        {'reason': 'Finished.', 'drain_seconds': 1, 'interrupt_seconds': 1}))
    terminal = await observed(receipts, owner)
    assert terminal['operation_id'] == 'runtime.close' and terminal['stage'] == 'SUCCEEDED'
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
    terminal = await observed(receipts, owner)
    assert terminal["operation_id"] == "runtime.close" and terminal["stage"] == "SUCCEEDED"
    assert factory.native.stopped


async def test_close_owner_publishes_after_deadline_and_retains_commit_on_stop(execution, monkeypatch):
    owner, connection, factory, receipts, opening = execution
    await connection.emit(opening)
    await observed(receipts, owner)
    runtime = next(iter(owner._sessions.values())).runtime
    entered, release = asyncio.Event(), asyncio.Event()
    record = runtime._journal.record_receipt
    async def held_record(key, receipt):
        if key.operation_id == 'runtime.close' and receipt.stage == 'SUCCEEDED':
            entered.set()
            await release.wait()
        return await record(key, receipt)
    monkeypatch.setattr(runtime._journal, 'record_receipt', held_record)
    stopping = None
    try:
        await connection.emit(operation(opening, 'runtime.close',
            {'reason': 'Done.', 'drain_seconds': .02, 'interrupt_seconds': .02}))
        await asyncio.wait_for(entered.wait(), 3)
        await asyncio.sleep(.1)
        assert owner.failure is None and receipts.empty()
        assert owner.pending_count == 1 and len(connection.reservations) == 1
        stopping = asyncio.create_task(owner.stop())
        await asyncio.sleep(0)
        stopping.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopping
        release.set()
        receipt = await observed(receipts, owner)
        assert receipt['operation_id'] == 'runtime.close' and receipt['stage'] == 'SUCCEEDED'
        await owner.stop()
        assert not connection.reservations and not owner.pending_count
    finally:
        release.set()
        if stopping is not None:
            await asyncio.gather(stopping, return_exceptions=True)


async def test_stream_registry_failure_prevents_native_open(execution,monkeypatch):
    owner,connection,factory,_,opening=execution
    def fail(scope): raise OSError("The stream registry is unavailable.")
    monkeypatch.setattr(owner.events.store,"register",fail)
    await connection.emit(opening)
    await failed(owner)
    assert not factory.opened and not connection.online
