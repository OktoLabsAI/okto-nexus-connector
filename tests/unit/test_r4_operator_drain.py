"""Real Core/SQLite, synthetic native peer; no provider or native daemon."""
import asyncio

from nexus_connector_core import RuntimeEvent, OperationKey, OperationReceipt

from okto_nexus_connector.services.core_host import CoreRuntimeHost
from okto_nexus_connector.platform.paths import state_dir

from tests.unit.test_execution_selection import selection as selection
from tests.unit.test_r4_execution import execution as execution_fixture, observed, operation, Native

execution = execution_fixture


async def test_drain_preserves_native_decision_and_refuses_new_work_without_disconnect(execution):
    owner, connection, factory, receipts, opening = execution
    await connection.emit(opening)
    await observed(receipts, owner)
    request = dict(schema_version=1, request_id=7, request_hash='a' * 64,
        method='item/commandExecution/requestApproval',
        params={'threadId': 'thread', 'turnId': 'native-turn', 'itemId': 'item'})
    await factory.native.events_queue.put(RuntimeEvent(*factory.scope, 0, 'approval',
        request['method'], {'native_approval': request}))
    await asyncio.wait_for(factory.native.observed.wait(), 3)
    owner.begin_drain()
    owner.host.begin_drain()
    await connection.emit(operation(opening, 'turn.submit', {'text': 'not admitted'}))
    refused = await observed(receipts, owner)
    assert refused['stage'] == 'FAILED' and refused['error_code'] == 'RUNTIME_DRAINING'
    assert not factory.native.sent and connection.online
    await connection.emit(operation(opening, 'approval.decide', dict(canonical_request_id='request',
        decision_id='decision', decision_revision=1, decision='accept', request=request, response_digest=None)))
    decision = await observed(receipts, owner)
    assert decision['stage'] == 'SUBMITTED' and len(factory.native.decisions) == 1
    await connection.emit({**opening, 'operation_id': 'new-open', 'session_id': 'new-session'})
    refused_open = await observed(receipts, owner)
    assert refused_open['error_code'] == 'RUNTIME_DRAINING'
    assert len(factory.opened) == 1 and connection.lease_count == 1 and connection.online
    assert not factory.native.stopped and owner.failure is None


async def test_admitted_receipt_publication_and_controls_remain_owned_during_drain(execution, monkeypatch):
    owner, connection, factory, receipts, opening = execution
    monkeypatch.setattr(type(connection), 'pending_operations', property(lambda value: len(value.reservations)), raising=False)
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
        await connection.emit(operation(opening, 'turn.submit', {'text': 'admitted before drain'}))
        await asyncio.wait_for(entered.wait(), 15)
        owner.begin_drain()
        owner.host.begin_drain()
        assert owner.pending_count and connection.online and not factory.native.stopped
        assert await owner.drain_status() == 'BUSY'
        await connection.emit(operation(opening, 'turn.interrupt', {'reason': 'operator control'}))
        assert (await observed(receipts, owner))['operation_id'] == 'turn.interrupt'
        assert not release.is_set() and owner.pending_count
        release.set()
        assert (await observed(receipts, owner))['operation_id'] == 'turn.submit'
        assert [row[0] for row in factory.native.sent] == ['send_turn', 'interrupt']
        assert connection.online and owner.failure is None
    finally:
        release.set()


async def test_two_idle_runtime_siblings_share_one_quiescent_journal(execution):
    owner, connection, factory, receipts, opening = execution
    natives = []
    async def native_open(*_args, **_kwargs):
        native = Native()
        natives.append(native)
        return native
    factory.open = native_open
    for index in range(2):
        await connection.emit({**opening, 'operation_id': f'open-{index}', 'session_id': f'session-{index}'})
        assert (await observed(receipts, owner))['stage'] in {'SUBMITTED', 'SUCCEEDED'}
    owner.begin_drain()
    owner.host.begin_drain()
    async with asyncio.timeout(10):
        while not (await owner.host.drain_status())['quiescent']:
            await asyncio.sleep(.01)
    assert len(natives) == 2 and all(not native.stopped for native in natives)
    assert all(not native.sent for native in natives)


async def test_empty_restarted_host_does_not_erase_orphan_or_open_history_for_status(tmp_path):
    root = state_dir(tmp_path / 'runtime')
    previous = CoreRuntimeHost(root, None)
    journal = await previous.ensure_history_journal()
    key = OperationKey('server', 'executor', 'orphan-opening')
    await journal.admit(key, 'digest', 'orphan', claim_session=True, action='runtime.open')
    await journal.record_receipt(key, OperationReceipt(key.operation_id, 'digest', 'SUBMISSION_STARTED', True, False, 'orphan'))
    await journal.record_receipt(key, OperationReceipt(key.operation_id, 'digest', 'SUBMITTED', True, False, 'orphan'))
    await previous.shutdown_all()  # Dispose fixture stores; no native runtime exists.
    current = CoreRuntimeHost(root, None)
    current.begin_drain()
    assert (await current.drain_status())['state'] == 'UNKNOWN'
    assert current.journal_if_open() is None
    journal = await current.ensure_history_journal()  # Explicit owner inspection, never status.
    try:
        assert (await current.drain_status())['state'] == 'UNKNOWN'
        snapshot = await journal.drain_snapshot()
        assert len(snapshot['claims']) == 1 and snapshot['claims'][0]['operation_id'] == 'orphan-opening'
    finally:
        await current.shutdown_all()
