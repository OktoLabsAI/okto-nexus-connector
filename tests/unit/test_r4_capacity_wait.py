"""Ordinary work waits through the real Core gate without blocking its owner."""
import asyncio
from dataclasses import replace

import pytest
from nexus_connector_core import (CAPACITY_CONTRACT_REVISION, OperationKey,
                                  RuntimeEvent, decode_capacity_frame, encode_capacity_frame)

from tests.unit.test_r4_execution import execution, selection, Native, operation, observed


def enable(execution):
    owner, connection, factory, receipts, opening = execution
    owner.host.capacity_enabled = True
    connection.state = replace(connection.state, control_capabilities=(CAPACITY_CONTRACT_REVISION,))
    flow = asyncio.Queue()
    async def publish(item, frame):
        source = connection.require_current(item)
        assert frame['operation_id'] == source['operation_id']
        await flow.put(decode_capacity_frame(encode_capacity_frame(frame)))
    connection.publish_capacity_dispatch_state = publish
    return flow


async def matching(queue, owner, predicate):
    async with asyncio.timeout(15):
        while True:
            value = await observed(queue, owner)
            if predicate(value):
                return value


async def test_four_waiting_turns_leave_worker_and_renewal_gates_free(execution):
    flow = enable(execution)
    owner, connection, factory, receipts, opening = execution
    natives, frames = {}, []
    async def open_native(prepared, session_id, context, *, stream_epoch):
        native = natives[session_id] = Native()
        native.scope = (context.server_id, context.executor_id, session_id, stream_epoch)
        return native
    factory.open = open_native
    for index in range(5):
        frame = dict(opening, operation_id=f'open-{index}', session_id=f'session-{index}')
        frames.append(frame)
        await connection.emit(frame)
        assert (await observed(receipts, owner))['operation_id'] == frame['operation_id']
    for index, frame in enumerate(frames[:4]):
        await connection.emit(operation(frame, 'turn.submit', {'text': 'Occupy'}, operation_id=f'active-{index}'))
        assert (await observed(receipts, owner))['stage'] == 'SUBMITTED'
        await connection.emit(operation(frame, 'turn.submit', {'text': 'Wait'}, operation_id=f'wait-{index}'))
    waiting = set()
    while len(waiting) < 4:
        item = await matching(flow, owner, lambda value: value['state'] == 'WAITING')
        waiting.add(item['operation_id'])
    assert waiting == {f'wait-{index}' for index in range(4)}
    journal = await owner.host.ensure_journal()
    for op in waiting:
        assert await journal.get_receipt(OperationKey(opening['server_id'], opening['executor_id'], op)) is None
    # Renewal may acquire the same gates while those exact turns are waiting.
    async with asyncio.timeout(2):
        for session in owner._sessions.values():
            async with session.gate.renewal():
                assert session.gate.active == 0
    await connection.emit(operation(frames[4], 'turn.submit', {'text': 'Independent'}, operation_id='healthy'))
    assert (await observed(receipts, owner))['operation_id'] == 'healthy'
    await connection.emit(operation(frames[0], 'turn.interrupt', {'reason': 'Control'}, operation_id='interrupt'))
    assert (await observed(receipts, owner))['operation_id'] == 'interrupt'
    first = natives[frames[0]['session_id']]
    await first.events_queue.put(RuntimeEvent(*first.scope, 0, 'turn_state', 'turn/completed',
        {'delivery_phase': 'terminal', 'turn_id': 'active-0'}, 'active-0'))
    resumed = await matching(receipts, owner, lambda value: value['operation_id'] == 'wait-0')
    assert resumed['stage'] == 'SUBMITTED'
    assert [send[2] for send in first.sent] == ['active-0', 'interrupt', 'wait-0']
    for index in (1, 2, 3):
        assert [send[2] for send in natives[frames[index]['session_id']].sent] == [f'active-{index}']
    assert connection.online and owner.failure is None


@pytest.mark.parametrize('stop_kind', ['owner', 'session'])
async def test_stop_waiting_operation_persists_proven_no_effect(execution, stop_kind):
    flow = enable(execution)
    owner, connection, factory, receipts, opening = execution
    await connection.emit(opening)
    await observed(receipts, owner)
    await connection.emit(operation(opening, 'turn.submit', {'text': 'Active'}, operation_id='active'))
    await observed(receipts, owner)
    await connection.emit(operation(opening, 'turn.submit', {'text': 'Never sent'}, operation_id='waiting'))
    await matching(flow, owner, lambda value: value['state'] == 'WAITING')
    if stop_kind == 'owner':
        await asyncio.wait_for(owner.stop(), 5)
    else:
        await connection.emit(operation(opening, 'runtime.close',
            {'reason': 'Stop session', 'drain_seconds': 0, 'interrupt_seconds': 0}))
    receipt = await matching(receipts, owner, lambda value: value['operation_id'] == 'waiting')
    assert receipt['stage'] == 'FAILED' and receipt['error_code'] == 'RUNTIME_DRAINING'
    assert receipt['retry_safe'] and not receipt['possible_effect']
    assert all(send[2] != 'waiting' for send in factory.native.sent)
    stored = owner.publications.lookup(opening['server_id'], opening['executor_id'], ['waiting'])
    assert stored['waiting']['receipt'] == receipt
    assert owner.failure is None


async def test_open_wait_reuses_prepared_identity_and_initial_lease(execution):
    flow = enable(execution)
    owner, connection, factory, receipts, opening = execution
    owner.host.max_owned_slots = 1
    ledger = await owner.host.ensure_ledger()
    occupied = OperationKey('other-server', 'other-executor', 'opening')
    await ledger.reserve_owned_slot(occupied, 'occupied')
    calls = []
    original_candidates, original_launch = owner.candidate_provider, owner.launch_provider
    async def candidates(frame):
        calls.append('inventory')
        return await original_candidates(frame)
    async def launch(frame):
        calls.append('launch')
        return await original_launch(frame)
    owner.candidate_provider, owner.launch_provider = candidates, launch
    await connection.emit(opening)
    await matching(flow, owner, lambda value: value['state'] == 'WAITING')
    assert not factory.opened and calls == ['inventory', 'launch']
    assert connection.lease_count == 1
    journal = await owner.host.ensure_journal()
    assert await journal.get_receipt(OperationKey(opening['server_id'], opening['executor_id'], opening['operation_id'])) is None
    await ledger.release_owned_slot(occupied, 'occupied')
    receipt = await observed(receipts, owner)
    assert receipt['stage'] in ('SUBMITTED', 'SUCCEEDED')
    assert len(factory.opened) == 1 and connection.lease_count == 1
    assert calls == ['inventory', 'launch'] and owner.failure is None
