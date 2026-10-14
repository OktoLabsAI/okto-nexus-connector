"""Bounded capacity control shares the real single-reader R4 owner."""
import asyncio
import json
from dataclasses import replace

import pytest
from nexus_connector_core import (
    CAPACITY_CONTRACT_REVISION, CapacityScope, CapacityRequest, CoreError,
    decode_control_frame, encode_capacity_frame, capacity_denial_frame,
    SQLiteOwnedSlotLedger, ExecutionCapacity, execute_capacity_control,
)
from okto_nexus_connector.transport.r4_connection import R4Connection
from tests.contract.test_r4_connection import Socket, STATE, BASE, CHANNEL, SCOPE, attach, operation


CAPS = (CAPACITY_CONTRACT_REVISION,)


async def test_reader_can_cancel_absent_reservation_while_acquisition_is_delayed(tmp_path):
    from nexus_connector_core import capacity_reservation_id
    socket, entered, deliver = CapacitySocket(), asyncio.Event(), asyncio.Event()
    ledger = SQLiteOwnedSlotLedger(tmp_path / 'slots.db', max_slots=1)

    class Authority:
        async def check_capacity_scope(self, request, *, action):
            return 'AVAILABLE'

    service = ExecutionCapacity(ledger, authority=Authority(), runtime_lookup=lambda _: None, boot_id='boot')

    async def handler(frame):
        if frame['type'] == 'capacity.reserve':
            entered.set()
            await deliver.wait()
        return await execute_capacity_control(service, frame)

    owner = R4Connection(socket, replace(STATE, control_capabilities=CAPS), boot_id='boot',
        capacity_handler=handler, max_capacity_requests=1)
    owner.start()
    reserve = request_frame()
    req = CapacityRequest.from_dict(reserve['request'])
    try:
        await socket.capacity(reserve)
        await asyncio.wait_for(entered.wait(), 2)
        await socket.capacity(reserve | dict(type='capacity.release', request_id='cancel',
            reservation_id=capacity_reservation_id(req), reason='CANCELLED'))
        cancelled = await asyncio.wait_for(socket.outgoing.get(), 2)
        assert cancelled['request_id'] == 'cancel'
        assert cancelled['result']['state'] == 'RELEASED'
        assert cancelled['result']['consumption'] is None
        assert not (await ledger.owned_slot_page()).reservations
        deliver.set()
        late = await asyncio.wait_for(socket.outgoing.get(), 2)
        assert late['request_id'] == reserve['request_id']
        assert late['result'] == cancelled['result']
        assert owner.online and not (await ledger.owned_slot_page()).reservations
    finally:
        deliver.set()
        await owner.close()
        await service.aclose()
        ledger.close()


class CapacitySocket(Socket):
    async def send(self, raw):
        frame = decode_control_frame(raw.encode(), capabilities=CAPS)
        if frame['type'].startswith('capacity.'):
            await self.outgoing.put(frame)
        else:
            await super().send(raw)

    async def capacity(self, frame):
        await self.incoming.put(encode_capacity_frame(frame).decode())


def request_frame(request_id='rpc', kind='capacity.reserve'):
    req = CapacityRequest(CAPACITY_CONTRACT_REVISION, 'attempt-' + request_id,
        CapacityScope(**SCOPE, connection_generation=2, boot_id='boot'),
        'new_session', 'sha256:' + 'a' * 64, 10000)
    return dict(contract_revision=CAPACITY_CONTRACT_REVISION, type=kind, server_id='srv',
        executor_id='exe', **CHANNEL, request_id=request_id, request=req.to_dict())


async def until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0)


async def test_capacity_change_publication_requires_negotiation_and_current_lane():
    socket = CapacitySocket()
    async def handler(frame):
        return capacity_denial_frame(frame, 'RECOVERING')
    owner = R4Connection(socket, replace(STATE, control_capabilities=CAPS), boot_id='boot', capacity_handler=handler)
    owner.start()
    hint = dict(binding_id='binding', boot_id='capacity-clock', revision=7,
                reason='STALE_OBSERVATION', valid_for_ms=30000)
    try:
        with pytest.raises(CoreError, match='NOT_AUTHORIZED'):
            await owner.publish_capacity_change(**hint)
        await attach(owner, socket)
        await owner.publish_capacity_change(**hint)
        frame = await socket.outgoing.get()
        assert frame['type'] == 'capacity.changed' and frame['capacity_revision'] == 7
        assert frame['connection_id'] == STATE.connection_id
        assert frame['boot_id'] == 'capacity-clock'
        assert set(frame) == {'type', 'contract_revision', 'server_id', 'executor_id', 'connection_id',
            'connection_generation', 'binding_id', 'boot_id', 'capacity_revision', 'reason', 'valid_for_ms'}
    finally:
        await owner.close()
    legacy = R4Connection(CapacitySocket(), STATE, boot_id='boot')
    legacy.start()
    try:
        with pytest.raises(CoreError, match='CAPACITY_PROTOCOL_UNSUPPORTED'):
            await legacy.publish_capacity_change(**hint)
    finally:
        await legacy.close()


async def test_busy_capacity_does_not_block_control_operations_or_other_requests():
    socket, entered, release = CapacitySocket(), asyncio.Event(), asyncio.Event()
    async def handler(frame):
        if frame['request_id'] == 'slow':
            entered.set()
            await release.wait()
        return capacity_denial_frame(frame, 'RECOVERING')
    owner = R4Connection(socket, replace(STATE, control_capabilities=CAPS), boot_id='boot',
        capacity_handler=handler, max_capacity_requests=1)
    owner.start()
    try:
        await attach(owner, socket)
        await socket.capacity(request_frame('slow'))
        await asyncio.wait_for(entered.wait(), 2)
        await socket.capacity(request_frame('excess'))
        refused = await asyncio.wait_for(socket.outgoing.get(), 2)
        assert refused['request_id'] == 'excess' and refused['result']['reason'] == 'STALE_OBSERVATION'
        # Recovery has reserved control admission, independent of acquisitions.
        await socket.capacity(request_frame('lookup', 'capacity.get'))
        assert (await asyncio.wait_for(socket.outgoing.get(), 2))['request_id'] == 'lookup'
        await socket.emit(operation(action='turn.interrupt'))
        control = await asyncio.wait_for(owner.receive_operation(control=True), 2)
        assert owner.require_current(control)['action'] == 'turn.interrupt'
        owner.release_operation(control)
        await socket.emit({**BASE, **CHANNEL, 'type': 'heartbeat'})
        await until(lambda: socket.incoming.empty())
        assert owner.online and not release.is_set() and socket.peak_readers == 1
        release.set()
        assert (await asyncio.wait_for(socket.outgoing.get(), 2))['request_id'] == 'slow'
    finally:
        release.set()
        await owner.close()


async def test_dispatch_wait_uses_current_retained_operation_and_never_releases_transport_credit():
    socket = CapacitySocket()
    async def handler(frame):
        return capacity_denial_frame(frame, 'RECOVERING')
    owner = R4Connection(socket, replace(STATE, control_capabilities=CAPS), boot_id='boot', capacity_handler=handler)
    owner.start()
    try:
        await attach(owner, socket)
        await socket.emit(operation(action='turn.submit'))
        item = await asyncio.wait_for(owner.receive_operation(), 2)
        source = owner.require_current(item)
        frame = {k: source[k] for k in ('server_id', 'executor_id', 'connection_id', 'connection_generation',
            'binding_id', 'agent_id', 'session_id', 'operation_id', 'intent_hash', 'action')} | dict(
                contract_revision=CAPACITY_CONTRACT_REVISION, type='capacity.dispatch_state', state='WAITING', sequence=1)
        for field, value in (('operation_id', 'other'), ('binding_id', 'other'), ('connection_generation', 999)):
            with pytest.raises(CoreError, match='SCOPE_MISMATCH'):
                await owner.publish_capacity_dispatch_state(item, dict(frame, **{field: value}))
        await owner.publish_capacity_dispatch_state(item, frame)
        assert await asyncio.wait_for(socket.outgoing.get(), 2) == frame
        assert owner.require_current(item) == source  # Still retained, no ACK/receipt.
        await owner.publish_capacity_dispatch_state(item, dict(frame, state='RUNNING', sequence=2))
        assert (await asyncio.wait_for(socket.outgoing.get(), 2))['state'] == 'RUNNING'
        owner.release_operation(item)
        with pytest.raises(CoreError, match='STALE_GENERATION'):
            await owner.publish_capacity_dispatch_state(item, dict(frame, sequence=3))
        assert socket.outgoing.empty() and owner.online
    finally:
        await owner.close()


@pytest.mark.parametrize('failure', ['unnegotiated', 'foreign_host', 'stale_generation'])
async def test_reader_rejects_unnegotiated_or_cross_connection_capacity(failure):
    socket, calls = CapacitySocket(), []
    async def handler(frame):
        calls.append(frame)
        return capacity_denial_frame(frame, 'RECOVERING')
    state = STATE if failure == 'unnegotiated' else replace(STATE, control_capabilities=CAPS)
    owner = R4Connection(socket, state, boot_id='boot', capacity_handler=handler)
    owner.start()
    frame = request_frame(kind='capacity.get')
    if failure == 'foreign_host':
        frame['server_id'] = frame['request']['scope']['server_id'] = 'foreign'
    if failure == 'stale_generation':
        frame['connection_generation'] = 1
    try:
        await socket.capacity(frame)
        await until(lambda: not owner.online)
        assert isinstance(owner.failure, CoreError) and not calls
    finally:
        await owner.close()


async def test_close_waiter_cancellation_retains_mutation_for_durable_reconciliation(tmp_path):
    socket, stored, release = CapacitySocket(), asyncio.Event(), asyncio.Event()
    ledger = SQLiteOwnedSlotLedger(tmp_path / 'slots.db', max_slots=1)
    class Authority:
        async def check_capacity_scope(self, request, *, action):
            return 'AVAILABLE'
    service = ExecutionCapacity(ledger, authority=Authority(), runtime_lookup=lambda _: None, boot_id='boot')
    async def handler(frame):
        result = await execute_capacity_control(service, frame)
        stored.set()
        await release.wait()
        return result
    owner = R4Connection(socket, replace(STATE, control_capabilities=CAPS), boot_id='boot', capacity_handler=handler)
    owner.start()
    frame = request_frame()
    try:
        await socket.capacity(frame)
        await asyncio.wait_for(stored.wait(), 2)
        closer = asyncio.create_task(owner.close())
        await until(lambda: not owner.online)
        closer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closer
        assert len(owner._capacity_tasks) == 1
        assert not await owner._try_resume(socket)
        release.set()
        await asyncio.wait_for(owner.close(), 2)
        assert socket.outgoing.empty()  # No late response on a fenced socket.
        ledger.close()
        ledger = SQLiteOwnedSlotLedger(tmp_path / 'slots.db', max_slots=1)
        recovered = ExecutionCapacity(ledger, authority=Authority(), runtime_lookup=lambda _: None, boot_id='new-boot')
        found = await execute_capacity_control(recovered, frame | {'type': 'capacity.get', 'connection_generation': 3})
        assert found['result']['state'] == 'HELD'
        assert len((await ledger.owned_slot_page()).reservations) == 1
        await recovered.aclose()
    finally:
        release.set()
        await owner.close()
        await service.aclose()
        ledger.close()


@pytest.mark.parametrize('failure', ['exception', 'wrong_response'])
async def test_handler_failure_is_correlated_uncertainty_without_closing_other_lanes(failure):
    socket = CapacitySocket()
    async def handler(frame):
        if failure == 'exception':
            raise OSError('private filesystem details must not leave the host')
        return capacity_denial_frame(frame, 'RECOVERING') | {'request_id': 'other'}
    owner = R4Connection(socket, replace(STATE, control_capabilities=CAPS), boot_id='boot', capacity_handler=handler)
    owner.start()
    try:
        await socket.capacity(request_frame())
        result = await asyncio.wait_for(socket.outgoing.get(), 2)
        assert result['request_id'] == 'rpc' and result['result']['reason'] == 'NEEDS_RECONCILIATION'
        assert owner.online
        assert 'private' not in str(result)
    finally:
        await owner.close()


async def test_duplicate_request_is_coalesced_and_mutating_same_wire_id_is_rejected():
    socket, entered, release, calls = CapacitySocket(), asyncio.Event(), asyncio.Event(), []
    async def handler(frame):
        calls.append(frame)
        entered.set()
        await release.wait()
        return capacity_denial_frame(frame, 'RECOVERING')
    owner = R4Connection(socket, replace(STATE, control_capabilities=CAPS), boot_id='boot', capacity_handler=handler)
    owner.start()
    frame = request_frame()
    try:
        await socket.capacity(frame)
        await entered.wait()
        await socket.incoming.put(json.dumps(frame, indent=2))
        await socket.emit({**BASE, **CHANNEL, 'type': 'heartbeat'})
        await until(lambda: socket.incoming.empty())
        assert owner.online and len(calls) == 1
        changed = request_frame()
        changed['request']['attempt_id'] = 'other-attempt'
        await socket.capacity(changed)
        await until(lambda: not owner.online)
        assert owner.failure.code == 'RESERVATION_CONFLICT' and len(calls) == 1
    finally:
        release.set()
        await owner.close()


@pytest.mark.parametrize('enabled,accepted', [(False, False), (True, False), (True, True), (False, True)])
async def test_capacity_negotiation_requires_mutual_explicit_support(enabled, accepted):
    from nexus_connector_core import SNAPSHOT_FORMAT_VERSION
    from okto_nexus_connector.transport.wss_r4 import negotiate_r4_control
    from tests.contract.test_wss_r4 import Peer, MANAGEMENT
    peer = Peer()
    original = peer.send
    async def send(raw):
        await original(raw)
        if peer.sent[-1]['type'] == 'hello':
            welcome = await peer.inbox.get()
            reconcile = await peer.inbox.get()
            welcome['control_capabilities'] = list(CAPS) if accepted else []
            await peer.inbox.put(welcome)
            await peer.inbox.put(reconcile)
    peer.send = send
    async def report(frame):
        return {**BASE, **CHANNEL, 'type': 'reconcile.report', 'reconcile_id': frame['reconcile_id'],
            'cursor': frame['cursor'], 'next_cursor': None, 'complete': True,
            'receipts': [], 'claims': [], 'stream_watermarks': [], 'ownership_facts': []}
    async def negotiate():
        return await negotiate_r4_control(peer, server_id='srv', executor_id='exe',
            management_revision=MANAGEMENT, snapshot_format=SNAPSHOT_FORMAT_VERSION,
            boot_id='boot', report_reconciliation=report, capacity_enabled=enabled)
    if accepted and not enabled:
        with pytest.raises(ValueError, match='welcome'):
            await negotiate()
    else:
        state = await negotiate()
        assert (CAPACITY_CONTRACT_REVISION in state.control_capabilities) == accepted
    assert (CAPACITY_CONTRACT_REVISION in peer.sent[0]['control_capabilities']) == enabled
