"""Capacity uses the same approved lane, identity and local consent as opens."""
from dataclasses import replace
import asyncio

import pytest
from nexus_connector_core import CAPACITY_CONTRACT_REVISION, CapacityRequest, CapacityScope

from tests.unit.test_r4_daemon_execution import lifecycle, selection


@pytest.mark.parametrize('selection', [True], indirect=True)
async def test_owner_publishes_scoped_revision_without_starting_native_or_resolving_provider(lifecycle):
    from okto_nexus_connector.daemon.r4_execution import R4DaemonExecution
    owner, http, _, frame, native, _, _, keys = lifecycle
    owner.host.capacity_enabled = True
    owner.connection.state = replace(owner.connection.state, control_capabilities=(CAPACITY_CONTRACT_REVISION,))
    emitted = asyncio.Queue()
    async def emit(**item):
        await emitted.put(item)
    owner.connection.publish_capacity_change = emit
    publishing = R4DaemonExecution(owner.control, http, native_factory=native)
    try:
        await publishing.sync()
        initial = await asyncio.wait_for(emitted.get(), 2)
        assert initial['binding_id'] == frame['binding_id'] and initial['reason'] == 'STALE_OBSERVATION'
        baseline = list(keys)
        req = request(publishing, frame)
        reserved = await publishing.capacity_control(control(publishing, frame, req))
        publishing.capacity_publisher._next_poll = 0
        await publishing.sync()
        changed = await asyncio.wait_for(emitted.get(), 2)
        assert changed['revision'] > initial['revision']
        assert changed['boot_id'] == initial['boot_id'] == publishing.capacity_boot_id
        assert not native.opened and keys == baseline
        await publishing.capacity_control(control(publishing, frame, req, 'capacity.release',
            reservation_id=reserved['result']['reservation_id'], reason='LOST'))
    finally:
        await publishing.close()


@pytest.mark.parametrize('selection', [True], indirect=True)
async def test_reserved_open_and_first_turn_use_the_existing_daemon_dispatch_binding(lifecycle):
    from nexus_connector_core import CapacityConsumption, r4_submit_intent_hash
    from tests.unit.test_r4_execution import operation, observed
    owner, _, _, frame, native, _, published, _ = lifecycle
    owner.host.capacity_enabled = True
    await owner.sync()
    opening = dict(frame, intent_hash=r4_submit_intent_hash(frame))
    turn = operation(opening, 'turn.submit', {'text': 'Reserved first turn'}, operation_id='reserved-turn')
    turn['intent_hash'] = r4_submit_intent_hash(turn)
    req = request(owner, opening)
    service = await owner.execution_capacity()
    held = await service.try_reserve_capacity(req)
    assert held.state == 'HELD' and not native.opened
    await service.consume_capacity(req, held.reservation_id,
        CapacityConsumption(turn['operation_id'], turn['intent_hash'], opening['operation_id'], opening['intent_hash']))
    await owner.connection.emit(opening)
    assert (await observed(published, owner.owner))['operation_id'] == opening['operation_id']
    await owner.connection.emit(turn)
    assert (await observed(published, owner.owner))['operation_id'] == turn['operation_id']
    assert native.native.sent == [('send_turn', {'text': 'Reserved first turn'}, 'reserved-turn', None)]
    assert len(native.opened) == 1 and owner.owner.failure is None and owner.connection.online
    assert (await service.get_capacity_reservation(req)).state == 'CONSUMED'
    ledger = await owner.host.ensure_ledger()
    assert len((await ledger.owned_slot_page()).reservations) == 1


def request(owner, frame):
    scope = {key: frame[key] for key in ('server_id', 'executor_id', 'binding_id', 'agent_id',
        'workspace_id', 'workspace_binding_id', 'session_id', 'session_owner_generation',
        'connection_generation', 'authorization_revision', 'configuration_revision', 'binding_revision', 'credential_epoch')}
    return CapacityRequest(CAPACITY_CONTRACT_REVISION, 'attempt',
        CapacityScope(**scope, boot_id=owner.capacity_boot_id), 'new_session', 'sha256:' + 'a' * 64, 10000)


def control(owner, frame, req, kind='capacity.reserve', **extra):
    return {key: frame[key] for key in ('server_id', 'executor_id', 'connection_id', 'connection_generation')} | dict(
        contract_revision=CAPACITY_CONTRACT_REVISION, type=kind, request_id='request', request=req.to_dict(), **extra)


@pytest.mark.parametrize('selection', [True], indirect=True)
async def test_reservation_checks_real_lane_and_consent_without_launch_or_secret_resolution(lifecycle):
    owner, _, store, frame, native, _, _, keys = lifecycle
    owner.host.capacity_enabled = True
    await owner.sync()
    req = request(owner, frame)
    baseline = list(keys)
    assert await owner.check_capacity_scope(req, action='reserve') == 'AVAILABLE'
    result = await owner.capacity_control(control(owner, frame, req))
    assert result['result']['state'] == 'HELD'
    assert not native.opened and keys == baseline and not store.load().session_capabilities
    # Revoking a lane prevents new work, while the authenticated host can
    # still look up/cancel its exact old reservation without execution rights.
    store.update(lambda state: setattr(state.execution_bindings[0], 'state', 'REVOKED'))
    assert await owner.check_capacity_scope(req, action='reserve') != 'AVAILABLE'
    found = await owner.capacity_control(control(owner, frame, req, 'capacity.get'))
    assert found['result']['reservation_id'] == result['result']['reservation_id']
    released = await owner.capacity_control(control(owner, frame, req, 'capacity.release',
        reservation_id=result['result']['reservation_id'], reason='REVOKED'))
    assert released['result']['state'] == 'RELEASED'
    assert not native.opened and keys == baseline


@pytest.mark.parametrize('selection', [True], indirect=True)
async def test_revoked_lane_can_cancel_an_absent_attempt_without_native_or_provider_effect(lifecycle):
    from nexus_connector_core import capacity_reservation_id
    owner, _, store, frame, native, _, _, keys = lifecycle
    owner.host.capacity_enabled = True
    await owner.sync()
    req = request(owner, frame)
    before = list(keys)
    store.update(lambda state: setattr(state.execution_bindings[0], 'state', 'REVOKED'))
    assert await owner.check_capacity_scope(req, action='reserve') != 'AVAILABLE'
    missing = await owner.capacity_control(control(owner, frame, req, 'capacity.get'))
    assert missing['result_type'] == 'not_found'
    retired = await owner.capacity_control(control(owner, frame, req, 'capacity.release',
        reservation_id=capacity_reservation_id(req), reason='CANCELLED'))
    assert retired['result']['state'] == 'RELEASED' and retired['result']['consumption'] is None
    found = await owner.capacity_control(control(owner, frame, req, 'capacity.get'))
    assert found['result'] == retired['result']
    assert not native.opened and keys == before
    ledger = await owner.host.ensure_ledger()
    assert not (await ledger.owned_slot_page()).reservations


@pytest.mark.parametrize('selection', [True], indirect=True)
@pytest.mark.parametrize('field,value', [('server_id', 'other'), ('executor_id', 'other'),
    ('binding_id', 'other'), ('agent_id', 'other'), ('workspace_id', 'other'),
    ('workspace_binding_id', 'other'), ('binding_revision', 99), ('credential_epoch', 99),
    ('configuration_revision', 99), ('authorization_revision', 99), ('connection_generation', 99)])
async def test_capacity_refuses_cross_scope_or_stale_authority(lifecycle, field, value):
    owner, _, _, frame, native, _, _, _ = lifecycle
    await owner.sync()
    req = request(owner, frame)
    req = replace(req, scope=replace(req.scope, **{field: value}))
    assert await owner.check_capacity_scope(req, action='reserve') != 'AVAILABLE'
    assert not native.opened


@pytest.mark.parametrize('selection', [True], indirect=True)
@pytest.mark.parametrize('fault', ['consent', 'local_root', 'identity', 'expired_lane', 'old_connection'])
async def test_capacity_observation_refuses_local_drift_and_disconnected_owners(lifecycle, fault):
    owner, _, store, frame, native, _, _, _ = lifecycle
    await owner.sync()
    req = request(owner, frame)
    if fault == 'consent':
        store.update(lambda state: setattr(state.realizations[0], 'local_consent_id', 'changed'))
    elif fault == 'local_root':
        store.update(lambda state: setattr(state.realizations[0], 'workspace_root', 'changed'))
    elif fault == 'identity':
        store.update(lambda state: setattr(state.identities[0], 'credential_epoch', 99))
    elif fault == 'expired_lane':
        owner.lanes[req.scope.binding_id] = replace(owner.lanes[req.scope.binding_id], deadline=0)
    else:
        owner.control.connection = None
    assert await owner.check_capacity_scope(req, action='assess') != 'AVAILABLE'
    assert not native.opened
