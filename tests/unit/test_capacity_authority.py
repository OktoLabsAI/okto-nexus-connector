"""Capacity uses the same approved lane, identity and local consent as opens."""
from dataclasses import replace

import pytest
from nexus_connector_core import CAPACITY_CONTRACT_REVISION, CapacityRequest, CapacityScope

from tests.unit.test_r4_daemon_execution import lifecycle, selection


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
