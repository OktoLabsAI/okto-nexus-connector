"""Durable one-time credential ownership, without claiming restart authority."""

import asyncio
import copy
import time

import pytest
from nexus_connector_core import r4_submit_intent_hash

from okto_nexus_connector.errors import CapabilityMaterialUnavailable, ConnectorError
from okto_nexus_connector.identity.vault import RestrictedFileVault
from okto_nexus_connector.services.session_capabilities import SessionCapabilityOwner, CapabilityLaunchProvider
from okto_nexus_connector.storage.state_store import StateStore, state_from_json, state_to_json
from okto_nexus_connector.transport.https_client import R4SessionCapability
from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_execution import execution, observed


class Issuer:
    def __init__(self, store):
        self.store = store
        self.calls = []
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.release.set()
        self.failure = None

    async def request_r4_session_capability(self, key, **request):
        state = self.store.load()
        record = state.session_capabilities[0]
        assert record.request_id == request['capability_request_id']
        assert record.status == 'REQUESTED'
        self.calls.append(request)
        self.entered.set()
        await self.release.wait()
        if self.failure:
            raise self.failure
        frame = request['frame']
        scope = {k: frame[k] for k in ('server_id', 'executor_id', 'binding_id', 'agent_id',
            'workspace_id', 'workspace_binding_id', 'session_id', 'session_owner_generation',
            'binding_revision', 'credential_epoch', 'authorization_revision', 'configuration_revision')}
        return R4SessionCapability('cap-test', 'mcp-cap:cap-test', 'nxc4_' + 'x' * 43,
            scope, request['audience'], request['actions'], 120, time.monotonic() + 119,
            'https://nexus.test/mcp')


def setup(selection):
    store = selection[0]
    vault = RestrictedFileVault(store.path.parent, approved=True)
    owner = SessionCapabilityOwner(store, vault)
    http = Issuer(store)
    args = dict(frame=selection[3], audience='nexus-mcp-session', actions=('tools/call',),
                require_current=lambda: None)
    return store, vault, owner, http, args


@pytest.mark.asyncio
async def test_issuance_is_durable_and_vault_committed_before_return(selection):
    store, vault, owner, http, args = setup(selection)
    first, second = await asyncio.gather(*(owner.reserve(http, 'key', **args) for _ in range(2)))
    assert first == second and len(http.calls) == 1
    record = StateStore(store.path).load().session_capabilities[0]
    assert record.status == 'STORED' and record.capability_id == first.capability_id
    assert vault.resolve(record.secret_handle) == first.capability
    assert first.capability not in store.path.read_text()
    assert first.capability not in repr(first)
    with pytest.raises(TypeError):
        first.scope['agent_id'] = 'other'
    # Restart retains protected material but does not fabricate a new TTL.
    restarted = SessionCapabilityOwner(StateStore(store.path), vault)
    with pytest.raises(CapabilityMaterialUnavailable) as unavailable:
        await restarted.reserve(http, 'key', **args)
    assert not unavailable.value.recovery_allowed and len(http.calls) == 1
    await owner.close()
    await restarted.close()


@pytest.mark.asyncio
async def test_cancelled_observers_cannot_abandon_issuance(selection):
    store, vault, owner, http, args = setup(selection)
    http.release.clear()
    waiter = asyncio.create_task(owner.reserve(http, 'key', **args))
    await asyncio.wait_for(http.entered.wait(), 3)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    closing = asyncio.create_task(owner.close())
    await asyncio.sleep(0)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert owner.pending_count == 1
    http.release.set()
    await owner.close()
    record = store.load().session_capabilities[0]
    assert record.status == 'STORED' and vault.resolve(record.secret_handle)
    assert owner.pending_count == 0 and len(http.calls) == 1


@pytest.mark.asyncio
async def test_lost_response_replays_original_request_and_records_metadata(selection):
    store, vault, owner, http, args = setup(selection)
    http.failure = ConnectorError('CONTROL_DISCONNECTED', 'http', possible_effect=True)
    with pytest.raises(ConnectorError):
        await owner.reserve(http, 'key', **args)
    original = store.load().session_capabilities[0].request_id
    await owner.close()
    restarted = SessionCapabilityOwner(store, vault)
    http.failure = CapabilityMaterialUnavailable('CREDENTIAL_MATERIAL_UNAVAILABLE', 'http',
        capability_id='cap-test', recovery_allowed=False)
    with pytest.raises(CapabilityMaterialUnavailable):
        await restarted.reserve(http, 'key', **args)
    assert [c['capability_request_id'] for c in http.calls] == [original, original]
    assert all('replaces_capability_id' not in c for c in http.calls)
    record = store.load().session_capabilities[0]
    assert record.status == 'MATERIAL_UNAVAILABLE' and record.capability_id == 'cap-test'
    await restarted.close()


@pytest.mark.asyncio
async def test_vault_failure_cannot_return_material_or_issue_again(selection, monkeypatch):
    store, vault, owner, http, args = setup(selection)
    def fail(*args):
        raise OSError('Vault storage unavailable.')
    monkeypatch.setattr(vault, 'store', fail)
    with pytest.raises(OSError):
        await owner.reserve(http, 'key', **args)
    assert store.load().session_capabilities[0].status == 'MATERIAL_RECEIVED'
    restarted = SessionCapabilityOwner(store, vault)
    with pytest.raises(CapabilityMaterialUnavailable):
        await restarted.reserve(http, 'key', **args)
    assert len(http.calls) == 1
    await owner.close()
    await restarted.close()


@pytest.mark.asyncio
async def test_changed_opening_and_expired_cached_result_never_reissue(selection):
    store, vault, owner, http, args = setup(selection)
    first = await owner.reserve(http, 'key', **args)
    changed = copy.deepcopy(args['frame'])
    changed['configuration_revision'] += 1
    changed['intent_hash'] = r4_submit_intent_hash(changed)
    with pytest.raises(ConnectorError) as conflict:
        await owner.reserve(http, 'key', **{**args, 'frame': changed})
    assert conflict.value.code == 'OPERATION_CONFLICT'
    owner.clock = lambda: first.deadline_monotonic
    with pytest.raises(ConnectorError) as expired:
        await owner.reserve(http, 'key', **args)
    assert expired.value.code == 'AUTH_EXPIRED' and len(http.calls) == 1
    await owner.close()


@pytest.mark.asyncio
async def test_reserved_vault_namespace_cannot_be_retargeted(selection):
    store, vault, owner, http, args = setup(selection)
    http.failure = ConnectorError('CONTROL_DISCONNECTED', 'http', possible_effect=True)
    with pytest.raises(ConnectorError):
        await owner.reserve(http, 'key', **args)
    store.update(lambda state: setattr(state.session_capabilities[0],
                                       'secret_handle', 'vault:another-identity'))
    restarted = SessionCapabilityOwner(store, vault)
    with pytest.raises(ConnectorError) as rejected:
        await restarted.reserve(http, 'key', **args)
    assert rejected.value.code == 'OPERATION_CONFLICT' and len(http.calls) == 1
    await owner.close()
    await restarted.close()


@pytest.mark.asyncio
async def test_concurrent_replay_metadata_cannot_erase_stored_result(selection):
    store, vault, owner, _, args = setup(selection)
    replay_entered, replay_release = asyncio.Event(), asyncio.Event()
    class ReplayIssuer(Issuer):
        async def request_r4_session_capability(self, key, **request):
            if self.calls:
                assert request['capability_request_id'] == self.calls[0]['capability_request_id']
                replay_entered.set()
                await replay_release.wait()
                raise CapabilityMaterialUnavailable('CREDENTIAL_MATERIAL_UNAVAILABLE', 'http',
                    capability_id='cap-test', recovery_allowed=False)
            return await super().request_r4_session_capability(key, **request)
    http = ReplayIssuer(store)
    http.release.clear()
    other = SessionCapabilityOwner(StateStore(store.path), vault)
    first = asyncio.create_task(owner.reserve(http, 'key', **args))
    await asyncio.wait_for(http.entered.wait(), 3)
    second = asyncio.create_task(other.reserve(http, 'key', **args))
    await asyncio.wait_for(replay_entered.wait(), 3)
    http.release.set()
    result = await first
    replay_release.set()
    with pytest.raises(CapabilityMaterialUnavailable):
        await second
    record = store.load().session_capabilities[0]
    assert record.status == 'STORED' and vault.resolve(record.secret_handle) == result.capability
    await owner.close()
    await other.close()


@pytest.mark.asyncio
async def test_durable_capacity_prevents_http_even_after_restart(selection):
    store, vault, owner, http, args = setup(selection)
    await owner.reserve(http, 'key', **args)
    other = copy.deepcopy(args['frame'])
    other['session_id'] = 'second-session'
    other['intent_hash'] = r4_submit_intent_hash(other)
    restarted = SessionCapabilityOwner(store, vault, capacity=1)
    with pytest.raises(ConnectorError) as full:
        await restarted.reserve(http, 'key', **{**args, 'frame': other})
    assert full.value.code == 'CAPACITY_EXCEEDED' and len(http.calls) == 1
    await owner.close()
    await restarted.close()


@pytest.mark.asyncio
async def test_lane_loss_retains_secret_but_never_configures_launch(selection):
    store, vault, owner, http, args = setup(selection)
    http.release.clear()
    configured = []
    online = [True]
    def guard(frame):
        if not online[0]:
            raise ConnectorError('STALE_GENERATION', 'test')
    async def configure(frame, capability):
        configured.append(capability)
    provider = CapabilityLaunchProvider(owner, http, 'key', audience=args['audience'],
        actions=args['actions'], configure=configure, require_current=guard)
    task = asyncio.create_task(provider(args['frame']))
    await asyncio.wait_for(http.entered.wait(), 3)
    online[0] = False
    http.release.set()
    with pytest.raises(ConnectorError):
        await task
    assert not configured and store.load().session_capabilities[0].status == 'STORED'
    await owner.close()


@pytest.mark.asyncio
async def test_execution_owner_prepares_only_after_capability_vault_commit(execution):
    execution_owner, connection, factory, receipts, opening = execution
    store = execution_owner.store
    vault = RestrictedFileVault(store.path.parent, approved=True)
    owner, http = SessionCapabilityOwner(store, vault), Issuer(store)
    original = execution_owner.launch_provider
    async def configure(frame, capability):
        record = store.load().session_capabilities[0]
        assert record.status == 'STORED' and not factory.opened
        assert vault.resolve(record.secret_handle) == capability.capability
        return await original(frame)
    def guard(frame):
        assert connection.online and frame == opening
    execution_owner.launch_provider = CapabilityLaunchProvider(owner, http, 'key',
        audience='nexus-mcp-session', actions=('tools/call',), configure=configure,
        require_current=guard)
    await connection.emit(opening)
    receipt = await observed(receipts, execution_owner)
    assert receipt['operation_id'] == opening['operation_id'] and len(factory.opened) == 1
    assert len(http.calls) == 1
    await owner.close()


def test_schema_six_migrates_without_changing_identity_or_previous_records():
    legacy = dict(schema_version=6, connector_id='legacy', preferences={'keep': True})
    migrated = state_from_json(legacy)
    assert migrated.schema_version == 10 and migrated.session_capabilities == []
    assert state_to_json(migrated)['connector_id'] == 'legacy'
    assert state_to_json(migrated)['preferences'] == {'keep': True}
