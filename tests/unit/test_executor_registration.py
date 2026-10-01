"""Durable registration intent, scope checks, rotation and lost-response replay."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import json

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services.executor_registration import ExecutorRegistrationService
from okto_nexus_connector.storage.state_store import (
    ConnectorState, StateStore, ServerProfileRecord, IdentityRecord, state_from_json, state_to_json,
)
from okto_nexus_connector.transport.https_client import MeInfo, ExecutorRegistration


class Vault:
    def resolve(self, handle):
        assert handle == 'vault:registered-agent'
        return 'synthetic-canonical-key'


class Peer:
    def __init__(self, store):
        self.store = store
        self.requests = []
        self.lose_response = False
        self.mutate_response = lambda value: value
        self.during_request = None
        self.me_info = MeInfo('srv', 'agent', 'Agent', (), 3, 4, 2)

    async def me(self, key):
        assert key == 'synthetic-canonical-key'
        return self.me_info

    async def register_executor(self, key, **body):
        assert key == 'synthetic-canonical-key'
        record = self.store.load().execution_executors[0]
        assert record.client_intent_id == body['client_intent_id']
        self.requests.append(body)
        if self.during_request:
            await self.during_request()
        if self.lose_response:
            self.lose_response = False
            raise ConnectorError('CONTROL_DISCONNECTED', 'test_peer', 'The response was lost.')
        return self.mutate_response(ExecutorRegistration('srv', 'executor', body['connector_id'],
            'AWAITING_INVENTORY', 'nxt4_synthetic_' + str(len(self.requests)), 60, 'agent', 2, 3))


@pytest.fixture
def registration(tmp_path):
    store = StateStore(tmp_path / 'state.json')
    state = ConnectorState(connector_id='connector')
    state.servers['srv'] = ServerProfileRecord('srv', 'https://nexus.test', 'https://nexus.test', 'now')
    state.identities.append(IdentityRecord('selected', 'srv', 'agent', 'vault:registered-agent', 1, 'now'))
    store.save(state)
    peer = Peer(store)
    @asynccontextmanager
    async def http(url):
        assert url == 'https://nexus.test'
        yield peer
    service = ExecutorRegistrationService(store, Vault(), http_factory=http)
    return store, peer, service


async def test_lost_response_replays_persisted_intent_and_bootstrap_stays_secret(registration):
    store, peer, service = registration
    peer.lose_response = True
    with pytest.raises(ConnectorError):
        await service.register(identity_alias='selected', label='Host')
    pending = store.load().execution_executors[0]
    assert pending.state == 'REGISTRATION_PENDING' and not pending.executor_id
    result = await service.register(identity_alias='selected', label='Host')
    assert peer.requests[0] == peer.requests[1]
    assert result.client_intent_id == pending.client_intent_id and result.state == 'REGISTERED'
    assert StateStore(store.path).load().execution_executors == [result]
    bootstrap = await service.bootstrap(server_id='srv')
    assert bootstrap.executor == result and len(peer.requests) == 3
    assert bootstrap.ticket not in repr(bootstrap)
    raw = store.path.read_text()
    assert 'nxt4_' not in raw and 'synthetic-canonical-key' not in raw
    assert json.loads(raw)['schema_version'] == 9


@pytest.mark.parametrize('change', ['server_id', 'connector_id', 'registration_agent_id',
                                    'credential_epoch', 'authorization_revision'])
async def test_changed_response_never_commits_registration(registration, change):
    store, peer, service = registration
    peer.mutate_response = lambda value: replace(value, **{change: 9 if change.endswith(('epoch', 'revision')) else 'other'})
    with pytest.raises(ConnectorError) as rejected:
        await service.register(identity_alias='selected', label='Host')
    assert rejected.value.code == 'SCOPE_MISMATCH'
    assert store.load().execution_executors[0].state == 'REGISTRATION_PENDING'


@pytest.mark.parametrize('change', ['credential', 'identity_removed', 'profile', 'connector', 'request'])
async def test_local_change_during_http_cannot_commit_old_response(registration, change):
    store, peer, service = registration
    entered, release = asyncio.Event(), asyncio.Event()
    async def wait():
        entered.set()
        await release.wait()
    peer.during_request = wait
    task = asyncio.create_task(service.register(identity_alias='selected', label='Host'))
    await asyncio.wait_for(entered.wait(), 2)
    def alter(state):
        if change == 'credential': state.identities[0].credential_epoch += 1
        elif change == 'identity_removed': state.identities.clear()
        elif change == 'profile': state.servers['srv'].display_name = 'Changed profile'
        elif change == 'connector': state.connector_id = 'another-connector'
        else: state.execution_executors[0].label = 'Changed label'
    store.update(alter)
    release.set()
    with pytest.raises(ConnectorError):
        await task
    assert store.load().execution_executors[0].state == 'REGISTRATION_PENDING'


async def test_replay_cannot_replace_executor_or_registration_actor(registration):
    store, peer, service = registration
    record = await service.register(identity_alias='selected', label='Host', client_intent_id='explicit')
    peer.mutate_response = lambda value: replace(value, executor_id='other')
    with pytest.raises(ConnectorError):
        await service.bootstrap(server_id='srv')
    assert store.load().execution_executors == [record]
    before = len(peer.requests)
    for changes in ({'label': 'Different'}, {'client_intent_id': 'different'}):
        with pytest.raises(ConnectorError):
            await service.register(**(dict(identity_alias='selected', label='Host') | changes))
    assert len(peer.requests) == before


async def test_delayed_ticket_does_not_gain_a_new_local_lifetime(registration):
    store, peer, service = registration
    now = [100.0]
    service.clock = lambda: now[0]
    async def delayed(): now[0] = 160.0
    peer.during_request = delayed
    with pytest.raises(ConnectorError) as expired:
        await service.register(identity_alias='selected', label='Host')
    assert expired.value.code == 'CONTROL_DISCONNECTED'
    assert store.load().execution_executors[0].state == 'REGISTERED'


def test_schema_four_upgrade_preserves_records_without_inventing_executor():
    state = state_from_json({'schema_version': 4, 'connector_id': 'existing', 'preferences': {'keep': True}})
    assert state.schema_version == 9 and not state.execution_executors
    assert state_to_json(state)['preferences'] == {'keep': True}
