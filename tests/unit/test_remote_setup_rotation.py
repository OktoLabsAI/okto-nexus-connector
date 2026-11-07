"""Completed reuse separates current credentials from historical bootstrap facts."""
from copy import deepcopy
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from nexus_connector_core import InstallationCandidate

from okto_nexus_connector.cli.commands import connect, configure_local
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.identity.import_flow import replace_credential
from okto_nexus_connector.services import remote_setup as setup
from tests.unit import test_connect_reuse as reuse_fixtures

registration = reuse_fixtures.registration
onboarding = reuse_fixtures.onboarding
binding = reuse_fixtures.binding
retained = reuse_fixtures.retained


@pytest.fixture
def remote(retained, monkeypatch):
    r = retained
    state = r.store.load()
    local = state.realizations[0]
    candidate = InstallationCandidate(local.adapter_id, local.candidate_executable,
        local.candidate_fingerprint, local.candidate_source, local.candidate_trust,
        installation_ref=local.candidate_ref, launch_script=local.candidate_launch_script,
        build_identity=local.candidate_build_identity, version=local.candidate_version,
        architecture=local.candidate_architecture)
    record = setup.prepare(r.store, setup_run_id='project', server='https://nexus.test',
        configuration=r.saved['configuration'], candidate_ref=local.candidate_ref,
        inventory_revision=local.inventory_revision, candidate=candidate)
    # This fixture wraps the real persisted acknowledged binding from retained;
    # its original launch consent already exists, so retain that exact consent.
    def consent(state):
        value = state.preferences[setup.PREFIX + 'project']
        value['draft']['local_consent']['local_consent_id'] = local.local_consent_id
        value['draft_revision'] = setup.document_revision(value['draft'])
    r.store.update(consent)
    record = setup.load(r.store, 'project')
    handoff = dict(format='okto.remote-setup-handoff', version=1,
        bootstrap=dict(setup_run_id='project', review_revision='a' * 64, server_id='srv',
            agent_id='agent', workspace_id='workspace', credential_epoch=state.identities[0].credential_epoch),
        review_revision='a' * 64, child_requests=record['child_requests'], draft_revision=record['draft_revision'])
    record = setup.accept_handoff(r.store, 'project', record, handoff, handoff)
    setup.checkpoint(r.store, 'project', record, identity_alias='selected', stage='configured')
    def associate(state):
        saved = state.preferences[r.storage_key]
        saved.update(remote_setup=dict(setup_run_id='project', review_revision='a' * 64),
            child_requests=deepcopy(handoff['child_requests']), local_consent_id=local.local_consent_id,
            executor_id=local.executor_id, binding=deepcopy(saved['result']['binding']))
        intent = state.binding_intents[0]
        intent.remote_setup = deepcopy(saved['remote_setup'])
        intent.apply_client_intent_id = handoff['child_requests']['apply']
        saved['proposal'] = deepcopy(intent.proposal)
    r.store.update(associate)
    r.saved = deepcopy(r.store.load().preferences[r.storage_key])
    r.handoff = handoff
    r.rotation_calls = []
    material = ['synthetic-canonical-key']
    def store_key(namespace, key):
        assert namespace == 'srv/agent'
        r.rotation_calls.append('replace')
        material[0] = key
    def resolve(handle):
        assert handle == 'vault:registered-agent'
        r.calls.append('vault.resolve')
        return material[0]
    r.vault = SimpleNamespace(replace=store_key, resolve=resolve)
    monkeypatch.setattr(connect, 'open_vault', lambda *a, **kw: r.vault)
    async def request(method, path, **kw):
        assert method == 'GET' and path == '/v1/connections/bindings/binding'
        assert kw['key'] == material[0]
        r.calls.append('binding.read')
        return dict(r.remote)
    r.peer._request = request
    return r


async def rotate(r):
    r.peer.me_info = replace(r.peer.me_info, credential_epoch=r.peer.me_info.credential_epoch + 1)
    async def me(key):
        assert key == 'synthetic-explicitly-rotated-key'
        return r.peer.me_info
    r.peer.me = me
    await replace_credential(r.peer, r.vault, r.store, alias='selected', key='synthetic-explicitly-rotated-key')
    assert r.rotation_calls == ['replace']


@pytest.mark.parametrize('root_stage', ['configured', 'awaiting_host'], ids=['cli-finish', 'background-finish'])
async def test_completed_reconnect_after_explicit_rotation_keeps_original_receipts(remote, root_stage):
    r = remote
    r.store.update(lambda state: state.preferences[setup.PREFIX + 'project'].update(stage=root_stage))
    historical = deepcopy(r.store.load().preferences)
    await rotate(r)
    before = r.store.path.read_bytes()
    result = await reuse_fixtures.run(r)
    assert result['saved'] and result['reused'] and not result['runtime_started']
    assert result['binding'] == historical[r.storage_key]['result']['binding']
    assert result['current_binding']['authorization_revision'] == 100
    assert r.calls == ['vault.resolve', 'binding.read', 'start', 'state.reload']
    assert r.store.path.read_bytes() == before
    assert r.store.load().preferences == historical
    assert r.store.load().identities[0].credential_epoch == r.handoff['bootstrap']['credential_epoch'] + 1
    assert len(r.peer.prepares) == len(r.peer.applies) == 1
    assert r.rotation_calls == ['replace']


@pytest.mark.parametrize('stage', ['apply', 'policies'])
async def test_incomplete_setup_remains_anchored_to_bootstrap_epoch(remote, stage):
    r = remote
    def incomplete(state):
        saved = state.preferences[r.storage_key]
        saved.update(stage=stage)
        saved.pop('result')
        if stage == 'apply':
            state.binding_intents[0].status = 'APPLY_PENDING'
            saved.pop('binding')
    r.store.update(incomplete)
    await rotate(r)
    before = r.store.path.read_bytes()
    with pytest.raises(ConnectorError) as refused:
        await reuse_fixtures.run(r)
    assert refused.value.code == 'AGENT_ID_MISMATCH'
    assert r.store.path.read_bytes() == before and 'start' not in r.calls
    assert len(r.peer.prepares) == len(r.peer.applies) == 1


@pytest.mark.parametrize('mutation', ['result', 'result-binding', 'applied-binding', 'apply-id', 'intent-metadata',
    'child-map', 'root-namespace', 'root-selection', 'pending-review', 'startup-pending'])
async def test_completed_reuse_rejects_incoherent_history_without_effects(remote, mutation):
    r = remote
    def corrupt(state):
        saved = state.preferences[r.storage_key]
        root = state.preferences[setup.PREFIX + 'project']
        intent = state.binding_intents[0]
        if mutation == 'result':
            saved['result']['saved'] = False
        elif mutation == 'result-binding':
            saved['result']['binding']['binding_id'] = 'other'
        elif mutation == 'applied-binding':
            intent.applied_binding['workspace_id'] = 'other'
        elif mutation == 'apply-id':
            intent.apply_client_intent_id = 'other_apply'
        elif mutation == 'intent-metadata':
            intent.remote_setup['review_revision'] = 'b' * 64
        elif mutation == 'child-map':
            saved['child_requests']['apply'] += '_' + 'e' * 24
        elif mutation == 'root-namespace':
            root['handoff']['bootstrap']['agent_id'] = 'other'
        elif mutation == 'root-selection':
            root['local']['configuration']['alias'] = 'other'
        elif mutation == 'pending-review':
            root['pending_local_review'] = {'retained': True}
        else:
            plan = {'plan_revision': 'retained', 'scope': {}, 'action': 'install'}
            root['local']['login_start_plan'] = plan
            root['draft']['start_at_login'] = True
            root['draft_revision'] = setup.document_revision(root['draft'])
            root['handoff']['draft_revision'] = root['draft_revision']
            saved.update(login_start_plan=plan, login_start_operation={'plan_revision': 'retained', 'state': 'pending'})
    r.store.update(corrupt)
    before = r.store.path.read_bytes()
    with pytest.raises(ConnectorError):
        await reuse_fixtures.run(r)
    assert r.store.path.read_bytes() == before and 'start' not in r.calls
    assert len(r.peer.prepares) == len(r.peer.applies) == 1


@pytest.mark.parametrize('change', ['identity', 'binding-local', 'binding-server', 'mapping-server', 'intent-status'])
async def test_current_authority_refusal_cannot_use_historical_finish(remote, change):
    r = remote
    if change == 'identity':
        r.store.update(lambda state: setattr(state.identities[0], 'revoked', True))
    elif change == 'binding-local':
        r.store.update(lambda state: setattr(state.execution_bindings[0], 'state', 'REVOKED'))
    elif change == 'binding-server':
        r.remote['state'] = 'REVOKED'
    elif change == 'mapping-server':
        r.remote['endpoint_id'] = 'other'
    else:
        r.store.update(lambda state: setattr(state.binding_intents[0], 'status', 'APPLY_PENDING'))
    before = r.store.path.read_bytes()
    with pytest.raises(ConnectorError):
        await reuse_fixtures.run(r)
    assert r.store.path.read_bytes() == before and 'start' not in r.calls


@pytest.mark.parametrize('change', ['root', 'intent', 'identity', 'binding'])
async def test_current_reuse_fence_covers_changes_during_binding_read(remote, change):
    r = remote
    request = r.peer._request
    async def changed(*a, **kw):
        result = await request(*a, **kw)
        def mutate(state):
            if change == 'root':
                state.preferences[setup.PREFIX + 'project']['pending_local_review'] = {'retained': True}
            elif change == 'intent':
                state.binding_intents[0].status = 'REVIEW_SUSPENDED'
            elif change == 'identity':
                state.identities[0].credential_epoch += 1
            else:
                state.execution_bindings[0].state = 'REVOKED'
        r.store.update(mutate)
        return result
    r.peer._request = changed
    with pytest.raises(ConnectorError) as refused:
        await reuse_fixtures.run(r)
    assert refused.value.code == 'STALE_GENERATION'
    assert 'start' not in r.calls


@pytest.mark.parametrize('change', ['root', 'identity', 'profile', 'binary'])
async def test_reuse_rechecks_after_http_close_before_daemon_effect(remote, monkeypatch, change):
    r = remote
    @asynccontextmanager
    async def http(url):
        assert url == 'https://nexus.test'
        yield r.peer
        def mutate(state):
            if change == 'root':
                state.preferences[setup.PREFIX + 'project']['pending_local_review'] = {'retained': True}
            elif change == 'identity':
                state.identities[0].credential_epoch += 1
            elif change == 'profile':
                state.servers['srv'].base_url = 'https://other.test'
            else:
                Path(state.realizations[0].candidate_executable).write_bytes(b'changed synthetic binary, never executed')
        r.store.update(mutate)
    monkeypatch.setattr(connect, 'NexusHTTPClient', http)
    with pytest.raises(ConnectorError) as refused:
        await reuse_fixtures.run(r)
    # An inconsistent origin is refused by the existing profile guard before
    # snapshot comparison; physical byte drift likewise has its precise code.
    assert refused.value.code == ('PROFILE_DRIFT' if change in ('profile', 'binary') else 'STALE_GENERATION')
    assert r.calls == ['vault.resolve', 'binding.read']


async def test_finished_startup_is_reported_without_reinstall_or_reenable(remote, monkeypatch):
    r = remote
    def startup(state):
        root = state.preferences[setup.PREFIX + 'project']
        plan = {'plan_revision': 'retained', 'action': 'install', 'scope': {}}
        root['local']['login_start_plan'] = plan
        root['draft']['start_at_login'] = True
        root['draft_revision'] = setup.document_revision(root['draft'])
        root['handoff']['draft_revision'] = root['draft_revision']
        state.preferences[r.storage_key].update(login_start_plan=plan,
            login_start_operation={'plan_revision': 'retained', 'state': 'done'})
    r.store.update(startup)
    await rotate(r)
    monkeypatch.setattr(configure_local.manager, 'status', lambda *a: SimpleNamespace(user_service='disabled', state='stopped', reason='user-disabled'))
    before = r.store.path.read_bytes()
    result = await reuse_fixtures.run(r)
    assert result['login_start']['current_user_service'] == 'disabled'
    assert r.store.path.read_bytes() == before
    assert r.calls == ['vault.resolve', 'binding.read', 'start', 'state.reload']
