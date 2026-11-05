"""Reconnect resolves saved canonical scope before secrets, discovery or host effects."""
import io
from pathlib import Path
from types import SimpleNamespace as NS

import pytest
from nexus_connector_core.connection_configuration import materialize_connection_configuration

from okto_nexus_connector.cli.commands import connect, configure, configure_local
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.errors import ConnectorError
from tests.unit import test_binding_onboarding as binding_fixtures

registration = binding_fixtures.registration
onboarding = binding_fixtures.onboarding
binding = binding_fixtures.binding


@pytest.fixture
async def retained(binding, tmp_path, monkeypatch):
    service, peer, store, prepare, apply = binding
    store.update(lambda state: setattr(state.identities[0], 'credential_epoch', peer.me_info.credential_epoch))
    await service.prepare(**(prepare | {'client_intent_id': 'project_prepare'}))
    peer.approved = True
    result = await service.apply(**(apply | {'prepare_intent_id': 'project_prepare'}))
    state = store.load()
    local = state.realizations[0]
    configuration = materialize_connection_configuration(configure.template() | dict(adapter_id=local.adapter_id, alias='assistant'),
        workspace_root=local.workspace_root, workspace_label=local.workspace_label, provider_home=str(tmp_path), execution_location='remote')
    saved = dict(identity='selected', stage='done', configuration=configuration, workspace_id=local.canonical_workspace_id,
        candidate_ref=local.candidate_ref, inventory_revision=local.inventory_revision, replace_binding_id=None,
        conflict_checked=True, realization_ref=local.realization_ref,
        result=dict(saved=True, request_id='project', connection_name='assistant', binding=result['binding'], runtime_started=False))
    storage_key = 'configure.srv.agent.project'
    store.update(lambda state: state.preferences.update({storage_key: saved}))
    calls = []
    vault = NS(resolve=lambda handle: calls.append('vault.resolve') or service.vault.resolve(handle))
    monkeypatch.setattr(connect, 'open_vault', lambda *a, **kw: vault)
    monkeypatch.setattr(connect, 'StateStore', lambda _: store)
    monkeypatch.setattr(connect, 'NexusHTTPClient', service.http_factory)
    monkeypatch.setattr(connect, '_read_key', lambda *a: pytest.fail('credential re-entry'))
    async def unexpected(*a, **kw):
        pytest.fail('reconnect performed discovery or reimported a credential')
    monkeypatch.setattr(connect, '_choose_harness', unexpected)
    monkeypatch.setattr(connect, 'import_identity', unexpected)
    monkeypatch.setattr(configure_local.manager, 'start', lambda root: calls.append('start'))
    async def ipc(root, method, body=None):
        calls.append(method)
        return {}
    monkeypatch.setattr(configure_local, 'ipc', ipc)
    remote = dict(result['binding'], authorization_revision=100, configuration_revision=200)
    async def request(method, path, **kw):
        assert method == 'GET' and path == '/v1/connections/bindings/binding'
        assert kw['key'] == 'synthetic-canonical-key'
        calls.append('binding.read')
        return dict(remote)
    peer._request = request
    monkeypatch.chdir(local.workspace_root)
    args = build_parser().parse_args(['connect'])
    return NS(args=args, output=Output(json_mode=False, stream=io.StringIO()), root=tmp_path,
        store=store, peer=peer, remote=remote, calls=calls, storage_key=storage_key, saved=saved)


async def run(r):
    return await connect.run_connect(r.args, r.output, r.root)


async def test_bare_connect_reuses_project_and_current_authority_without_rewrite(retained):
    r = retained
    before = r.store.path.read_bytes()
    result = await run(r)
    assert result['saved'] and result['reused'] and result['runtime_started'] is False
    assert result['current_binding']['configuration_revision'] == 200
    assert r.args.request_id == 'project' and r.args.server == 'https://nexus.test'
    assert r.calls == ['vault.resolve', 'binding.read', 'start', 'state.reload']
    assert r.store.path.read_bytes() == before
    assert len(r.peer.prepares) == len(r.peer.applies) == 1
    assert 'synthetic-canonical-key' not in r.output.stream.getvalue()


async def test_customization_cannot_rewrite_retained_project_by_reconnect(retained):
    r = retained
    r.args.customize = True
    before = r.store.path.read_bytes()
    with pytest.raises(ConnectorError) as error:
        await run(r)
    assert error.value.code == 'OPERATION_CONFLICT'
    assert not r.calls and r.store.path.read_bytes() == before


@pytest.mark.parametrize('option,value', [('identity', 'selected'), ('alias', 'selected'), ('agent', 'agent')])
async def test_explicit_saved_identity_reuses_vault(retained, option, value):
    r = retained
    setattr(r.args, option, value)
    assert (await run(r))['reused']
    assert r.calls.count('vault.resolve') == 1


async def test_imported_identity_can_start_a_new_project_without_key_reentry(retained, monkeypatch):
    r = retained
    r.store.update(lambda s: s.preferences.clear())
    r.args.identity = 'selected'
    async def choose(*a):
        r.calls.append('choose')
        return 'codex_app_server', NS(executable='codex', installation_ref='selected'), 'test'
    monkeypatch.setattr(connect, '_choose_harness', choose)
    async def wizard(args, output, root, store, identity, server, portable):
        assert identity.alias == 'selected' and server == 'https://nexus.test'
        assert args.request_id is None and portable['adapter_id'] == 'codex_app_server'
        return dict(saved=False, canceled=True)
    monkeypatch.setattr(configure_local, 'configure_local', wizard)
    assert (await run(r))['canceled']
    assert r.calls == ['vault.resolve', 'choose']


async def test_explicit_credential_import_still_validates_and_reactivates_local_identity(retained, monkeypatch):
    from okto_nexus_connector.identity.import_flow import import_identity
    r = retained
    r.store.update(lambda s: setattr(s.identities[0], 'revoked', True))
    r.args.credential_stdin = True
    monkeypatch.setattr(connect, '_read_key', lambda *a: r.calls.append('credential.read') or 'synthetic-canonical-key')
    def replace(namespace, key):
        assert namespace == 'srv/agent' and key == 'synthetic-canonical-key'
        r.calls.append('vault.replace')
    monkeypatch.setattr(connect, 'open_vault', lambda *a, **kw: NS(replace=replace))
    monkeypatch.setattr(connect, 'import_identity', import_identity)
    # This is an explicitly selected protected import, including the existing
    # restricted-file consent; reconnect alone never selects a new backend.
    r.store.update(lambda s: s.preferences.update({'vault.fallback_file.approved': True}))
    assert (await run(r))['reused']
    assert not r.store.load().identities[0].revoked
    assert r.calls == ['credential.read', 'vault.replace', 'binding.read', 'start', 'state.reload']


@pytest.mark.parametrize('state', ['REVOKED', 'DISABLED', 'STALE', 'PENDING_REVIEW'])
async def test_current_remote_refusal_never_starts_or_recreates(retained, state):
    r = retained
    before = r.store.path.read_bytes()
    r.remote['state'] = state
    with pytest.raises(ConnectorError) as refused:
        await run(r)
    assert refused.value.code == 'BINDING_NOT_AUTHORIZED'
    assert r.calls == ['vault.resolve', 'binding.read']
    assert r.store.path.read_bytes() == before
    assert len(r.peer.prepares) == len(r.peer.applies) == 1


@pytest.mark.parametrize('field', ['agent_id', 'workspace_id', 'candidate_ref', 'binding_revision'])
async def test_remote_mapping_drift_is_not_adopted(retained, field):
    r = retained
    r.remote[field] = 99 if field == 'binding_revision' else 'changed'
    with pytest.raises(ConnectorError) as refused:
        await run(r)
    assert refused.value.code == 'PROFILE_DRIFT' and 'start' not in r.calls


@pytest.mark.parametrize('value', [None, True, 0, '1'])
async def test_malformed_server_revision_is_not_success(retained, value):
    r = retained
    r.remote['authorization_revision'] = value
    with pytest.raises(ConnectorError) as refused:
        await run(r)
    assert refused.value.code == 'VERSION_INCOMPATIBLE' and 'start' not in r.calls


async def test_unavailable_credential_never_falls_back_to_import(retained):
    r = retained
    async def refused(key):
        raise ConnectorError('AGENT_AUTH_REQUIRED', 'me', 'Credential revoked')
    r.peer.me = refused
    with pytest.raises(ConnectorError) as error:
        await run(r)
    assert error.value.code == 'AGENT_AUTH_REQUIRED' and r.calls == ['vault.resolve']


async def test_saved_folder_cannot_retarget_an_existing_binding(retained, tmp_path):
    r = retained
    r.args.project = tmp_path
    r.store.update(lambda s: s.preferences[r.storage_key]['configuration'].update(workspace_root=str(tmp_path)))
    with pytest.raises(ConnectorError) as refused:
        await run(r)
    assert refused.value.code == 'PROFILE_DRIFT' and 'start' not in r.calls


@pytest.mark.parametrize('change', ['revoked', 'profile', 'server', 'agent', 'project', 'harness', 'executable'])
async def test_conflicting_scope_refuses_before_vault_or_effects(retained, change, tmp_path):
    r = retained
    r.args.identity = 'selected'
    r.args.request_id = 'project'
    if change == 'revoked':
        r.store.update(lambda s: setattr(s.identities[0], 'revoked', True))
    elif change == 'profile':
        r.store.update(lambda s: setattr(s.servers['srv'], 'base_url', 'https://other.test'))
    elif change == 'server':
        r.args.server = 'https://other.test'
    elif change == 'agent':
        r.args.agent = 'another-agent'
    elif change == 'project':
        r.args.project = tmp_path
    elif change == 'harness':
        r.args.harness = 'pi_rpc'
    else:
        r.args.executable = str(tmp_path / 'different.exe')
    with pytest.raises(ConnectorError):
        await run(r)
    assert r.calls == []


@pytest.mark.parametrize('field', ['server_id', 'agent_id', 'credential_epoch'])
async def test_stored_key_is_authenticated_for_exact_identity(retained, field):
    from dataclasses import replace
    r = retained
    r.peer.me_info = replace(r.peer.me_info, **{field: 99 if field == 'credential_epoch' else 'other'})
    with pytest.raises(ConnectorError) as refused:
        await run(r)
    assert refused.value.code == 'AGENT_ID_MISMATCH' and r.calls == ['vault.resolve']


@pytest.mark.parametrize('change', ['identity', 'request'])
async def test_local_change_during_authentication_is_not_used(retained, change):
    r = retained
    async def me(key):
        def mutate(state):
            if change == 'identity':
                state.identities[0].credential_epoch += 1
            else:
                state.preferences[r.storage_key]['configuration']['tool_access'] = 'always_allow'
        r.store.update(mutate)
        return r.peer.me_info
    r.peer.me = me
    with pytest.raises(ConnectorError) as refused:
        await run(r)
    assert refused.value.code == 'STALE_GENERATION' and r.calls == ['vault.resolve']


async def test_missing_saved_identity_does_not_guess_or_prompt_for_another_key(retained):
    r = retained
    r.store.update(lambda s: s.identities.clear())
    with pytest.raises(ConnectorError) as refused:
        await run(r)
    assert refused.value.code == 'AGENT_AUTH_REQUIRED' and r.calls == []


async def test_same_path_with_replaced_directory_does_not_reuse_physical_consent(retained, tmp_path):
    r = retained
    project = Path(r.store.load().realizations[0].workspace_root)
    destination = tmp_path / 'retained-project'
    assert project.is_relative_to(tmp_path) and destination.is_relative_to(tmp_path)
    # Move after leaving cwd so Windows has no process working-directory handle.
    r.args.project = project
    import os
    os.chdir(tmp_path)
    project.rename(destination)
    project.mkdir()
    with pytest.raises(ConnectorError) as refused:
        await run(r)
    assert refused.value.code == 'PROFILE_DRIFT' and 'start' not in r.calls


async def test_ambiguous_saved_connections_require_selection(retained, monkeypatch):
    from copy import deepcopy
    r = retained
    other = deepcopy(r.saved)
    other['configuration']['adapter_id'] = 'claude_stream'
    r.store.update(lambda s: s.preferences.update({'configure.srv.agent.other': other}))
    asked = []
    def choose(label, options, **kw):
        asked.append(options)
        return r.storage_key
    monkeypatch.setattr(connect, 'select', choose)
    assert (await run(r))['reused']
    assert len(asked) == 1 and len(asked[0]) == 2


async def test_pending_saved_request_resumes_without_credential_import_or_new_id(retained, monkeypatch):
    r = retained
    def pending(state):
        state.preferences[r.storage_key]['stage'] = 'apply'
        state.preferences[r.storage_key]['proposal'] = state.binding_intents[0].proposal
        state.binding_intents[0].status = 'APPLY_PENDING'
    r.store.update(pending)
    async def resume(args, output, root, store, identity, server, portable):
        assert args.request_id == 'project' and identity.alias == 'selected'
        return dict(saved=False, status='awaiting_operator_approval', request_id=args.request_id)
    monkeypatch.setattr(configure_local, 'configure_local', resume)
    result = await run(r)
    assert result['request_id'] == 'project' and result['status'] == 'awaiting_operator_approval'
    assert r.calls == ['vault.resolve']


async def test_new_project_without_identity_requires_explicit_server(tmp_path, monkeypatch):
    from okto_nexus_connector.storage.state_store import StateStore
    store = StateStore(tmp_path / 'state.json')
    monkeypatch.setattr(connect, 'StateStore', lambda _: store)
    monkeypatch.setattr(connect, '_read_key', lambda *a: pytest.fail('credential access without destination'))
    args = build_parser().parse_args(['connect'])
    with pytest.raises(ConnectorError) as refused:
        await connect.run_connect(args, Output(json_mode=False), tmp_path)
    assert refused.value.code == 'SERVER_REQUIRED'
