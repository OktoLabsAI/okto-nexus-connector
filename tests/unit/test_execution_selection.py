"""Persisted approved selection, physical drift and effect-free Core composition."""

import asyncio
from dataclasses import replace
from pathlib import Path

import pytest
from nexus_connector_core import InstallationCandidate, calculate_inventory_revision, r4_submit_intent_hash, R4_PREVIEW_REVISION
from nexus_connector_core.discovery import fingerprint, installation_ref

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.platform.paths import state_dir
from okto_nexus_connector.services.execution_selection import acknowledge_execution_binding, resolve_execution_selection
from okto_nexus_connector.services.realization_service import stage_local_realization, acknowledge_local_realization
from okto_nexus_connector.services.core_host import CoreRuntimeHost
from okto_nexus_connector.storage.state_store import StateStore, state_from_json, state_to_json
from okto_nexus_connector.transport.https_client import R4BindingView, R4Realization


@pytest.fixture
def selection(tmp_path, request):
    workspace = tmp_path / 'workspace'
    workspace.mkdir()
    binary = tmp_path / 'codex.exe'
    binary.write_bytes(b'Synthetic approved binary')
    adapter_id = {'pi': 'pi_rpc', 'claude': 'claude_stream', 'claude_no_refs': 'claude_stream'}.get(getattr(request, 'param', None), 'codex_app_server')
    candidate = InstallationCandidate(adapter_id, str(binary), fingerprint(binary), 'explicit', 'selected',
        installation_ref=installation_ref(adapter_id, str(binary)))
    revision = calculate_inventory_revision([candidate])
    store = StateStore(tmp_path / 'state.json')
    digest = 'sha256:' + 'a' * 64
    if getattr(request, 'param', False):
        from okto_nexus_connector.services.launch_configuration import stage_launch_configuration
        home = tmp_path / 'provider-home'
        home.mkdir()
        config = stage_launch_configuration(store, server_id='srv', executor_id='exe',
            agent_id='agent', local_consent_id='consent', adapter_id=candidate.adapter_id,
            profile_revision=1, provider_home=home,
            secret_bindings={} if getattr(request, 'param', None) in ('no_refs','claude_no_refs') else {'OPENAI_API_KEY': 'vault:provider-demo'})
        digest = config.configuration_digest
    local = stage_local_realization(store, server_id='srv', executor_id='exe', agent_id='agent',
        client_intent_id='realize', candidates=[candidate], adapter_id=candidate.adapter_id,
        candidate_ref=candidate.installation_ref, inventory_revision=revision,
        workspace_root=workspace, workspace_id=None, workspace_label='Workspace',
        configuration_digest=digest, local_consent_id='consent')
    published = R4Realization('srv', 'exe', 'realization', local.local_realization_ref, 1,
        'agent', 'workspace', 'wxb', revision, local.configuration_digest)
    acknowledged = acknowledge_local_realization(store, record=local, published=published)
    binding = R4BindingView('binding', 'srv', 'exe', 'agent', 'endpoint', 'workspace', 'wxb',
        candidate.adapter_id, candidate.installation_ref, revision, 'realization', 1, 1, 2, 2, 'APPROVED')
    frame = dict(protocol_major=1, contract_revision=R4_PREVIEW_REVISION, type='operation.submit',
        server_id='srv', executor_id='exe', binding_id='binding', agent_id='agent', workspace_id='workspace',
        workspace_binding_id='wxb', session_id='session', session_owner_generation=1,
        authorization_revision=2, configuration_revision=2, binding_revision=1, credential_epoch=1,
        connection_id='connection', connection_generation=1, grant_id='grant', operation_id='open', action='runtime.open',
        payload=dict(adapter_id=candidate.adapter_id, candidate_ref=candidate.installation_ref,
            inventory_revision=revision, realization_ref='realization', realization_revision=1, profile_revision=1, mode='managed'))
    frame['intent_hash'] = r4_submit_intent_hash(frame)
    return store, candidate, binding, frame, local, published


def test_approved_binding_roundtrip_does_not_downgrade_on_publication_replay(selection):
    store, candidate, binding, frame, local, published = selection
    with pytest.raises(ConnectorError):
        resolve_execution_selection(store, frame=frame, candidates=[candidate])
    first = acknowledge_execution_binding(store, binding=binding)
    assert acknowledge_execution_binding(store, binding=binding) == first
    acknowledge_local_realization(store, record=local, published=published)
    assert store.load().realizations[0].status == 'BOUND'
    selected = resolve_execution_selection(StateStore(store.path), frame=frame, candidates=[candidate])
    assert selected.candidate == candidate
    assert selected.workspace_root == str(store.path.parent / 'workspace')
    assert len(store.load().execution_bindings) == 1
    old = state_from_json({'schema_version': 3, 'connector_id': 'legacy', 'preferences': {'keep': True}})
    assert old.schema_version == 13 and old.execution_bindings == []
    assert state_to_json(old)['preferences'] == {'keep': True}
    with pytest.raises(ConnectorError):
        acknowledge_execution_binding(store, binding=replace(binding, endpoint_id='another'))
    assert store.load().execution_bindings[0] == first


@pytest.mark.parametrize('changed', ['server_id', 'executor_id', 'agent_id', 'workspace_id',
    'workspace_binding_id', 'binding_revision', 'authorization_revision', 'configuration_revision',
    'candidate_ref', 'realization_ref', 'realization_revision', 'inventory_revision'])
def test_cross_scope_or_stale_open_never_resolves(selection, changed):
    store, candidate, binding, frame, *_ = selection
    acknowledge_execution_binding(store, binding=binding)
    frame = {**frame, 'payload': dict(frame['payload'])}
    target = frame['payload'] if changed in frame['payload'] else frame
    old = target[changed]
    target[changed] = old + 1 if type(old) is int else 'sha256:' + 'b' * 64 if changed == 'inventory_revision' else 'other'
    if changed == 'candidate_ref':
        target[changed] = 'nexus-install-v1:' + 'b' * 64
    frame['intent_hash'] = r4_submit_intent_hash(frame)
    with pytest.raises(ConnectorError):
        resolve_execution_selection(store, frame=frame, candidates=[candidate])


@pytest.mark.parametrize('changed', ['binary', 'root', 'local_configuration', 'local_candidate', 'duplicate_binding', 'inventory'])
def test_physical_or_persisted_drift_never_resolves(selection, changed):
    store, candidate, binding, frame, *_ = selection
    acknowledge_execution_binding(store, binding=binding)
    candidates = [candidate]
    if changed == 'binary':
        Path(candidate.executable).write_bytes(b'Changed binary')
    elif changed == 'root':
        root = store.path.parent / 'workspace'
        root.rename(root.with_name('original-workspace'))
        root.mkdir()
    elif changed == 'local_configuration':
        store.update(lambda s: setattr(s.realizations[0], 'configuration_digest', 'sha256:' + 'b' * 64))
    elif changed == 'local_candidate':
        store.update(lambda s: setattr(s.realizations[0], 'candidate_architecture', 'invented'))
    elif changed == 'duplicate_binding':
        store.update(lambda s: s.execution_bindings.append(s.execution_bindings[0]))
    else:
        candidates = [replace(candidate, version='changed')]
    with pytest.raises(ConnectorError):
        resolve_execution_selection(store, frame=frame, candidates=candidates)


def test_publication_ack_rejects_concurrent_configuration_change(selection):
    store, _, _, _, local, published = selection
    store.update(lambda s: setattr(s.realizations[0], 'configuration_digest', 'sha256:' + 'b' * 64))
    with pytest.raises(ConnectorError):
        acknowledge_local_realization(store, record=local, published=published)


def test_pi_dependency_bytes_remain_part_of_approved_selection(selection, tmp_path):
    from nexus_connector_core.discovery import candidate_pi_node_cli, selected_fingerprint
    _, _, old_binding, frame, _, _ = selection
    node = tmp_path / 'node.exe'
    node.write_bytes(b'Synthetic Node binary')
    node.chmod(0o700)
    package = tmp_path / 'node_modules' / '@earendil-works' / 'pi-coding-agent'
    script = package / 'dist' / 'bundle' / 'cli.js'
    script.parent.mkdir(parents=True)
    script.write_text('Synthetic Pi CLI', encoding='utf-8')
    (package / 'package.json').write_text('{"name":"@earendil-works/pi-coding-agent","version":"1.0.0"}', encoding='utf-8')
    dependency = package / 'helper.js'
    dependency.write_text('Original loaded helper', encoding='utf-8')
    candidate = candidate_pi_node_cli(node, script, explicit=True)
    revision = calculate_inventory_revision([candidate])
    store = StateStore(tmp_path / 'pi-state.json')
    local = stage_local_realization(store, server_id='srv', executor_id='exe', agent_id='agent', client_intent_id='pi',
        candidates=[candidate], adapter_id=candidate.adapter_id, candidate_ref=candidate.installation_ref,
        inventory_revision=revision, workspace_root=tmp_path / 'workspace', workspace_id=None,
        workspace_label='Pi project', configuration_digest='sha256:' + 'a' * 64, local_consent_id='consent')
    acknowledge_local_realization(store, record=local, published=R4Realization('srv', 'exe', 'realization',
        local.local_realization_ref, 1, 'agent', 'workspace', 'wxb', revision, local.configuration_digest))
    binding = replace(old_binding, adapter_id=candidate.adapter_id, candidate_ref=candidate.installation_ref, inventory_revision=revision)
    acknowledge_execution_binding(store, binding=binding)
    frame['payload'].update(adapter_id=candidate.adapter_id, candidate_ref=candidate.installation_ref, inventory_revision=revision)
    frame['intent_hash'] = r4_submit_intent_hash(frame)
    assert resolve_execution_selection(store, frame=frame, candidates=[candidate]).candidate.launch_script == str(script)
    dependency.write_text('Changed loaded helper', encoding='utf-8')
    assert selected_fingerprint(candidate) == candidate.fingerprint
    with pytest.raises(ConnectorError, match='PROFILE_DRIFT'):
        resolve_execution_selection(store, frame=frame, candidates=[candidate])


async def test_host_revalidates_after_shared_store_wait_and_before_cache_reuse(selection, tmp_path, monkeypatch):
    store, candidate, binding, frame, *_ = selection
    acknowledge_execution_binding(store, binding=binding)
    host = CoreRuntimeHost(state_dir(tmp_path / 'runtime'), None)
    entered, release = asyncio.Event(), asyncio.Event()
    original = host.ensure_journal
    async def blocked_journal():
        entered.set()
        await release.wait()
        return await original()
    async def environment(_):
        raise AssertionError('Runtime composition must not launch a provider.')
    monkeypatch.setattr(host, 'ensure_journal', blocked_journal)
    task = asyncio.create_task(host.build_r4(store, frame=frame, candidates=[candidate], environment=environment))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        store.update(lambda s: setattr(s.execution_bindings[0], 'state', 'REVOKED'))
        release.set()
        with pytest.raises(ConnectorError):
            await task
        assert not host._runtimes
        store.update(lambda s: setattr(s.execution_bindings[0], 'state', 'APPROVED'))
        runtime = await host.build_r4(store, frame=frame, candidates=[candidate], environment=environment)
        assert await host.build_r4(store, frame=frame, candidates=[candidate], environment=environment) is runtime
        assert host.get('binding') is None
        Path(candidate.executable).write_bytes(b'Changed binary')
        with pytest.raises(ConnectorError):
            await host.build_r4(store, frame=frame, candidates=[candidate], environment=environment)
    finally:
        release.set()
        await host.shutdown_all()


async def test_r4_host_defers_native_capture_to_installed_lease(selection, tmp_path, monkeypatch):
    from okto_nexus_connector.services import core_host
    store, candidate, binding, frame, *_ = selection
    acknowledge_execution_binding(store, binding=binding)
    host = CoreRuntimeHost(state_dir(tmp_path / 'capture-runtime'), None)
    composed = []
    original = core_host.create_runtime
    def compose(**options):
        composed.append(options)
        return original(**options)
    monkeypatch.setattr(core_host, 'create_runtime', compose)
    async def environment(_):
        raise AssertionError('Composition must not resolve secrets or launch a provider.')
    try:
        runtime = await host.build_r4(store, frame=frame, candidates=[candidate], environment=environment)
        assert await host.build_r4(store, frame=frame, candidates=[candidate], environment=environment) is runtime
        assert len(composed) == 1
        assert composed[0]['native_approvals_from_lease'] is True
        assert not composed[0].get('native_approvals_enabled', False)
    finally:
        await host.shutdown_all()
