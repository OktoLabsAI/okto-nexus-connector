"""Resume/add is decided before credentials, replacement or host effects."""
from copy import deepcopy
from pathlib import Path

import pytest
from nexus_connector_core import InstallationCandidate, calculate_inventory_revision
from nexus_connector_core.discovery import fingerprint

from okto_nexus_connector.cli.commands import connect, connect_setup, configure_startup
from okto_nexus_connector.cli.commands.connection_choices import choose_project_intent, operator_shortcut
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.errors import ConnectorError
from tests.unit import test_connect_reuse as fixtures
from tests.unit import test_remote_setup as remote_fixtures

retained = fixtures.retained
binding = fixtures.binding
onboarding = fixtures.onboarding
registration = fixtures.registration
host = remote_fixtures.host
continuation = remote_fixtures.continuation
peer = remote_fixtures.peer


def choose_action(action):
    def choose(label, options, **kwargs):
        return next(value for value, _ in options if isinstance(value, tuple) and value[0] == action)
    return choose


async def test_single_legacy_connection_requires_choice_before_vault(retained, monkeypatch):
    r = retained
    before = r.store.path.read_bytes()
    seen = []
    def cancel(label, options, **kwargs):
        seen.extend(value[0] for value, _ in options)
        assert r.calls == []
        return ('cancel', None)
    monkeypatch.setattr(connect, 'select', cancel)
    result = await connect.run_connect(r.args, r.output, r.root)
    assert result == dict(saved=False, canceled=True, runtime_started=False, effects_started=False)
    assert seen == ['resume', 'add', 'cancel']
    assert r.store.path.read_bytes() == before and r.calls == []


@pytest.fixture
def addition(retained, monkeypatch, tmp_path):
    r = retained
    candidate_file = tmp_path / 'inert-claude.exe'
    candidate_file.write_bytes(b'Owned synthetic candidate, never executed')
    candidate = InstallationCandidate('claude_stream', str(candidate_file), fingerprint(candidate_file), 'path', 'untrusted')
    async def discover(args, output):
        args.discovery_inventory_revision = calculate_inventory_revision([candidate])
        return candidate.adapter_id, candidate, None
    monkeypatch.setattr(connect, 'select', choose_action('add'))
    monkeypatch.setattr(connect, '_choose_harness', discover)
    monkeypatch.setattr(connect_setup, 'discover_provider_home', lambda adapter: None)
    monkeypatch.setattr(connect_setup, 'choice', lambda *args, **kwargs: True)
    monkeypatch.setattr(configure_startup, 'choose', lambda *args: None)
    monkeypatch.setattr('asyncio.create_subprocess_exec', lambda *a, **k: pytest.fail('no native process'))
    monkeypatch.setattr('subprocess.Popen', lambda *a, **k: pytest.fail('no native process'))
    r.args.harness = 'claude_stream'
    return r


async def test_add_uses_new_operator_bootstrap_and_keeps_previous_identity_and_binding(addition):
    r = addition
    before = r.store.load()
    result = await connect.run_connect(r.args, r.output, r.root)
    after = r.store.load()
    assert result['status'] == 'awaiting_operator_bootstrap' and not result['runtime_started']
    assert after.identities == before.identities and after.execution_bindings == before.execution_bindings
    assert after.binding_intents == before.binding_intents and after.realizations == before.realizations
    assert after.connector_id == before.connector_id
    assert after.execution_executors == before.execution_executors and after.runtime_intents == before.runtime_intents
    assert after.bindings == before.bindings
    assert after.preferences[r.storage_key] == before.preferences[r.storage_key]
    keys = set(after.preferences) - set(before.preferences)
    assert keys == {'remote-setup.' + result['request_id']}
    record = after.preferences[next(iter(keys))]
    assert record['draft']['agent_hint'] is None
    assert record['draft']['workspace_hint'] == r.saved['workspace_id']
    assert record['draft']['configuration']['adapter_id'] == 'claude_stream'
    assert record['local']['configuration']['alias'] != r.saved['configuration']['alias']
    assert record['child_requests']['prepare'] == result['request_id'] + '_prepare'
    assert r.calls == []
    assert Path(result['draft_file']).is_file()
    text = r.output.stream.getvalue()
    assert operator_shortcut(result['request_id']) in text
    assert 'CONNECT_PROFILE' not in text and 'synthetic-canonical-key' not in text


async def test_add_review_cancel_does_not_create_checkpoint_or_identity(addition, monkeypatch):
    r = addition
    before = r.store.path.read_bytes()
    monkeypatch.setattr(connect_setup, 'choice', lambda *args, **kwargs: False)
    result = await connect.run_connect(r.args, r.output, r.root)
    assert result['canceled'] and r.store.path.read_bytes() == before
    assert r.calls == [] and not (r.root / 'setup-drafts').exists()


async def test_pending_draft_picker_recovers_same_root_without_discovery_or_new_identity(addition, monkeypatch):
    r = addition
    first = await connect.run_connect(r.args, r.output, r.root)
    before = deepcopy(r.store.load().preferences)
    async def unexpected(*args):
        pytest.fail('pending root must not rediscover another installation')
    monkeypatch.setattr(connect, '_choose_harness', unexpected)
    monkeypatch.setattr(connect, 'select', choose_action('pending'))
    args = build_parser().parse_args(['connect'])
    second = await connect.run_connect(args, r.output, r.root)
    assert second == first and args.request_id == first['request_id']
    assert r.store.load().preferences == before and r.calls == []


async def test_explicit_request_id_bypasses_picker_and_preserves_original_checkpoint(retained, monkeypatch):
    r = retained
    r.args.request_id = 'project'
    monkeypatch.setattr(connect, 'select', lambda *a, **k: pytest.fail('explicit request needs no picker'))
    result = await connect.run_connect(r.args, r.output, r.root)
    assert result['reused'] and r.args.request_id == 'project'


async def test_add_cannot_use_old_credential_or_replace_alias(addition):
    r = addition
    before = r.store.path.read_bytes()
    r.args.credential_stdin = True
    with pytest.raises(ConnectorError) as error:
        await connect.run_connect(r.args, r.output, r.root)
    assert error.value.code == 'OPERATION_CONFLICT'
    assert r.calls == [] and r.store.path.read_bytes() == before
    r.args.credential_stdin = False
    r.args.binding_alias = r.saved['configuration']['alias']
    with pytest.raises(ConnectorError) as error:
        await connect.run_connect(r.args, r.output, r.root)
    assert error.value.code == 'OPERATION_CONFLICT' and r.calls == []


def test_destination_and_project_filter_do_not_merge_by_path(retained):
    r = retained
    args = build_parser().parse_args(['connect', '--server', 'https://other.example'])
    def unexpected(*args, **kwargs):
        pytest.fail('another destination cannot expose this connection')
    assert choose_project_intent(args, r.store.load(), unexpected).kind == 'initial'


def test_other_destination_missing_identity_does_not_retarget_or_block_selected_server(retained):
    r = retained
    state = r.store.load()
    state.identities.clear()
    args = build_parser().parse_args(['connect', '--server', 'https://other.example'])
    assert choose_project_intent(args, state, lambda *a, **k: pytest.fail('unrelated connection')).kind == 'initial'


@pytest.mark.parametrize('damage', ['revision', 'unreadable'])
async def test_invalid_pending_root_is_not_treated_as_permission_for_new_setup(addition, damage):
    r = addition
    result = await connect.run_connect(r.args, r.output, r.root)
    def corrupt(state):
        if damage == 'revision':
            state.preferences['remote-setup.' + result['request_id']].update(draft_revision='changed')
        else:
            state.preferences['remote-setup.' + result['request_id']] = None
    r.store.update(corrupt)
    before = r.store.path.read_bytes()
    args = build_parser().parse_args(['connect'])
    with pytest.raises(ConnectorError) as error:
        await connect.run_connect(args, r.output, r.root)
    assert error.value.code == 'PROFILE_DRIFT'
    assert r.store.path.read_bytes() == before and r.calls == []


@pytest.mark.parametrize('flags', [['--json'], ['--non-interactive']])
async def test_wizard_json_contract_refused_before_catalog_or_credentials(retained, monkeypatch, flags):
    r = retained
    args = build_parser().parse_args([*flags, 'connect'])
    monkeypatch.setattr(connect, 'StateStore', lambda *a: pytest.fail('wizard rejection must precede state access'))
    with pytest.raises(ConnectorError) as error:
        await connect.run_connect(args, r.output, r.root)
    assert error.value.code == 'VALIDATION_ERROR' and r.calls == []


@pytest.mark.parametrize('lost_stage', ['identity_import', 'host_pipeline'])
async def test_picker_recovers_lost_stage_on_original_root_without_reimport_or_new_proposal(
        continuation, monkeypatch, lost_stage):
    r = continuation
    original_import = connect.import_identity
    imports, pipelines = [], []
    async def import_then_lose(*args, **kwargs):
        imported = await original_import(*args, **kwargs)
        imports.append(imported.identity.agent_id)
        if lost_stage == 'identity_import' and len(imports) == 1:
            raise ConnectorError('OUTCOME_UNKNOWN', 'identity_import', 'Synthetic lost import acknowledgment')
        return imported
    async def pipeline(args, output, root, store, identity, server, portable):
        saved = store.load().preferences['configure.srv_fake.agent.setup-test']
        remote_fixtures.setup.require_configuration(store, saved, identity)
        pipelines.append(deepcopy(saved['child_requests']))
        if lost_stage == 'host_pipeline' and len(pipelines) == 1:
            raise ConnectorError('OUTCOME_UNKNOWN', 'host_pipeline', 'Synthetic lost claim-stage acknowledgment')
        return dict(saved=False, status='awaiting_operator_approval', request_id=args.request_id)
    monkeypatch.setattr(connect, 'import_identity', import_then_lose)
    monkeypatch.setattr(remote_fixtures.configure_local, 'configure_local', pipeline)
    monkeypatch.setattr(connect, 'open_vault', lambda *a, **k: r.vault)
    with pytest.raises(ConnectorError) as error:
        await connect.run_connect(r.args, r.host.output, r.host.root)
    assert error.value.code == 'OUTCOME_UNKNOWN'
    state = r.host.store.load()
    before_root = deepcopy(state.preferences['remote-setup.setup-test'])
    assert len(state.identities) == 1 and imports == ['agent']
    monkeypatch.setattr(connect, '_read_key', lambda *a: pytest.fail('must not reimport an acknowledged identity'))
    monkeypatch.setattr(connect, 'select', choose_action('pending' if lost_stage == 'identity_import' else 'resume'))
    args = build_parser().parse_args(['connect', '--project', str(r.host.project)])
    result = await connect.run_connect(args, r.host.output, r.host.root)
    assert result['request_id'] == args.request_id == 'setup-test'
    after = r.host.store.load()
    assert len(after.identities) == 1 and imports == ['agent']
    assert sum(key.startswith('remote-setup.') for key in after.preferences) == 1
    assert after.preferences['remote-setup.setup-test']['child_requests'] == before_root['child_requests']
    assert after.preferences['remote-setup.setup-test']['draft_revision'] == before_root['draft_revision']
    assert all(item == r.handoff['child_requests'] for item in pipelines)
    assert all(request[:2] in [('GET', '/v1/connections/me'), ('POST', '/v1/connections/remote-setup:recover')]
        for request in r.peer.requests)
