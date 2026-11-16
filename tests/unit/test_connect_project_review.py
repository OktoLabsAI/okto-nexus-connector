"""First-use selection is passive; one project review owns host effects."""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace as NS

import pytest
from nexus_connector_core import InstallationCandidate, calculate_inventory_revision
from nexus_connector_core.installation import effective_installation_ref

from okto_nexus_connector.cli.commands import connect, configure_local as local
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.errors import ConnectorError
from tests.unit import test_configure as wizard_fixtures

local_wizard = wizard_fixtures.local_wizard


@pytest.fixture
def selection(tmp_path, monkeypatch):
    candidates = [InstallationCandidate('codex_app_server', str(tmp_path / 'codex.exe'),
        'sha256:' + 'a' * 64, 'path', 'selected', build_identity='sha256:' + 'b' * 64)]
    calls = []
    async def inventory(configuration, *, observations):
        calls.append((configuration, observations))
        return candidates
    monkeypatch.setattr(connect, 'configured_candidates', inventory)
    monkeypatch.setattr(connect, 'select', lambda *a, **k: pytest.fail('unnecessary harness question'))
    monkeypatch.setattr('asyncio.create_subprocess_exec', lambda *a, **k: pytest.fail('probe before review'))
    monkeypatch.setattr('subprocess.Popen', lambda *a, **k: pytest.fail('process before review'))
    args = build_parser().parse_args(['connect'])
    return NS(args=args, candidates=candidates, calls=calls)


async def test_one_detected_installation_is_recommended_without_executing_or_prompting(selection):
    r = selection
    adapter, candidate, version = await connect._choose_harness(r.args, None)
    assert candidate is r.candidates[0] and adapter == 'codex_app_server' and version is None
    assert r.args.discovery_inventory_revision == calculate_inventory_revision(r.candidates)
    assert r.calls == [({}, ())]


async def test_adapter_hint_filters_candidates_without_requiring_executable(selection):
    r = selection
    r.candidates.append(replace(r.candidates[0], adapter_id='claude_stream'))
    r.args.harness = 'claude_stream'
    assert (await connect._choose_harness(r.args, None))[1] is r.candidates[1]


async def test_material_ambiguity_asks_once_and_preserves_exact_candidate(selection, monkeypatch):
    r = selection
    r.candidates.append(replace(r.candidates[0], executable=r.candidates[0].executable + '-second'))
    choices = []
    def choose(label, options, **kwargs):
        choices.append(options)
        return options[1][0]
    monkeypatch.setattr(connect, 'select', choose)
    assert (await connect._choose_harness(r.args, None))[1] is r.candidates[1]
    assert len(choices) == 1 and choices[0][0][0] != choices[0][1][0]


@pytest.mark.parametrize('explicit', [False, True])
async def test_pi_keeps_node_and_script_as_one_installation(selection, explicit):
    r = selection
    node = r.candidates[0].executable
    pi = replace(r.candidates[0], adapter_id='pi_rpc', launch_script=node + '-cli.js', version='test')
    r.candidates[:] = [pi]
    if explicit:
        r.args.executable, r.args.pi_node = pi.launch_script, pi.executable
    assert (await connect._choose_harness(r.args, None))[1] is pi


async def test_two_pi_scripts_sharing_node_remain_distinct_choices(selection, monkeypatch):
    r = selection
    a = replace(r.candidates[0], adapter_id='pi_rpc', launch_script='a-cli.js')
    b = replace(a, launch_script='b-cli.js')
    r.candidates[:] = [a, b]
    def choose(label, options, **kwargs):
        assert options[0][0] != options[1][0]
        assert 'a-cli.js' in options[0][1] and 'b-cli.js' in options[1][1]
        return options[1][0]
    monkeypatch.setattr(connect, 'select', choose)
    assert (await connect._choose_harness(r.args, None))[1] is b


async def test_previous_executor_discovery_and_observations_are_used(selection):
    r = selection
    r.args.discovery_configuration = {'selected': 'roots'}
    r.args.installation_observations = [{'retained': 'observation'}]
    await connect._choose_harness(r.args, None)
    assert r.calls == [(r.args.discovery_configuration, r.args.installation_observations)]


@pytest.mark.parametrize('change', ['empty', 'missing-path', 'missing-adapter', 'wrong-node', 'unsupported'])
async def test_unavailable_choice_does_not_fall_back_or_execute(selection, change):
    r = selection
    if change == 'empty':
        r.candidates.clear()
    elif change == 'missing-path':
        r.args.executable = str(Path(r.candidates[0].executable).with_name('other.exe'))
    elif change == 'missing-adapter':
        r.args.harness = 'pi_rpc'
    elif change == 'wrong-node':
        r.args.pi_node = r.candidates[0].executable
    else:
        r.args.harness = 'unsupported'
    with pytest.raises(ConnectorError) as error:
        await connect._choose_harness(r.args, None)
    assert error.value.code == ('VALIDATION_ERROR' if change == 'unsupported' else 'BINARY_NOT_FOUND')


async def test_conflicting_ref_is_not_resolved_by_inventory_order(selection):
    r = selection
    ref = effective_installation_ref(r.candidates[0])
    r.candidates.append(replace(r.candidates[0], executable='another-target', installation_ref=ref))
    with pytest.raises(ConnectorError) as error:
        await connect._choose_harness(r.args, None)
    assert error.value.code == 'INSTALLATION_REF_AMBIGUOUS'


@pytest.fixture
def project_review(local_wizard, monkeypatch):
    args, output, root, store, identity, portable, calls, gate = local_wizard
    args.project_preset_flow = True
    args.customize = False
    args.candidate_ref = 'candidate'
    args.workspace_label = None
    args.provider_home = None
    args.start_at_login = False
    args.discovery_inventory_revision = 'revision'
    questions = []
    def choose(label, options, default=None):
        questions.append(label)
        assert label.startswith('7.'), 'unexpected technical question'
        assert not calls, 'host effects preceded review'
        return True
    monkeypatch.setattr(local, 'choice', choose)
    monkeypatch.setattr(local, 'text', lambda *a, **k: pytest.fail('unnecessary setting question'))
    monkeypatch.setattr(local, 'preferences', lambda *a, **k: pytest.fail('advanced preferences required'))
    async def run():
        return await local.configure_local(args, output, root, store, identity, 'https://nexus.test', portable)
    return NS(args=args, output=output, root=root, store=store, identity=identity, portable=portable,
        calls=calls, gate=gate, questions=questions, run=run)


async def test_one_project_review_submits_existing_canonical_defaults(project_review):
    r = project_review
    assert (await r.run())['status'] == 'awaiting_operator_approval'
    assert len(r.questions) == 1
    request = r.gate['prepare_request']
    assert request['project_preset'] == {'id': 'project-generalist', 'revision': 1}
    configuration = request['connection_configuration']
    assert configuration['harness_settings'] == {} and configuration['tool_access'] == 'ask'
    assert configuration['session_policy'] == 'per_sender'
    assert configuration['authorization'] == dict(minutes=60, actions=20, no_expiry=False, unlimited_actions=False)
    saved = r.store.load().preferences['configure.server.agent.wizard-test']
    assert saved['configuration']['workspace_label'] == r.root.name
    assert saved['configuration']['provider_home'] is None
    assert r.calls[:3] == ['register', 'probe', 'launch']
    rendered = r.output.stream.getvalue()
    assert '60 minutes / 20 actions' in rendered and 'Harness default' in rendered
    assert 'No project task is sent' in rendered and 'not yet confirmed' in rendered
    assert 'node + pi.js' in rendered and 'secret-not-in-output' not in rendered


async def test_decline_review_has_no_registration_probe_daemon_or_setup_state(project_review, monkeypatch):
    r = project_review
    monkeypatch.setattr(local, 'choice', lambda *a, **k: False)
    assert (await r.run())['canceled']
    assert not r.calls and not r.store.load().preferences


async def test_changed_inventory_requires_new_review_before_any_effect(project_review):
    r = project_review
    r.args.discovery_inventory_revision = 'previous-content'
    with pytest.raises(ConnectorError) as error:
        await r.run()
    assert error.value.code == 'STALE_GENERATION'
    assert not r.calls and not r.questions and not r.store.load().preferences


async def test_customization_uses_same_wizard_and_approval(project_review, monkeypatch):
    r = project_review
    r.args.customize = True
    r.args.provider_home = str(r.root)
    edits = []
    monkeypatch.setattr(local, 'text', lambda label, default='', **kw: default)
    def edit(portable, schema):
        edits.append(schema)
        return dict(portable, harness_settings={'model': 'explicit-model'})
    monkeypatch.setattr(local, 'preferences', edit)
    assert (await r.run())['status'] == 'awaiting_operator_approval'
    assert len(edits) == 1 and len(r.questions) == 1
    assert r.gate['prepare_request']['connection_configuration']['harness_settings'] == {'model': 'explicit-model'}


async def test_pending_project_resumes_same_request_without_questions_or_defaults(project_review):
    r = project_review
    await r.run()
    before = list(r.calls)
    r.gate['approved'] = True
    assert (await r.run())['saved']
    assert r.calls[len(before):] == ['state.reload', 'apply', 'state.reload']
    assert len(r.questions) == 1


async def test_discovered_provider_home_is_visible_in_review_and_not_secret(project_review, monkeypatch):
    r = project_review
    monkeypatch.setattr(local, 'discover_provider_home', lambda adapter: str(r.root))
    await r.run()
    saved = r.store.load().preferences['configure.server.agent.wizard-test']
    assert saved['configuration']['provider_home'] == str(r.root)
    assert str(r.root) in r.output.stream.getvalue()


@pytest.mark.parametrize('adapter', ['codex_app_server', 'pi_rpc'])
async def test_connect_entry_reaches_one_review_with_current_folder_and_complete_installation(
        project_review, selection, monkeypatch, adapter):
    from okto_nexus_connector.storage.state_store import IdentityRecord, ServerProfileRecord
    r = project_review
    candidate = replace(selection.candidates[0], adapter_id=adapter,
        launch_script=str(r.root / 'pi-cli.js') if adapter == 'pi_rpc' else None)
    selection.candidates[:] = [candidate]
    state = r.store.load()
    identity = IdentityRecord('worker', 'server', 'agent', 'handle', 1, 'test')
    state.identities = [identity]
    state.identity_by_alias = lambda alias: identity if alias == 'worker' else None
    state.servers = {'server': ServerProfileRecord('server', 'https://nexus.test', 'https://nexus.test', 'test')}
    monkeypatch.setattr(connect, 'StateStore', lambda _: r.store)
    monkeypatch.setattr(connect, 'open_vault', lambda *a, **k: NS(resolve=lambda _: 'test-key'))
    monkeypatch.setattr(connect, '_read_key', lambda *a: pytest.fail('reimport instead of identity reuse'))
    class HTTP:
        def __init__(self, *a): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): pass
        async def me(self, key):
            assert key == 'test-key'
            return NS(server_id='server', agent_id='agent', credential_epoch=1)
    monkeypatch.setattr(connect, 'NexusHTTPClient', HTTP)
    monkeypatch.setattr(local, 'configured_candidates', connect.configured_candidates)
    monkeypatch.setattr(local, 'calculate_inventory_revision', calculate_inventory_revision)
    monkeypatch.setattr(local, 'effective_installation_ref', effective_installation_ref)
    def decline(label, options, default=None):
        r.questions.append(label)
        assert label.startswith('7.') and not r.calls
        return False
    monkeypatch.setattr(local, 'choice', decline)
    monkeypatch.chdir(r.root)
    args = build_parser().parse_args(['connect', '--identity', 'worker', '--no-start-at-login'])
    assert (await connect.run_connect(args, r.output, r.root))['canceled']
    assert len(r.questions) == 1 and not r.calls and not state.preferences
    rendered = r.output.stream.getvalue()
    assert str(r.root) in rendered and candidate.executable in rendered
    assert '60 minutes / 20 actions' in rendered and 'not yet confirmed' in rendered
    if candidate.launch_script:
        assert candidate.launch_script in rendered
