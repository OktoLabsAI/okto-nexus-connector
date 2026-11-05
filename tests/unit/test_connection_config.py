import json
from types import SimpleNamespace
import pytest
from okto_nexus_connector.cli.commands import connection_config as module
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.storage.state_store import IdentityRecord, ServerProfileRecord


def document():
    return dict(format='okto-nexus-connection',version=1,adapter_id='pi_rpc',execution_location='local',runtime_enabled=True,
        session_policy='per_sender',workspace_root='/work',workspace_label='Project',provider_home=None,secret_bindings={},
        alias='pi',harness_settings={'model':'glm-5.3','provider':'zai'},automatic_reply=True,tool_access='ask',
        authorization={'minutes':60,'actions':20})


@pytest.mark.parametrize('policy',['shared','per_sender','per_sender_session',None])
async def test_validate_full_configuration_offline(tmp_path, policy):
    file = tmp_path / 'connection.json'
    file.write_text(json.dumps(document() | {'session_policy': policy}))
    args=build_parser().parse_args(['connection-config','validate','--file',str(file)])
    config=(await module.run_connection_config(args,None,tmp_path))['configuration']
    assert config['version']==2
    assert 'workspace_root' not in config
    assert 'provider_home' not in config
    assert config['session_policy']==policy


@pytest.fixture
def setup_run(tmp_path, monkeypatch):
    file = tmp_path / 'connection.json'
    file.write_text(json.dumps(document()))
    args=build_parser().parse_args(['connection-config','apply','--file',str(file),'--identity','operator','--agent','agent',
        '--request-id','test-one','--executor-id','exe','--candidate-ref','candidate','--inventory-revision','revision',
        '--project','/destination','--workspace-label','Destination'])
    identity = IdentityRecord('operator', 'server', 'operator', 'reference', 1, 'now')
    store = module.StateStore(module.paths.state_file(tmp_path))
    def seed(state):
        state.identities.append(identity)
        state.servers['server'] = ServerProfileRecord('server', 'http://127.0.0.1:8202', 'http://127.0.0.1:8202', 'now')
    store.update(seed)
    monkeypatch.setattr(module,'_vault',lambda *a:SimpleNamespace(resolve=lambda _:'test-key'))
    scenario = SimpleNamespace(calls=[], fail=None, committed=False, test_exists=False, mismatch=False,
                               receipt_error=None, test_status='succeeded', malformed=False)
    storage_key = 'connection-setup.server.operator.test-one'
    result = {'saved': True, 'agent_id': 'agent', 'binding': {'binding_id': 'existing'}}
    class HTTP:
        def __init__(self,*a): pass
        async def __aenter__(self):return self
        async def __aexit__(self,*a):pass
        async def me(self,key):return identity
        async def _request(self,method,path,**kwargs):
            scenario.calls.append((method,path,kwargs.get('json_body')))
            if path.endswith('/setup/agent'):
                return {'baseline': {'execution_revision': 0}, 'runtime_default': True}
            saved = store.load().preferences[storage_key]
            if path.endswith(':recover'):
                if scenario.receipt_error:
                    raise scenario.receipt_error
                if scenario.mismatch:
                    raise ConnectorError('CONFLICT', 'connection.setup_commit')
                if not scenario.committed:
                    raise ConnectorError('NOT_FOUND', 'connection.setup_commit')
                return {'setup_run_id': 'another' if scenario.malformed else args.request_id,
                        'state': 'committed', 'request_hash': 'sha256:server-normalized', 'result': result}
            if path.endswith(':test'):
                assert saved['stage'] == 'test_submitted'
                scenario.test_exists = True
            elif '/setup-tests/' in path:
                if not scenario.test_exists:
                    raise ConnectorError('CONFLICT', 'connection.test')
            elif path.endswith(':finish'):
                assert saved['stage'] == 'finish'
                scenario.committed = True
            else:
                pytest.fail('Unexpected HTTP call: ' + path)
            if scenario.fail and path.endswith(':' + scenario.fail):
                scenario.fail = None
                raise RuntimeError('lost reply')
            return result if path.endswith(':finish') else {'status': scenario.test_status, 'stage': 'Connection verified'}
    monkeypatch.setattr(module,'NexusHTTPClient',HTTP)
    async def run():
        return await module.run_connection_config(args, None, tmp_path)
    return SimpleNamespace(run=run, state=scenario, store=store, key=storage_key, args=args, result=result)


async def test_lost_finish_reply_recovers_receipt_without_retesting_or_reapplying(setup_run):
    setup_run.state.fail = 'finish'
    with pytest.raises(RuntimeError):
        await setup_run.run()
    assert await setup_run.run() == setup_run.result
    assert await setup_run.run() == setup_run.result
    posts = [c for c in setup_run.state.calls if c[0] == 'POST']
    assert [c[1].rsplit(':', 1)[1] for c in posts] == ['test', 'finish', 'recover']
    assert posts[0][2] == posts[1][2] == posts[2][2]
    assert posts[0][2]['configuration']['workspace_root'] == '/destination'
    assert posts[0][2]['configuration']['provider_home'] is None
    saved = setup_run.store.load().preferences[setup_run.key]
    assert saved['stage'] == 'done' and saved['result'] == setup_run.result
    assert 'test-key' not in setup_run.store.path.read_text()


async def test_lost_test_reply_observes_existing_test_before_finish(setup_run):
    setup_run.state.fail = 'test'
    with pytest.raises(RuntimeError):
        await setup_run.run()
    assert await setup_run.run() == setup_run.result
    calls = [(method, path.rsplit('/', 1)[1]) for method, path, _ in setup_run.state.calls]
    assert calls == [('GET', 'agent'), ('POST', 'setup:test'), ('POST', 'setup:recover'),
                     ('GET', 'test-one'), ('POST', 'setup:finish')]


@pytest.mark.parametrize('legacy_stage', [False, True])
async def test_restart_without_test_or_commit_does_not_repeat_model_turn(setup_run, legacy_stage):
    setup_run.state.fail = 'test'
    with pytest.raises(RuntimeError):
        await setup_run.run()
    setup_run.state.test_exists = False
    if legacy_stage:
        setup_run.store.update(lambda state: state.preferences[setup_run.key].update(stage='test'))
    for _ in range(2):
        with pytest.raises(ConnectorError) as unknown:
            await setup_run.run()
        assert unknown.value.code == 'SETUP_TEST_UNCONFIRMED'
        assert unknown.value.possible_effect and not unknown.value.retry_safe
        assert unknown.value.operation_id == 'test-one'
    assert len([c for c in setup_run.state.calls if c[1].endswith(':test')]) == 1
    assert not any(c[1].endswith(':finish') for c in setup_run.state.calls)


@pytest.mark.parametrize('failure', ['old_server', 'unauthorized', 'changed_request', 'invalid_receipt'])
async def test_unverifiable_receipt_never_repeats_setup(setup_run, failure):
    setup_run.state.fail = 'finish'
    with pytest.raises(RuntimeError):
        await setup_run.run()
    if failure == 'old_server':
        setup_run.state.receipt_error = ConnectorError('AGENT_AUTH_REQUIRED', 'http', 'HTTP 404')
    elif failure == 'unauthorized':
        setup_run.state.receipt_error = ConnectorError('PERMISSION_DENIED', 'connection.setup_commit')
    elif failure == 'changed_request':
        setup_run.state.mismatch = True
    else:
        setup_run.state.malformed = True
    with pytest.raises(ConnectorError) as unknown:
        await setup_run.run()
    assert unknown.value.possible_effect and not unknown.value.retry_safe
    assert len([c for c in setup_run.state.calls if c[1].endswith(':test')]) == 1
    assert len([c for c in setup_run.state.calls if c[1].endswith(':finish')]) == 1
    assert setup_run.store.load().preferences[setup_run.key]['stage'] == 'finish'


async def test_failure_does_not_automatically_retest(setup_run):
    setup_run.state.test_status = 'failed'
    assert (await setup_run.run())['saved'] is False
    assert (await setup_run.run())['saved'] is False
    assert len([c for c in setup_run.state.calls if c[1].endswith(':test')]) == 1
    assert not setup_run.state.committed


async def test_changed_configuration_is_rejected_before_http(setup_run):
    setup_run.state.fail = 'test'
    with pytest.raises(RuntimeError):
        await setup_run.run()
    before = list(setup_run.state.calls)
    setup_run.args.project = '/other-project'
    with pytest.raises(ConnectorError, match='OPERATION_CONFLICT'):
        await setup_run.run()
    assert setup_run.state.calls == before


@pytest.mark.parametrize('changed', [None, '--agent', '--harness', '--project', '--candidate-ref', '--workspace-id'])
async def test_server_wizard_resumes_before_any_discovery_or_new_questions(setup_run, tmp_path, monkeypatch, changed):
    from okto_nexus_connector.cli.commands import configure

    setup_run.state.fail = 'finish'
    with pytest.raises(RuntimeError):
        await setup_run.run()
    async def select(*args):
        state = setup_run.store.load()
        return setup_run.store, state.identities[0], state.servers['server'].base_url
    def unexpected(*args, **kwargs):
        pytest.fail('Resume must not rediscover or ask for a new configuration')
    monkeypatch.setattr(configure, 'select_identity', select)
    monkeypatch.setattr(configure, 'choice', unexpected)
    monkeypatch.setattr(configure, 'text', unexpected)
    monkeypatch.setattr(configure, 'NexusHTTPClient', unexpected)
    flags = ['configure', '--host', 'server', '--identity', 'operator', '--request-id', 'test-one']
    if changed:
        flags.extend([changed, 'another-value'])
    args = build_parser().parse_args(flags)
    output = SimpleNamespace(line=lambda _: None, heading=lambda _: None)
    before = list(setup_run.state.calls)
    if changed:
        with pytest.raises(ConnectorError, match='OPERATION_CONFLICT'):
            await configure.run_configure(args, output, tmp_path)
        assert setup_run.state.calls == before
    else:
        assert await configure.run_configure(args, output, tmp_path) == setup_run.result
        assert setup_run.state.calls[-1][1].endswith(':recover')
    assert len([c for c in setup_run.state.calls if c[1].endswith(':test')]) == 1
    assert len([c for c in setup_run.state.calls if c[1].endswith(':finish')]) == 1
