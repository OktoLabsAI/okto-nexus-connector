import io
from types import SimpleNamespace as NS
import pytest
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.cli import wizard_prompts
from okto_nexus_connector.cli.commands import configure,configure_local as local
from okto_nexus_connector.errors import ConnectorError


def test_choice_retries_invalid_input_and_preserves_default(monkeypatch,capsys):
    answers=iter(['bad','8',''])
    monkeypatch.setattr('builtins.input',lambda:next(answers))
    assert wizard_prompts.choice('Harness',[('pi','Pi'),('codex','Codex')],'codex')=='codex'
    assert 'Select a number' in capsys.readouterr().err


def test_eof_cancels_required_questions(monkeypatch):
    def eof():raise EOFError()
    monkeypatch.setattr('builtins.input',eof)
    with pytest.raises(ConnectorError,match='canceled'):wizard_prompts.text('Server',required=True)


async def test_noninteractive_wizard_never_prompts(tmp_path,monkeypatch):
    args=build_parser().parse_args(['--non-interactive','configure'])
    monkeypatch.setattr('builtins.input',lambda:pytest.fail('unexpected question'))
    with pytest.raises(ConnectorError,match='interactive terminal'):
        await configure.run_configure(args,Output(json_mode=False),tmp_path)


async def test_existing_identity_does_not_request_key(tmp_path,monkeypatch):
    identity=NS(alias='worker',server_id='server',agent_id='agent',revoked=False)
    state=NS(identities=[identity],identity_by_alias=lambda a:identity if a=='worker' else None,
        servers={'server':NS(base_url='http://127.0.0.1:8202')})
    monkeypatch.setattr(configure,'StateStore',lambda _:NS(load=lambda:state))
    async def unexpected(*a):pytest.fail('existing key was requested again')
    monkeypatch.setattr(configure,'run_identity',unexpected)
    args=build_parser().parse_args(['configure','--identity','worker'])
    assert (await configure.select_identity(args,Output(json_mode=False),tmp_path))[1] is identity


@pytest.fixture
def local_wizard(tmp_path,monkeypatch):
    args=build_parser().parse_args(['configure','--identity','worker','--project',str(tmp_path),
        '--provider-home',str(tmp_path),'--workspace-label','Test','--request-id','wizard-test','--operator-identity','operator'])
    identity=NS(server_id='server',agent_id='agent',secret_handle='handle',alias='worker',revoked=False)
    operator=NS(server_id='server',agent_id='operator',secret_handle='operator-handle',alias='operator',revoked=False)
    state=NS(preferences={},execution_executors=[],identities=[identity,operator],identity_by_alias=lambda a:operator if a=='operator' else identity)
    store=NS(load=lambda:state,update=lambda f:f(state))
    candidate=NS(adapter_id='pi_rpc',executable='node',launch_script='pi.js',version='1')
    calls=[]
    async def candidates(*a,**k):return [candidate]
    monkeypatch.setattr(local,'configured_candidates',candidates)
    monkeypatch.setattr(local,'effective_installation_ref',lambda c:'candidate')
    monkeypatch.setattr(local,'calculate_inventory_revision',lambda c:'revision')
    monkeypatch.setattr(local,'discover_provider_home',lambda a:None)
    monkeypatch.setattr(local,'preferences',lambda c,s:c)
    monkeypatch.setattr(local,'text',lambda label,default='',**k:default)
    monkeypatch.setattr(local,'choice',lambda label,options,default=None:options[0][0])
    monkeypatch.setattr(local,'_vault',lambda *a:NS(resolve=lambda _:'secret-not-in-output'))
    class Register:
        def __init__(self,*a):pass
        async def register(self,**k):
            calls.append('register')
            state.execution_executors=[NS(server_id='server',executor_id='executor',discovery_configuration={},installation_observations=[])]
            return state.execution_executors[0]
    monkeypatch.setattr(local,'ExecutorRegistrationService',Register)
    async def probe(*a,**k):calls.append('probe')
    monkeypatch.setattr(local,'observe_installation',probe)
    class Onboard:
        def __init__(self,*a):pass
        async def configure_launch(self,**k):calls.append('launch');return NS(configuration_digest='digest')
    monkeypatch.setattr(local,'ExecutorOnboarding',Onboard)
    monkeypatch.setattr(local.manager,'start',lambda _:None)
    async def ipc(root,method,body=None):
        calls.append(method)
        return {'realization_ref':'realization'}
    monkeypatch.setattr(local,'ipc',ipc)
    class HTTP:
        def __init__(self,*a):pass
        async def __aenter__(self):return self
        async def __aexit__(self,*a):pass
        async def read_r4_inventory(self,*a,**k):return {'freshness':'FRESH','snapshot':{'inventory_revision':'revision'}}
    monkeypatch.setattr(local,'NexusHTTPClient',HTTP)
    gate={'approved':False}
    class Bind:
        def __init__(self,*a):pass
        async def prepare(self,**k):calls.append('prepare');return {'proposal':{'required_approvals':['apr_test'],'approved_diff_hash':'hash'}}
        async def apply(self,**k):
            calls.append('apply')
            assert k['operator_proof_ref']=='apr_test'
            if not gate['approved']:raise ConnectorError('PERMISSION_DENIED','apply','Operator approval required')
            return {'binding':{'binding_id':'binding','workspace_id':'workspace'}}
    monkeypatch.setattr(local,'BindingOnboarding',Bind)
    async def finish(args,output,root,c):
        calls.append('finish')
        assert args.identity=='operator' and args.agent=='agent'
        assert c['execution_location']=='remote' and c['workspace_root']==''
        assert c['provider_home'] is None
        return {'saved':True}
    monkeypatch.setattr(local,'apply_configuration',finish)
    output=Output(json_mode=False,stream=io.StringIO())
    portable=configure.template() | {'adapter_id':'pi_rpc'}
    return args,output,tmp_path,store,identity,portable,calls,gate


async def test_local_wizard_resumes_approval_without_recreating_connection(local_wizard):
    args,output,root,store,identity,portable,calls,gate=local_wizard
    result=await local.configure_local(args,output,root,store,identity,'http://127.0.0.1:8202',portable)
    assert result['status']=='awaiting_operator_approval'
    assert 'finish' not in calls
    gate['approved']=True
    result=await local.configure_local(args,output,root,store,identity,'http://127.0.0.1:8202',portable)
    assert result['saved'] and result['runtime_started'] is False
    assert calls.count('register')==calls.count('prepare')==calls.count('executor.realize')==1
    before=list(calls)
    assert await local.configure_local(args,output,root,store,identity,'http://127.0.0.1:8202',portable)==result
    assert calls==before
    assert 'secret-not-in-output' not in output.stream.getvalue()


async def test_cancel_does_not_register_or_save(local_wizard,monkeypatch):
    args,output,root,store,identity,portable,calls,_=local_wizard
    monkeypatch.setattr(local,'choice',lambda label,options,default=None:False if label.startswith('7.') else options[0][0])
    result=await local.configure_local(args,output,root,store,identity,'http://127.0.0.1:8202',portable)
    assert result['canceled']
    assert calls==[] and store.load().preferences=={}
