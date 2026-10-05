import json
from types import SimpleNamespace
import pytest
from okto_nexus_connector.cli.commands import connection_config as module
from okto_nexus_connector.cli.main import build_parser


def document():
    return dict(format='okto-nexus-connection',version=1,adapter_id='pi_rpc',execution_location='local',runtime_enabled=True,
        session_policy='per_sender',workspace_root='/work',workspace_label='Project',provider_home=None,secret_bindings={},
        alias='pi',harness_settings={'model':'glm-5.3','provider':'zai'},automatic_reply=True,tool_access='ask',
        authorization={'minutes':60,'actions':20})


@pytest.mark.parametrize('policy',['shared','per_sender','per_sender_session',None])
async def test_validate_full_configuration_offline(tmp_path, policy):
    file=tmp_path/'connection.json';file.write_text(json.dumps(document() | {'session_policy':policy}))
    args=build_parser().parse_args(['connection-config','validate','--file',str(file)])
    config=(await module.run_connection_config(args,None,tmp_path))['configuration']
    assert config['version']==2
    assert 'workspace_root' not in config
    assert 'provider_home' not in config
    assert config['session_policy']==policy


async def test_lost_finish_reply_retries_exact_request_without_retesting(tmp_path,monkeypatch):
    file=tmp_path/'connection.json';file.write_text(json.dumps(document()))
    args=build_parser().parse_args(['connection-config','apply','--file',str(file),'--identity','operator','--agent','agent',
        '--request-id','test-one','--executor-id','exe','--candidate-ref','candidate','--inventory-revision','revision',
        '--project','/destination','--workspace-label','Destination'])
    identity=SimpleNamespace(server_id='server',agent_id='operator',secret_handle='reference')
    state=SimpleNamespace(preferences={},servers={'server':SimpleNamespace(base_url='http://127.0.0.1:8202')},identity_by_alias=lambda _:identity)
    store=SimpleNamespace(load=lambda:state,update=lambda f:f(state))
    monkeypatch.setattr(module,'StateStore',lambda _:store)
    monkeypatch.setattr(module,'_vault',lambda *a:SimpleNamespace(resolve=lambda _:'test-key'))
    calls=[]
    class HTTP:
        def __init__(self,*a): pass
        async def __aenter__(self):return self
        async def __aexit__(self,*a):pass
        async def me(self,key):return identity
        async def _request(self,method,path,**kwargs):
            calls.append((method,path,kwargs.get('json_body')))
            if method=='GET':return {'baseline':{'execution_revision':0}}
            if path.endswith(':test'):return {'status':'succeeded','stage':'Connection verified'}
            if len([c for c in calls if c[1].endswith(':finish')])==1:raise RuntimeError('lost reply')
            return {'saved':True}
    monkeypatch.setattr(module,'NexusHTTPClient',HTTP)
    with pytest.raises(RuntimeError):await module.run_connection_config(args,None,tmp_path)
    assert await module.run_connection_config(args,None,tmp_path)=={'saved':True}
    assert len([c for c in calls if c[1].endswith(':test')])==1
    finishes=[c[2] for c in calls if c[1].endswith(':finish')]
    assert finishes[0]==finishes[1]
    assert finishes[0]['configuration']['workspace_root']=='/destination'
    assert finishes[0]['configuration']['provider_home'] is None
