import io
from types import SimpleNamespace as NS

import pytest

from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.cli.commands import connect, configure_local
from okto_nexus_connector.errors import ConnectorError


@pytest.mark.parametrize('resume', [False, True])
async def test_connect_uses_canonical_r4_wizard(monkeypatch, tmp_path, resume):
    args = build_parser().parse_args(['connect','--server','http://nexus.test:8202','--agent','claude-coder',
        '--project',str(tmp_path),'--request-id','resume-me'])
    identity = NS(alias='claude-coder',server_id='server',agent_id='claude-coder')
    state = NS(preferences={'vault.fallback_file.approved':True})
    if resume:
        state.preferences['configure.server.claude-coder.resume-me'] = {'stage':'apply'}
    store = NS(load=lambda:state)
    monkeypatch.setattr(connect,'StateStore',lambda _:store)
    monkeypatch.setattr(connect,'open_vault',lambda *a,**k:object())
    monkeypatch.setattr(connect,'_read_key',lambda *a:'test-key')
    monkeypatch.setattr(connect,'_remember_server',lambda *a:None)
    class HTTP:
        def __init__(self,*a):pass
        async def __aenter__(self):return self
        async def __aexit__(self,*a):pass
        async def prepare_binding(self,*a,**k):pytest.fail('legacy binding request')
    monkeypatch.setattr(connect,'NexusHTTPClient',HTTP)
    async def imported(http,vault,store,**kwargs):
        assert kwargs['agent_hint']=='claude-coder' and kwargs['key']=='test-key'
        return NS(identity=identity,server_id='server',me_agent_id='claude-coder')
    monkeypatch.setattr(connect,'import_identity',imported)
    async def selected(*a):
        assert not resume, 'resume must not select another installation'
        return 'claude_stream',NS(executable='claude',installation_ref='install-ref'), '2.1.288'
    monkeypatch.setattr(connect,'_choose_harness',selected)
    async def wizard(wizard_args,output,root,actual_store,actual_identity,server,portable):
        assert actual_store is store and actual_identity is identity
        assert wizard_args.identity=='claude-coder' and wizard_args.request_id=='resume-me'
        assert wizard_args.project==str(tmp_path)
        assert server=='http://nexus.test:8202'
        if not resume:
            assert wizard_args.candidate_ref=='install-ref'
            assert portable['adapter_id']=='claude_stream'
        return {'saved':False,'status':'awaiting_operator_approval','request_id':'resume-me'}
    monkeypatch.setattr(configure_local,'configure_local',wizard)
    result = await connect.run_connect(args,Output(json_mode=False,stream=io.StringIO()),tmp_path)
    assert result['status']=='awaiting_operator_approval'


async def test_noninteractive_connect_fails_before_credentials_or_writes(monkeypatch,tmp_path):
    args=build_parser().parse_args(['--non-interactive','connect','--server','http://nexus.test'])
    monkeypatch.setattr(connect,'_read_key',lambda *a:pytest.fail('unexpected credential access'))
    with pytest.raises(ConnectorError,match='interactive R4'):
        await connect.run_connect(args,Output(json_mode=False),tmp_path)
