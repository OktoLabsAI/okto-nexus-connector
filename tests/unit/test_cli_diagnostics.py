import io
import logging
from types import SimpleNamespace as NS

import pytest

from okto_nexus_connector.cli.commands import diagnostics
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.daemon.logging_setup import configure_logging


@pytest.mark.parametrize('running,attached,error,expected', [
    (False, False, None, 'DAEMON_STOPPED'),
    (True, False, None, 'NOT_ATTACHED'),
    (True, True, None, 'CONNECTED'),
    (True, False, 'CONTROL_DISCONNECTED', 'CONNECTION_ERROR'),
    (True, True, None, 'HARNESS_ERROR'),
])
async def test_status_requires_live_agent_attachment(monkeypatch, tmp_path, running, attached, error, expected):
    state = NS(servers={'server': NS(base_url='http://nexus:8202')},
        identities=[NS(server_id='server', agent_id='claude', alias='local', revoked=False)],
        execution_bindings=[NS(server_id='server', agent_id='claude', adapter_id='claude_stream',
                               binding_id='binding', state='APPROVED')], preferences={})
    monkeypatch.setattr(diagnostics, 'StateStore', lambda _: NS(load=lambda: state))
    monkeypatch.setattr(diagnostics.manager, 'status', lambda _: NS(running=running, pid=123 if running else None))
    client = NS(call=lambda _: {'ok': True, 'result': {'r4_controls': {'server': {
        'state': 'CONTROL_READY', 'control_ready': not error, 'execution_ready': attached,
        'attached_bindings': ['binding'] if attached else [], 'error_code': error,
        'execution_errors': {'binding': 'NATIVE_PROTOCOL_INCOMPATIBLE'} if expected == 'HARNESS_ERROR' else {}}}}}, close=lambda: None)
    monkeypatch.setattr(diagnostics.manager, 'connect', lambda _: client)
    async def checked(*args):
        return {'local': {'connections': [{'binding_id':'binding', 'status':'CONNECTED'}]}}
    monkeypatch.setattr(diagnostics, '_query_connections', checked)
    result = await diagnostics.run_status(NS(agent=None), Output(json_mode=True), tmp_path)
    assert result['agents'][0]['status'] == expected
    assert result['servers'][0]['error'] == (error if running else None)
    assert 'secret_handle' not in str(result)
    if expected == 'HARNESS_ERROR':
        assert result['servers'][0]['control_ready'] is True
        assert result['connections'][0]['error'] == 'NATIVE_PROTOCOL_INCOMPATIBLE'


async def test_logs_are_persisted_redacted_and_filtered(tmp_path):
    handler = configure_logging(tmp_path)
    logger = logging.getLogger('okto_nexus_connector')
    try:
        logger.info('Runtime connected: server=one')
        logger.error('Authentication failed nxs_' + 'x' * 32)
        handler.flush()
        raw = (tmp_path / 'logs/daemon.log').read_text(encoding='utf-8')
        assert 'x' * 32 not in raw and '[redacted]' in raw
        stream = io.StringIO()
        await diagnostics.run_logs(NS(tail=50, errors=True, follow=False),
                                   Output(json_mode=False, stream=stream), tmp_path)
        assert 'Authentication failed' in stream.getvalue()
        assert 'Runtime connected' not in stream.getvalue()
        assert handler.maxBytes and handler.backupCount == 3
    finally:
        logger.removeHandler(handler)
        handler.close()


async def test_follow_reads_replacement_after_rotation(monkeypatch, tmp_path):
    folder = tmp_path / 'logs'
    folder.mkdir()
    path = folder / 'daemon.log'
    path.write_text('first\n', encoding='utf-8')
    calls = 0
    async def tick(_):
        nonlocal calls
        calls += 1
        if calls == 1:
            path.rename(folder / 'daemon.log.1')
            path.write_text('second\n', encoding='utf-8')
        else:
            raise KeyboardInterrupt
    monkeypatch.setattr(diagnostics.asyncio, 'sleep', tick)
    stream = io.StringIO()
    with pytest.raises(KeyboardInterrupt):
        await diagnostics.run_logs(NS(tail=50, errors=False, follow=True),
                                   Output(json_mode=False, stream=stream), tmp_path)
    assert stream.getvalue().splitlines() == ['first', 'second']


def test_diagnostics_parser():
    parser = build_parser()
    assert parser.parse_args(['status', '--agent', 'claude']).agent == 'claude'
    assert parser.parse_args(['logs', '--follow', '--errors']).follow
    with pytest.raises(SystemExit):
        parser.parse_args(['logs', '--tail', '0'])


async def test_status_reports_wizard_resume_id_without_exporting_configuration(monkeypatch, tmp_path):
    state = NS(servers={}, execution_bindings=[],
        identities=[NS(server_id='s', agent_id='a', alias='local', revoked=False)],
        preferences={'configure.s.a.my-request': {'stage': 'apply', 'credential': 'do-not-show'},
                     'configure.s.a.finished': {'stage': 'done'}})
    monkeypatch.setattr(diagnostics, 'StateStore', lambda _: NS(load=lambda: state))
    monkeypatch.setattr(diagnostics.manager, 'status', lambda _: NS(running=False, pid=None))
    async def checked(*args): return {}
    monkeypatch.setattr(diagnostics, '_query_connections', checked)
    result = await diagnostics.run_status(NS(agent=None), Output(json_mode=True), tmp_path)
    assert result['agents'][0]['pending_requests'] == [{'request_id': 'my-request', 'status': 'apply'}]
    assert 'do-not-show' not in str(result)


async def test_status_reports_each_connection_and_server_revocation(monkeypatch, tmp_path):
    bindings = [NS(server_id='s', agent_id='a', adapter_id='claude_stream', binding_id=b, state='APPROVED') for b in ('one','two')]
    state = NS(servers={'s':NS(base_url='http://server')}, execution_bindings=bindings,
               identities=[NS(server_id='s',agent_id='a',alias='agent',revoked=False)], preferences={})
    monkeypatch.setattr(diagnostics, 'StateStore', lambda _: NS(load=lambda:state))
    monkeypatch.setattr(diagnostics.manager, 'status', lambda _:NS(running=True,pid=1))
    monkeypatch.setattr(diagnostics.manager, 'connect', lambda _:NS(close=lambda:None,
        call=lambda _:dict(ok=True,result={'r4_controls':{'s':{'control_ready':True,'execution_ready':True,'attached_bindings':['one','two']}}})))
    async def checked(*args):
        return {'agent':{'connections':[{'binding_id':'one','status':'CONNECTED','machine_id':'machine-x'},
                                        {'binding_id':'two','status':'REVOKED','machine_id':'machine-y'}]}}
    monkeypatch.setattr(diagnostics, '_query_connections', checked)
    result = await diagnostics.run_status(NS(agent=None),Output(json_mode=True),tmp_path)
    assert [r['status'] for r in result['connections']] == ['CONNECTED','REVOKED']
    assert result['agents'][0]['status'] == 'PARTIAL'
    async def unavailable(*args): return {'agent':{'connections':[],'error':'EXECUTOR_OFFLINE'}}
    monkeypatch.setattr(diagnostics, '_query_connections', unavailable)
    result = await diagnostics.run_status(NS(agent=None),Output(json_mode=True),tmp_path)
    assert all(r['status']=='SERVER_UNREACHABLE' for r in result['connections'])
