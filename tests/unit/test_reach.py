import json

import httpx
import pytest

from okto_nexus_connector.cli.main import main
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.transport.https_client import NexusHTTPClient


PAYLOAD = dict(service='okto-nexus', server_version='0.2.0',
               server_core_version='0.2.62.dev0', minimum_cli_version='0.5.0.dev0')


async def test_public_probe_sends_no_credentials():
    def respond(request):
        assert request.url.path == '/v1/reach'
        assert 'authorization' not in request.headers
        return httpx.Response(200, json=PAYLOAD)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        async with NexusHTTPClient('https://nexus.test', client=http) as client:
            result = await client.reach()
    assert result == {k: v for k, v in PAYLOAD.items() if k != 'service'}


@pytest.mark.parametrize('payload', [{}, dict(PAYLOAD, service='another-server'),
    dict(PAYLOAD, minimum_cli_version=123), dict(PAYLOAD, server_core_version=[])])
async def test_malformed_probe_is_not_reported_as_success(payload):
    async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json=payload))) as http:
        async with NexusHTTPClient('https://nexus.test', client=http) as client:
            with pytest.raises(ConnectorError, match='Invalid Nexus reach response'):
                await client.reach()


def test_cli_json_needs_no_identity_or_state(tmp_path, monkeypatch, capsys):
    async def reach(self): return {k: v for k, v in PAYLOAD.items() if k != 'service'}
    monkeypatch.setattr(NexusHTTPClient, 'reach', reach)
    root = tmp_path / 'not-created'
    assert main(['--json', '--state-dir', str(root), 'reach', '--server', 'http://127.0.0.1:8202']) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['reachable'] is True and result['minimum_cli_version'] == '0.5.0.dev0'
    assert result['cli_version'] and result['latency_ms'] >= 0
    assert not root.exists()


def test_cli_unreachable_returns_nonzero(monkeypatch, capsys):
    async def reach(self): raise ConnectorError('EXECUTOR_OFFLINE', 'http', 'Server unavailable.')
    monkeypatch.setattr(NexusHTTPClient, 'reach', reach)
    assert main(['--json', 'reach', '--server', 'http://127.0.0.1:8202']) == 3
    assert json.loads(capsys.readouterr().out)['error']['code'] == 'EXECUTOR_OFFLINE'
