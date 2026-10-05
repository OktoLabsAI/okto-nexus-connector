import json
import httpx
import pytest
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.cli.commands.harness_config import run_harness_config, read_configuration_file
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.transport.https_client import NexusHTTPClient
from nexus_connector_core import discover_harness_configuration


async def test_validate_file_without_identity_or_network(tmp_path):
    path = tmp_path / 'pi.json'
    path.write_text(json.dumps(dict(format='nexus-harness-config', version=1, adapter_id='pi_rpc',
                                   settings=dict(provider='zai', model='glm-5.3', effort='low'))))
    args = build_parser().parse_args(['harness-config', 'validate', '--file', str(path)])
    result = await run_harness_config(args, None, tmp_path)
    assert result['settings']['model'] == 'glm-5.3'
    assert not (tmp_path / 'state.json').exists()
    path.write_bytes(b' ' * 65537)
    with pytest.raises(ConnectorError):
        read_configuration_file(path)


async def test_canonical_settings_transport_preserves_revision_and_parameters():
    settings = {'model':'gpt-6.1-sol', 'effort':'low', 'user_input':'enabled'}
    calls = []
    def handle(request):
        calls.append(request)
        assert request.headers['authorization'] == 'Bearer test-key'
        if request.method == 'PUT':
            assert json.loads(request.content) == {'expected_revision':4, 'settings':settings}
        return httpx.Response(200, json={'ok':True, 'data':{
            'endpoint_id':'ep_test', 'revision':4 if request.method == 'GET' else 5,
            'adapter_id':'codex_app_server', 'settings':settings,
            'configuration':discover_harness_configuration('codex_app_server')}})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        async with NexusHTTPClient('http://127.0.0.1:8202', client=client) as http:
            assert (await http.harness_settings('test-key', 'ep_test'))['revision'] == 4
            assert (await http.harness_settings('test-key', 'ep_test',
                changes={'expected_revision':4, 'settings':settings}))['revision'] == 5
    assert len(calls) == 2
