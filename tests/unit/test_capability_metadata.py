"""Metadata reads preserve nonce, origin, exact scope and monotonic lifetime."""

import httpx
import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.transport import https_client
from okto_nexus_connector.transport.https_client import NexusHTTPClient, MANAGEMENT_REVISION
from tests.unit.test_execution_selection import selection


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['none', 'nonce', 'secret', 'scope', 'boolean', 'serial', 'origin', 'late'])
async def test_metadata_is_correlated_and_never_reanchors_old_response(selection, monkeypatch, fault):
    frame = selection[3]
    now = [100.]
    class Clock:
        @staticmethod
        def monotonic(): return now[0]
    monkeypatch.setattr(https_client, 'time', Clock)
    requests = []
    def reply(request):
        requests.append(request)
        assert request.method == 'GET'
        scope = {k: frame[k] for k in ('server_id', 'executor_id', 'binding_id', 'agent_id',
            'workspace_id', 'workspace_binding_id', 'session_id', 'session_owner_generation',
            'binding_revision', 'credential_epoch', 'authorization_revision', 'configuration_revision')}
        response = dict(request_id=request.url.params['request_id'], capability_id='cap-test',
            capability_ref='mcp-cap:cap-test', scope=scope, audience='nexus-mcp-session',
            actions=['tools/call'], expires_in=60, lease_id='lease', lease_serial=2,
            mcp_url='https://nexus.test/mcp')
        if fault == 'nonce': response['request_id'] = 'old'
        elif fault == 'secret': response['capability'] = 'unexpected-secret'
        elif fault == 'scope': response['scope']['agent_id'] = 'other'
        elif fault == 'boolean': response['scope']['credential_epoch'] = True
        elif fault == 'serial': response['lease_serial'] = True
        elif fault == 'origin': response['mcp_url'] = 'https://other.test/mcp'
        elif fault == 'late': now[0] = 160.
        return httpx.Response(200, json=response, headers={'X-Nexus-Connections-Revision': MANAGEMENT_REVISION})
    async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as raw:
        async with NexusHTTPClient('https://nexus.test', client=raw) as http:
            kwargs = dict(frame=frame, capability_id='cap-test', audience='nexus-mcp-session', actions=('tools/call',))
            if fault == 'none':
                first = await http.describe_r4_session_capability('key', **kwargs)
                second = await http.describe_r4_session_capability('key', **kwargs)
                assert first.deadline_monotonic == second.deadline_monotonic == 159.5
                assert requests[0].url.params['request_id'] != requests[1].url.params['request_id']
            else:
                with pytest.raises(ConnectorError) as rejected:
                    await http.describe_r4_session_capability('key', **kwargs)
                assert rejected.value.code == ('AUTH_EXPIRED' if fault == 'late' else 'VERSION_INCOMPATIBLE')
                assert len(requests) == 1
