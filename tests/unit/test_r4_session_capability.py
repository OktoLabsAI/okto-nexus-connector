"""R4 credential responses are scoped, finite, origin-pinned and never retried."""

import copy
import json

import httpx
import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.transport import https_client
from okto_nexus_connector.transport.https_client import NexusHTTPClient, MANAGEMENT_REVISION
from okto_nexus_connector.redaction import redact_mapping
from tests.unit.test_execution_selection import selection


def test_session_credentials_are_redacted_from_nested_diagnostics():
    for prefix in ('nxc4_', 'nxt4_'):
        secret = prefix + 'a' * 43
        assert secret not in str(redact_mapping({'message': ['Failed: ' + secret]}))


def response_for(frame, audience='nexus-mcp-session'):
    scope = {k: frame[k] for k in ('server_id', 'executor_id', 'binding_id', 'agent_id',
        'workspace_id', 'workspace_binding_id', 'session_id', 'session_owner_generation',
        'binding_revision', 'credential_epoch', 'authorization_revision', 'configuration_revision')}
    return dict(capability_id='cap-example', capability_ref='mcp-cap:cap-example'
        if audience == 'nexus-mcp-session' else 'native-cap:cap-example', capability='secret-' + 'a' * 32,
        scope=scope, audience=audience, actions=['tools/call'], expires_in=120,
        mcp_url='https://nexus.test/mcp' if audience == 'nexus-mcp-session' else None)


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['valid', 'native', 'scope', 'epoch_type', 'audience', 'actions',
    'origin', 'path', 'query', 'secret', 'reference', 'ttl', 'ttl_type', 'extra', 'revision', 'late'])
async def test_capability_response_is_bound_to_opening_and_origin(selection, monkeypatch, change):
    frame = selection[3]
    audience = 'nexus-native-session' if change == 'native' else 'nexus-mcp-session'
    response = response_for(frame, audience)
    if change == 'scope': response['scope']['agent_id'] = 'other'
    elif change == 'epoch_type': response['scope']['credential_epoch'] = True
    elif change == 'audience': response['audience'] = 'nexus-native-session'
    elif change == 'actions': response['actions'] += ['tools/list']
    elif change == 'origin': response['mcp_url'] = 'https://other.test/mcp'
    elif change == 'path': response['mcp_url'] = 'https://nexus.test/api/admin'
    elif change == 'query': response['mcp_url'] += '?secret=bad'
    elif change == 'secret': response['capability'] = 'short'
    elif change == 'reference': response['capability_ref'] = 'native-cap:cap-example'
    elif change == 'ttl': response['expires_in'] = 121
    elif change == 'ttl_type': response['expires_in'] = True
    elif change == 'extra': response['environment'] = {}
    now = [100.0]
    # Replace the module's clock object, not global time.monotonic used by asyncio.
    class Clock:
        @staticmethod
        def monotonic(): return now[0]
    monkeypatch.setattr(https_client, 'time', Clock)
    requests = []
    def reply(request):
        requests.append(request)
        body = json.loads(request.content)
        assert request.url.path == '/v1/runtime/sessions/session/capability'
        assert body == dict(capability_request_id='request', binding_id='binding', audience=audience,
                            actions=['tools/call'], replaces_capability_id=None)
        now[0] += 120 if change == 'late' else 1
        headers = {'X-Nexus-Connections-Revision': MANAGEMENT_REVISION if change != 'revision' else 'old'}
        return httpx.Response(200, json=response, headers=headers)
    async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as raw:
        async with NexusHTTPClient('https://nexus.test', client=raw) as http:
            if change in ('valid', 'native'):
                capability = await http.request_r4_session_capability('agent-key', frame=frame,
                    capability_request_id='request', audience=audience, actions=('tools/call',))
                assert capability.deadline_monotonic == 219.5
                assert capability.capability not in repr(capability)
                assert capability.actions == ('tools/call',)
            else:
                with pytest.raises(ConnectorError) as refused:
                    await http.request_r4_session_capability('agent-key', frame=frame,
                        capability_request_id='request', audience=audience, actions=('tools/call',))
                assert refused.value.code == ('AUTH_EXPIRED' if change == 'late' else 'VERSION_INCOMPATIBLE')
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_capability_lost_reply_does_not_create_another_request(selection):
    requests = []
    def unavailable(request):
        requests.append(request)
        raise httpx.ReadTimeout('Response lost after issuance.')
    async with httpx.AsyncClient(transport=httpx.MockTransport(unavailable)) as raw:
        async with NexusHTTPClient('https://nexus.test', client=raw) as http:
            with pytest.raises(ConnectorError) as error:
                await http.request_r4_session_capability('agent-key', frame=copy.deepcopy(selection[3]),
                    capability_request_id='durable-request', audience='nexus-mcp-session', actions=('tools/call',))
            assert error.value.possible_effect and not error.value.retry_safe
    assert len(requests) == 1
