"""Contract: HTTPS client against the fake peer (plan C04.1, A.5).

Covers /me identity validation, hint comparison, cross-origin redirect
refusal and authentication failures. Contract PASS only — the real Server
integration remains J-gated.
"""

from __future__ import annotations

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.transport.https_client import (
    NexusHTTPClient, is_loopback_origin, origin_of,
)
from tests.fakes.http_peer import FakeAgent, FakeNexusHTTPPeer

KEY_A = "nxs_key_agent_a"
KEY_B = "nxs_key_agent_b"


@pytest.fixture
async def peer():
    server = FakeNexusHTTPPeer()
    server.add_agent(FakeAgent(agent_id="ag_111", key=KEY_A))
    server.add_agent(FakeAgent(agent_id="ag_222", key=KEY_B,
                               server_id="srv_fake"))
    url = await server.start()
    yield server, url
    await server.stop()


async def test_me_resolves_authenticated_identity(peer):
    server, url = peer
    async with NexusHTTPClient(url) as http:
        me = await http.me(KEY_A)
    assert me.agent_id == "ag_111"
    assert me.server_id == "srv_fake"


async def test_wrong_key_rejected(peer):
    server, url = peer
    async with NexusHTTPClient(url) as http:
        with pytest.raises(ConnectorError) as error:
            await http.me("nxs_wrong_key")
    assert error.value.code == "AGENT_AUTH_REQUIRED"


async def test_revoked_key_rejected(peer):
    server, url = peer
    server.agents[KEY_B].revoked = True
    async with NexusHTTPClient(url) as http:
        with pytest.raises(ConnectorError) as error:
            await http.me(KEY_B)
    assert error.value.code == "AGENT_REVOKED"


async def test_prepare_apply_binding_roundtrip(peer):
    server, url = peer
    async with NexusHTTPClient(url) as http:
        proposal = await http.prepare_binding(
            KEY_A, agent_id_hint="ag_111", connector_id="conn_1",
            adapter_id="codex_app_server", candidate_version="0.157.0",
            binding_alias="codex")
        assert proposal.binding_id.startswith("bind_")
        applied = await http.apply_binding(KEY_A, proposal)
        assert applied.binding_id == proposal.binding_id
        ticket, expires = await http.binding_ticket(KEY_A,
                                                    proposal.binding_id)
        assert ticket.startswith("nstkt_") and expires > 0
        # the issued ticket is accepted exactly once server-side
        assert ticket in server.tickets


async def test_ticket_refused_for_foreign_binding(peer):
    """TC-16: ticket of A cannot serve binding of B."""
    server, url = peer
    async with NexusHTTPClient(url) as http:
        proposal_a = await http.prepare_binding(
            KEY_A, agent_id_hint="ag_111", connector_id="conn_1",
            adapter_id="codex_app_server", candidate_version="x",
            binding_alias="a")
        await http.apply_binding(KEY_A, proposal_a)
        proposal_b = await http.prepare_binding(
            KEY_B, agent_id_hint="ag_222", connector_id="conn_1",
            adapter_id="pi_rpc", candidate_version="x",
            binding_alias="b")
        applied_b = await http.apply_binding(KEY_B, proposal_b)
        with pytest.raises(ConnectorError) as error:
            await http.binding_ticket(KEY_A, applied_b.binding_id)
    assert error.value.code == "BINDING_NOT_AUTHORIZED"


async def test_hint_mismatch_refused_by_server(peer):
    server, url = peer
    async with NexusHTTPClient(url) as http:
        with pytest.raises(ConnectorError) as error:
            await http.prepare_binding(
                KEY_A, agent_id_hint="ag_222", connector_id="conn_1",
                adapter_id="codex_app_server", candidate_version="x",
                binding_alias="x")
    assert error.value.code == "AGENT_ID_MISMATCH"


async def test_intent_resolution_and_operations(peer):
    server, url = peer
    async with NexusHTTPClient(url) as http:
        proposal = await http.prepare_binding(
            KEY_A, agent_id_hint="ag_111", connector_id="conn_1",
            adapter_id="codex_app_server", candidate_version="x",
            binding_alias="codex")
        applied = await http.apply_binding(KEY_A, proposal)
        resolution = await http.resolve_intent(
            KEY_A, binding_id=applied.binding_id,
            workspace_binding_id=applied.workspace_binding_id,
            intent="runtime.start", new_session=True)
        assert resolution.session_id.startswith("rs_")
        assert "turn.submit" in resolution.allowed_actions
        receipt = await http.submit_operation(
            KEY_A, operation_id=resolution.operation_id,
            binding_id=applied.binding_id,
            session_id=resolution.session_id, action="turn.submit",
            payload={"text_length": 5})
        assert receipt["stage"] == "RECEIVED_DURABLE"
        fetched = await http.get_operation(
            KEY_A, resolution.operation_id)
        assert fetched["operation_id"] == resolution.operation_id


async def test_session_capability_shape(peer):
    server, url = peer
    async with NexusHTTPClient(url) as http:
        proposal = await http.prepare_binding(
            KEY_A, agent_id_hint="ag_111", connector_id="conn_1",
            adapter_id="claude_stream", candidate_version="x",
            binding_alias="claude")
        applied = await http.apply_binding(KEY_A, proposal)
        resolution = await http.resolve_intent(
            KEY_A, binding_id=applied.binding_id,
            workspace_binding_id=applied.workspace_binding_id,
            intent="runtime.start", new_session=True)
        capability = await http.session_capability(
            KEY_A, binding_id=applied.binding_id,
            session_id=resolution.session_id,
            actions=("tools/call",))
        assert capability.capability_ref.startswith("mcp-cap:")
        assert capability.capability.startswith("nxcap_")


async def test_cross_origin_redirect_refused(peer):
    """TC-05: credential redirects to another origin are refused."""
    server, url = peer
    server.redirect_cross_origin = True
    async with NexusHTTPClient(url) as http:
        with pytest.raises(ConnectorError) as error:
            await http.me(KEY_A)
    assert error.value.code == "PROFILE_DRIFT"


async def test_loopback_http_allowed_non_loopback_tls_required():
    assert is_loopback_origin("http://127.0.0.1:8080") is True
    assert is_loopback_origin("https://nexus.example") is False
    with pytest.raises(ConnectorError):
        NexusHTTPClient("https://nexus.example", verify=False)


async def test_origin_normalization():
    assert origin_of("https://Nexus.Example:443/x") == \
        "https://nexus.example"
    assert origin_of("http://127.0.0.1:9000/x") == "http://127.0.0.1:9000"
