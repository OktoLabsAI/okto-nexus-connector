"""Contract: HTTPS client against the fake peer (plan C04.1, A.5).

Covers /me identity validation, hint comparison, cross-origin redirect
refusal and authentication failures. Contract PASS only — the real Server
integration remains J-gated.
"""

from __future__ import annotations

import pytest
import httpx

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


async def test_r4_registration_and_inventory_use_distinct_bearers():
    seen = []
    revision = "nexus-connections-2026-09-29-r4"

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, request.headers.get("authorization")))
        if request.url.path.endswith("executors:register"):
            return httpx.Response(201, headers={
                "X-Nexus-Connections-Revision": revision}, json={
                    "server_id": "srv", "executor_id": "exe",
                    "connector_id": "connector", "state": "AWAITING_INVENTORY",
                    "bootstrap_ticket": {
                        "ticket_id": "ticket", "ticket": "nxt4_" + "x" * 48,
                        "executor_id": "exe", "binding_id": None,
                        "agent_id": "agent", "expires_in": 300,
                        "credential_epoch": 1, "authorization_revision": 1,
                        "audience": "nexus-executor-control",
                        "scopes": ["inventory:publish", "link:connect"],
                    },
                })
        assert request.url.path == "/v1/runtime/executors/exe/inventory"
        return httpx.Response(200, headers={
            "X-Nexus-Connections-Revision": revision}, json={
                "server_id": "srv", "executor_id": "exe",
                "publication_sequence": 1,
                "inventory_revision": "sha256:" + "a" * 64,
                "fresh_for_ms": 120000, "accepted": True,
            })

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    async with client:
        async with NexusHTTPClient("http://127.0.0.1:8202", client=client) as http:
            registered = await http.register_executor(
                "nxs_agent", client_intent_id="intent", connector_id="connector",
                label="Remote host")
            accepted = await http.publish_inventory(
                registered.bootstrap_ticket, executor_id=registered.executor_id,
                snapshot={"server_id": "srv", "executor_id": "exe",
                          "publication_sequence": 1,
                          "inventory_revision": "sha256:" + "a" * 64},
            )
    assert accepted.fresh_for_ms == 120000
    assert seen == [
        ("/v1/connections/executors:register", "Bearer nxs_agent"),
        ("/v1/runtime/executors/exe/inventory", "Bearer nxt4_" + "x" * 48),
    ]


async def test_r4_management_revision_is_required():
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "agent_id": "agent", "server_id": "srv", "display_name": "Agent",
            "permissions": [], "revisions": {
                "authorization": 1, "configuration": 1, "credential_epoch": 1},
        })

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    async with client:
        async with NexusHTTPClient("http://127.0.0.1:8202", client=client) as http:
            with pytest.raises(ConnectorError) as error:
                await http.me("nxs_agent")
    assert error.value.code == "VERSION_INCOMPATIBLE"


async def test_r4_receipt_publication_uses_binding_ticket_and_exact_ack():
    from nexus_connector_core import R4_PREVIEW_REVISION

    frame = {
        "protocol_major": 1, "contract_revision": R4_PREVIEW_REVISION,
        "type": "operation.receipt", "server_id": "srv",
        "executor_id": "exe", "binding_id": "binding", "agent_id": "agent",
        "session_id": "session", "connection_id": "control",
        "connection_generation": 1, "operation_id": "op",
        "intent_hash": "sha256:" + "a" * 64, "receipt_revision": 1,
        "stage": "RECEIVED_DURABLE", "possible_effect": False,
        "retry_safe": True,
    }
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, request.headers.get("authorization"),
                     request.read()))
        return httpx.Response(200, headers={
            "X-Nexus-Connections-Revision": "nexus-connections-2026-09-29-r4",
        }, json={"operation_id": "op", "receipt_revision": 1,
                 "stage": "RECEIVED_DURABLE", "accepted": True, "reused": False})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    async with client:
        async with NexusHTTPClient("http://127.0.0.1:8202", client=client) as http:
            accepted = await http.publish_operation_receipt(
                "nxt4_binding-ticket", frame=frame)
            with pytest.raises(ConnectorError):
                await http.publish_operation_receipt(
                    "nxt4_binding-ticket", frame={**frame, "stage": "unknown"})
    assert accepted.operation_id == "op" and not accepted.reused
    assert seen[0][0] == "/v1/runtime/operations/op/receipts"
    assert seen[0][1] == "Bearer nxt4_binding-ticket"
    assert len(seen) == 1
