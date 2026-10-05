"""Keep operator consent and profile facts in the R4 HTTP client DTO."""

import json

import httpx
import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.transport.https_client import MANAGEMENT_REVISION, NexusHTTPClient


def proposal_payload():
    return {
        "proposal_id": "proposal", "proposal_revision": 1,
        "expires_at": "2026-09-30T12:00:00Z", "server_id": "server",
        "executor_id": "executor", "agent_id": "agent", "binding_id": "binding",
        "endpoint_id": "endpoint", "profile_id": "profile", "workspace_id": "workspace",
        "workspace_binding_id": "workspace-binding", "adapter_id": "codex_app_server",
        "candidate_ref": "nexus-install-v1:" + "a" * 64,
        "inventory_revision": "sha256:" + "b" * 64,
        "realization_ref": "realization", "realization_revision": 1,
        "authorization_revision": 1, "configuration_revision": 1,
        "required_approvals": ["agent_confirmation", "apr_operator"],
        "can_apply": True,
        "diff": {"approved_diff_hash": "sha256:" + "c" * 64,
                 "summary": "Enable the approved endpoint and managed profile.",
                 "requires_operator": True, "fields_changed": ["profile", "enabled"]},
    }


async def prepare(http, payload):
    return await http.prepare_r4_binding(
        "agent-key", client_intent_id="prepare", executor_id=payload["executor_id"],
        adapter_id=payload["adapter_id"], candidate_ref=payload["candidate_ref"],
        inventory_revision=payload["inventory_revision"], realization_ref=payload["realization_ref"],
        workspace_id=payload["workspace_id"], alias="assistant", agent_id_hint="agent")


@pytest.mark.parametrize("profile_id", ["profile", None])
async def test_r4_consent_and_operator_proof_survive_roundtrip(profile_id):
    payload = proposal_payload()
    payload["profile_id"] = profile_id
    sent = []

    def handler(request):
        sent.append(json.loads(request.content))
        view = payload if request.url.path.endswith(":prepare") else {
            **payload, "binding_revision": 1, "state": "APPROVED"}
        return httpx.Response(200, json=view, headers={"X-Nexus-Connections-Revision": MANAGEMENT_REVISION})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
        async with NexusHTTPClient("http://127.0.0.1:8202", client=raw) as http:
            proposal = await prepare(http, payload)
            assert proposal.profile_id == profile_id
            assert proposal.requires_operator is True
            assert proposal.fields_changed == ("profile", "enabled")
            assert proposal.required_approvals == ("agent_confirmation", "apr_operator")
            binding = await http.apply_r4_binding(
                "agent-key", client_intent_id="apply", proposal=proposal,
                operator_proof_ref="apr_operator")
            assert binding.binding_id == proposal.binding_id
    assert sent[-1] == {"client_intent_id": "apply", "proposal_id": "proposal",
                        "proposal_revision": 1, "approved_diff_hash": payload["diff"]["approved_diff_hash"],
                        "operator_proof_ref": "apr_operator"}


@pytest.mark.parametrize("field,value", [("profile_id", 1), ("profile_id", ""),
                                        ("requires_operator", "true"), ("fields_changed", [1])])
async def test_r4_malformed_consent_is_refused(field, value):
    payload = proposal_payload()
    (payload if field == "profile_id" else payload["diff"])[field] = value
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(
            200, json=payload, headers={"X-Nexus-Connections-Revision": MANAGEMENT_REVISION}))) as raw:
        async with NexusHTTPClient("http://127.0.0.1:8202", client=raw) as http:
            with pytest.raises(ConnectorError) as error:
                await prepare(http, payload)
    assert error.value.code == "VERSION_INCOMPATIBLE"
