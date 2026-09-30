"""Native HTTP actions retain identity and never retry uncertain mutations."""
import asyncio
from dataclasses import replace
import json
import time

import httpx
import pytest

from nexus_connector_core import CoreError
from nexus_connector_core.native_action_bridge import ContextGet, HandoffClaim, HandoffComplete
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.transport.https_client import (
    MANAGEMENT_REVISION, NexusHTTPClient, R4SessionCapability,
)
from okto_nexus_connector.transport.native_actions import NexusNativeActions

SCOPE = dict(server_id="srv", executor_id="exe", binding_id="binding", agent_id="agent",
             workspace_id="ws", workspace_binding_id="wxb", session_id="session",
             session_owner_generation=1, authorization_revision=1, configuration_revision=1,
             binding_revision=1, credential_epoch=1)


def capability(**changes):
    return replace(R4SessionCapability("cap", "native-cap:cap", "nxc4_" + "x" * 40,
        dict(SCOPE), "nexus-native-session",
        ("handoff.get", "handoff.claim", "handoff.complete"), 60,
        time.monotonic() + 60, None), **changes)


def request(action="claim"):
    base = ("original-id", "session", "native-cap:cap", "work")
    return (ContextGet(*base) if action == "context" else
            HandoffComplete(*base, 1, {"summary": "Done."}) if action == "complete" else
            HandoffClaim(*base, "original-key"))


def response(action="claim"):
    state = {"context": "OPEN", "claim": "CLAIMED", "complete": "COMPLETED"}[action]
    return dict(action_id="original-id", action=action, state=state,
                result=dict(handoff_id="work", status=state, claim_epoch=1))


@pytest.mark.parametrize("action", ["context", "claim", "complete"])
def test_native_request_uses_canonical_route_and_original_identity(action):
    async def run():
        seen = []
        cap = capability()
        def handler(req):
            seen.append(req)
            body = json.loads(req.content)
            assert body["action_id"] == "original-id"
            assert body["scope"] == SCOPE
            assert body["action"] == action
            assert body["payload"]["handoff_id"] == "work"
            if action == "claim":
                assert body["payload"]["idempotency_key"] == "original-key"
            assert req.url == "https://nexus.test/v1/runtime/native-actions"
            assert req.headers["authorization"] == "Bearer " + cap.capability
            return httpx.Response(200, json=response(action),
                                  headers={"X-Nexus-Connections-Revision": MANAGEMENT_REVISION})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
            async with NexusHTTPClient("https://nexus.test", client=raw) as http:
                backend = NexusNativeActions(http, cap)
                cap.scope["workspace_id"] = "changed-after-construction"
                assert await backend._invoke(request(action)) == response(action)["result"]
        assert len(seen) == 1
    asyncio.run(run())


@pytest.mark.parametrize("fault", ["redirect", "revision", "oversize", "bad_json", "duplicate",
    "nan", "wrong_id", "wrong_handoff", "wrong_state", "mcp", "server_error", "read_timeout",
    "write_error", "late", "bad_rejection", "compressed"])
@pytest.mark.parametrize("action", ["claim", "context"])
def test_uncertain_response_is_bounded_and_never_retried(fault, action, monkeypatch):
    async def run():
        seen = []
        cap = capability()
        def handler(req):
            seen.append(req)
            headers = {"X-Nexus-Connections-Revision": MANAGEMENT_REVISION}
            data = response(action)
            if fault == "redirect":
                return httpx.Response(307, headers={"Location": "https://other.test/leak"})
            if fault == "read_timeout":
                raise httpx.ReadTimeout("Injected loss.", request=req)
            if fault == "write_error":
                raise httpx.WriteError("Injected loss.", request=req)
            if fault == "revision": headers.clear()
            if fault == "oversize":
                return httpx.Response(200, content=b"x" * 16385, headers=headers)
            if fault in ("bad_json", "duplicate", "nan"):
                raw = {"bad_json": b"{", "duplicate": b'{"a":1,"a":2}', "nan": b'{"a":NaN}'}[fault]
                return httpx.Response(200, content=raw, headers=headers)
            if fault == "wrong_id": data["action_id"] = "other"
            if fault == "wrong_handoff": data["result"]["handoff_id"] = "other"
            if fault == "wrong_state": data["state"] = "other"
            if fault == "mcp": data["result"]["jsonrpc"] = "2.0"
            if fault == "late":
                monkeypatch.setattr("okto_nexus_connector.transport.https_client.time.monotonic",
                                    lambda: cap.deadline_monotonic + 1)
            if fault == "compressed":
                # Claim an encoding on an empty body; no decoding allocation is needed.
                return httpx.Response(200, content=b"", headers=dict(headers, **{"Content-Encoding": "gzip"}))
            if fault in ("server_error", "bad_rejection"):
                return httpx.Response(503 if fault == "server_error" else 403,
                    json={"error": {"code": "PERMISSION_DENIED", "possible_effect": True,
                                    "retry_safe": False}}, headers=headers)
            return httpx.Response(200, json=data, headers=headers)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True) as raw:
            async with NexusHTTPClient("https://nexus.test", client=raw) as http:
                with pytest.raises(CoreError) as error:
                    await NexusNativeActions(http, cap)._invoke(request(action))
                assert error.value.code == ("OUTCOME_UNKNOWN" if action == "claim" else "EXECUTOR_OFFLINE")
                assert error.value.possible_effect is (action == "claim")
                assert error.value.retry_safe is (action == "context")
                assert error.value.operation_id == "original-id"
                assert cap.capability not in str(error.value)
        assert len(seen) == 1
    asyncio.run(run())


@pytest.mark.parametrize("fault", ["expired", "audience", "scope", "actions", "reference", "mcp_url"])
def test_invalid_capability_is_refused_before_transport(fault):
    async def run():
        cap = capability()
        changes = {
            "expired": {"deadline_monotonic": time.monotonic() - 1},
            "audience": {"audience": "nexus-mcp-session"},
            "scope": {"scope": dict(SCOPE, credential_epoch=True)},
            "actions": {"actions": ("handoff.get",)},
            "reference": {"capability_ref": "native-cap:other"},
            "mcp_url": {"mcp_url": "https://nexus.test/mcp"},
        }
        seen = []
        def handler(req):
            seen.append(req)
            raise AssertionError("Invalid capability reached the transport.")
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
            async with NexusHTTPClient("https://nexus.test", client=raw) as http:
                with pytest.raises(ConnectorError):
                    await http.native_action(replace(cap, **changes[fault]), request=request())
        assert not seen
    asyncio.run(run())


@pytest.mark.parametrize("fault", ["connect", "rejected"])
def test_known_no_effect_preserves_original_id(fault):
    async def run():
        seen = []
        def handler(req):
            seen.append(req)
            if fault == "connect":
                raise httpx.ConnectError("Injected connect failure.", request=req)
            return httpx.Response(403, json={"error": {"code": "PERMISSION_DENIED",
                "possible_effect": False, "retry_safe": False}},
                headers={"X-Nexus-Connections-Revision": MANAGEMENT_REVISION})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw:
            async with NexusHTTPClient("https://nexus.test", client=raw) as http:
                with pytest.raises(CoreError) as error:
                    await NexusNativeActions(http, capability())._invoke(request())
                assert error.value.code == ("EXECUTOR_OFFLINE" if fault == "connect" else "PERMISSION_DENIED")
                assert not error.value.possible_effect
                assert error.value.retry_safe is (fault == "connect")
                assert error.value.operation_id == "original-id"
        assert len(seen) == 1
    asyncio.run(run())
