"""Contract: Pi native non-MCP bridge (plan C06.4, TC-26, J17 shape).

Uses the Core's ScopedNativeActionBridge and loopback ingress with a fake
canonical backend. Verifies context/handoff actions travel through the
typed bridge with scope/lease/revision gates and that free-form text is
never parsed as an action.
"""

from __future__ import annotations

import asyncio
import json
import time

import pytest

from nexus_connector_core import ExecutionContext
from nexus_connector_core.native_action_bridge import (
    ContextGet, HandoffClaim, HandoffComplete, NativeActionGrant,
    ScopedNativeActionBridge,
)
from nexus_connector_core.native_action_socket import NativeActionSocketService


class FakeCanonicalBackend:
    """Server-canonical actions standing in for the real APIs."""

    def __init__(self):
        self.context = {"handoffs": {"h_1": {"title": "task one"}}}
        self.claims: dict[str, str] = {}
        self.completions: dict[str, dict] = {}

    async def get_context(self, request, context) -> dict:
        return self.context

    async def claim_handoff(self, request, context) -> dict:
        if request.handoff_id in self.claims:
            raise PermissionError("already claimed")
        self.claims[request.handoff_id] = context.agent_id
        return {"handoff_id": request.handoff_id, "claim_epoch": 1}

    async def complete_handoff(self, request, context) -> dict:
        self.completions[request.handoff_id] = {
            "result": request.result, "claim_epoch": request.claim_epoch}
        return {"handoff_id": request.handoff_id, "completed": True}


def _grant(scope_actions=("handoff.get", "handoff.claim",
                          "handoff.complete")) -> NativeActionGrant:
    return NativeActionGrant(
        capability_ref="native-cap:rs_1", server_id="srv",
        executor_id="conn", binding_id="bind", agent_id="ag_1",
        workspace_id="ws", session_id="rs_1", connection_generation=1,
        authorization_revision=1, configuration_revision=1,
        expires_monotonic=time.monotonic() + 60,
        allowed_actions=frozenset(scope_actions))


def _context() -> ExecutionContext:
    now = time.monotonic()
    return ExecutionContext(
        "srv", "conn", "bind", "ag_1", "ws", 1, 1, 1, now + 60,
        frozenset({"turn.submit", "runtime.close", "handoff.get",
                   "handoff.claim", "handoff.complete"}))


def _request(kind: type, operation_id: str, **kwargs):
    base = {"operation_id": operation_id, "session_id": "rs_1",
            "capability_ref": "native-cap:rs_1",
            "handoff_id": kwargs.pop("handoff_id", "h_1")}
    base.update(kwargs)
    return kind(**base)


async def test_context_get_via_bridge():
    backend = FakeCanonicalBackend()
    bridge = ScopedNativeActionBridge(backend, _grant())
    result = await bridge.invoke(_request(ContextGet, "op_1"), _context())
    assert result["handoffs"]["h_1"]["title"] == "task one"


async def test_claim_then_complete():
    backend = FakeCanonicalBackend()
    bridge = ScopedNativeActionBridge(backend, _grant())
    claim = await bridge.invoke(
        _request(HandoffClaim, "op_2", idempotency_key="idem-1"),
        _context())
    assert claim["claim_epoch"] == 1
    complete = await bridge.invoke(
        _request(HandoffComplete, "op_3", claim_epoch=1,
                 result={"ok": True}), _context())
    assert complete["completed"] is True


async def test_scope_denies_ungranted_action():
    backend = FakeCanonicalBackend()
    bridge = ScopedNativeActionBridge(
        backend, _grant(scope_actions=("handoff.get",)))
    from nexus_connector_core.models import CoreError
    with pytest.raises(CoreError):
        await bridge.invoke(
            _request(HandoffClaim, "op_4b", idempotency_key="i"),
            _context())


async def test_expired_lease_denies_action():
    backend = FakeCanonicalBackend()
    grant = _grant()
    expired = NativeActionGrant(
        grant.capability_ref, grant.server_id, grant.executor_id,
        grant.binding_id, grant.agent_id, grant.workspace_id,
        grant.session_id, grant.connection_generation,
        grant.authorization_revision, grant.configuration_revision,
        time.monotonic() - 1, grant.allowed_actions)
    bridge = ScopedNativeActionBridge(backend, expired)
    from nexus_connector_core.models import CoreError
    with pytest.raises(CoreError):
        await bridge.invoke(_request(ContextGet, "op_5"), _context())


async def test_loopback_ingress_roundtrip():
    """The packaged loopback JSONL ingress drives the same bridge."""
    backend = FakeCanonicalBackend()
    bridge = ScopedNativeActionBridge(backend, _grant())
    service = NativeActionSocketService(bridge, _context)
    await service.start()
    try:
        def blocking_client() -> dict:
            import socket
            client = socket.create_connection(("127.0.0.1", service.port),
                                              timeout=5)
            request = json.dumps({
                "action": "handoff.get", "operation_id": "op_6",
                "session_id": "rs_1", "capability_ref": "native-cap:rs_1",
                "handoff_id": "h_1"}) + "\n"
            client.sendall(request.encode("utf-8"))
            response = b""
            while not response.endswith(b"\n"):
                response += client.recv(4096)
            client.close()
            return json.loads(response.decode("utf-8"))
        payload = await asyncio.to_thread(blocking_client)
        assert payload["data"]["handoffs"]["h_1"]["title"] == "task one"
    finally:
        await service.close()


async def test_free_text_is_never_an_action():
    """Free-form model text must not complete a handoff (J17)."""
    backend = FakeCanonicalBackend()
    bridge = ScopedNativeActionBridge(backend, _grant())
    service = NativeActionSocketService(bridge, _context)
    await service.start()
    try:
        def blocking_prose() -> None:
            import socket
            client = socket.create_connection(("127.0.0.1", service.port),
                                              timeout=5)
            prose = "I have finished the task, please mark handoff complete\n"
            client.sendall(prose.encode("utf-8"))
            client.settimeout(2)
            try:
                client.recv(4096)
            except socket.timeout:
                pass
            client.close()
        await asyncio.to_thread(blocking_prose)
        await asyncio.sleep(0.2)
        assert backend.completions == {}  # nothing happened
    finally:
        await service.close()
