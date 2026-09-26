"""Integration: HITL approval surface — authority stays server-side (TC-29).

The daemon queues server-sent approval requests; the CLI forwards the
operator's decision through the Server's CAS mechanism. The agent's own
key only transports the decision; a CAS conflict (already answered) is
refused rather than applied to another request.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okto_nexus_connector.daemon.app import DaemonApp
from okto_nexus_connector.identity.vault import RestrictedFileVault
from okto_nexus_connector.platform import paths
from okto_nexus_connector.storage.state_store import (
    IdentityRecord, StateStore,
)
from tests.fakes.http_peer import FakeAgent, FakeNexusHTTPPeer
from tests.fakes.wss_peer import FakeNXLPeer
from tests.integration.test_daemon_app import DaemonHarness

KEY = "nxs_approval_key"
SERVER_ID = "srv_appr"
AGENT = "ag_appr"


@pytest.fixture
async def approval_harness(tmp_path: Path):
    http_peer = FakeNexusHTTPPeer(server_id=SERVER_ID)
    http_peer.add_agent(FakeAgent(agent_id=AGENT, key=KEY,
                                  server_id=SERVER_ID))
    http_url = await http_peer.start()
    wss_peer = FakeNXLPeer(server_id=SERVER_ID)
    wss_url = await wss_peer.start()
    vault = RestrictedFileVault(paths.vault_dir(tmp_path), approved=True)
    vault.store(f"{SERVER_ID}/{AGENT}", KEY)
    store = StateStore(paths.state_file(tmp_path))

    def mutate(state):
        from okto_nexus_connector.storage.state_store import (
            BindingRecord, ServerProfileRecord)
        state.connector_id = "conn_appr"
        state.preferences["vault.fallback_file.approved"] = True
        state.servers[SERVER_ID] = ServerProfileRecord(
            SERVER_ID, http_url, http_url, "now",
            link_url_override=wss_url + "/link")
        state.identities.append(IdentityRecord(
            alias="work", server_id=SERVER_ID, agent_id=AGENT,
            secret_handle=f"vault:{SERVER_ID}/{AGENT}", credential_epoch=1,
            added_at="now"))
        state.bindings.append(BindingRecord(
            binding_id="bind_x", alias="codex", server_id=SERVER_ID,
            agent_id=AGENT, adapter_id="codex_app_server",
            executor_id="conn_appr", workspace_id="ws_1",
            workspace_root=str(tmp_path)))
    store.update(mutate)
    app = DaemonApp(tmp_path)
    harness = DaemonHarness(app, tmp_path)
    harness.http = http_peer
    harness.wss = wss_peer
    await harness.start()
    yield harness
    await harness.stop(timeout=20)
    await http_peer.stop()
    await wss_peer.stop()


async def test_approval_routing_and_cas(approval_harness):
    harness = approval_harness
    peer = harness.wss

    class _Frame:
        pass

    # deliver a server approval request through the transport handler
    frame = {
        "type": "approval.request", "server_id": SERVER_ID,
        "executor_id": "conn_appr", "binding_id": "bind_x",
        "agent_id": AGENT, "session_id": "rs_1",
        "operation_id": "op_appr_1", "request_id": "req_1",
        "kind": "escalation", "proposal": {"summary": "sudo rm"},
    }
    await harness.app._on_remote_approval(frame)
    listing = await harness.call("approvals.pending")
    pending = listing["result"]["approvals"]
    assert [item["request_id"] for item in pending] == ["req_1"]

    # the daemon never decides by itself: no auto-approval exists
    relisting = await harness.call("approvals.pending")
    assert len(relisting["result"]["approvals"]) == 1

    # the Server fake requires the correlated CAS record
    harness.http.add_approval("req_1", agent_id=AGENT)
    decision = await harness.call("approvals.decide", {
        "request_id": "req_1", "decision": "approve",
        "cas_token": "cas-1"})
    assert decision["ok"], decision
    assert decision["result"]["applied"] is True

    # double decision fails the CAS (already answered)
    second = await harness.call("approvals.decide", {
        "request_id": "req_1", "decision": "deny", "cas_token": "cas-1"})
    assert second["ok"] is False
    assert second["error"]["code"] in ("VALIDATION_ERROR",
                                       "OPERATION_CONFLICT")
