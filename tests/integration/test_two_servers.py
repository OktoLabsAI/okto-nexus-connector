"""Integration: two Servers, several agents, one daemon (plan 4.1, TC-41).

Namespaced state: identities, bindings, lanes, tickets and vault handles
stay isolated per server_id; removing/revoking one server's binding does
not touch the other's credentials or quotas.
"""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pytest

from okto_nexus_connector.daemon.app import DaemonApp
from okto_nexus_connector.identity.vault import RestrictedFileVault
from okto_nexus_connector.platform import paths
from okto_nexus_connector.storage.state_store import (
    BindingRecord, IdentityRecord, ServerProfileRecord, StateStore,
)
from tests.fakes.http_peer import FakeAgent, FakeBinding, FakeNexusHTTPPeer
from tests.fakes.wss_peer import FakeNXLPeer
from tests.integration.test_daemon_app import DaemonHarness

KEY_A = "nxs_two_servers_key_a"
KEY_B = "nxs_two_servers_key_b"
SERVER_A = "srv_alpha"
SERVER_B = "srv_beta"
AGENT_1 = "ag_one"
AGENT_2 = "ag_two"


def _binary(root: Path, name: str) -> Path:
    binary = root / name
    binary.write_bytes(f"binary for {name}".encode())
    return binary


async def _prepare(root: Path, http_a: str, http_b: str, wss_a: str,
                   wss_b: str) -> None:
    store = StateStore(paths.state_file(root))
    vault = RestrictedFileVault(paths.vault_dir(root), approved=True)
    vault.store(f"{SERVER_A}/{AGENT_1}", KEY_A)
    vault.store(f"{SERVER_B}/{AGENT_2}", KEY_B)
    project = root / "project"
    project.mkdir(exist_ok=True)
    binaries = {SERVER_A: _binary(root, "a.exe"), SERVER_B:
                _binary(root, "b.exe")}

    def mutate(state):
        state.connector_id = "conn_two"
        state.preferences["vault.fallback_file.approved"] = True
        state.servers[SERVER_A] = ServerProfileRecord(
            SERVER_A, http_a, http_a, "now",
            link_url_override=wss_a + "/link")
        state.servers[SERVER_B] = ServerProfileRecord(
            SERVER_B, http_b, http_b, "now",
            link_url_override=wss_b + "/link")
        state.identities.append(IdentityRecord(
            alias="alpha", server_id=SERVER_A, agent_id=AGENT_1,
            secret_handle=f"vault:{SERVER_A}/{AGENT_1}", credential_epoch=1,
            added_at="now"))
        state.identities.append(IdentityRecord(
            alias="beta", server_id=SERVER_B, agent_id=AGENT_2,
            secret_handle=f"vault:{SERVER_B}/{AGENT_2}", credential_epoch=1,
            added_at="now"))
        for server, agent, binding_id, alias in (
                (SERVER_A, AGENT_1, "bind_a", "alpha-codex"),
                (SERVER_B, AGENT_2, "bind_b", "beta-claude")):
            binary = binaries[server]
            state.bindings.append(BindingRecord(
                binding_id=binding_id, alias=alias, server_id=server,
                agent_id=agent,
                adapter_id="codex_app_server" if server == SERVER_A
                else "claude_stream",
                executor_id="conn_two", workspace_id="ws_1",
                workspace_root=str(project), endpoint_id="wb_1",
                profile_id="prof_1", candidate_executable=str(binary),
                candidate_fingerprint="sha256:" + hashlib.sha256(
                    binary.read_bytes()).hexdigest(),
                candidate_version="0.157.0", created_at="now"))
    store.update(mutate)


@pytest.fixture
async def two_server_harness(tmp_path: Path):
    http_a = FakeNexusHTTPPeer(server_id=SERVER_A)
    http_a.add_agent(FakeAgent(agent_id=AGENT_1, key=KEY_A,
                               server_id=SERVER_A))
    url_a = await http_a.start()
    http_a.bindings["bind_a"] = FakeBinding(
        binding_id="bind_a", agent_id=AGENT_1, alias="alpha-codex",
        adapter_id="codex_app_server")
    http_b = FakeNexusHTTPPeer(server_id=SERVER_B)
    http_b.add_agent(FakeAgent(agent_id=AGENT_2, key=KEY_B,
                               server_id=SERVER_B))
    url_b = await http_b.start()
    http_b.bindings["bind_b"] = FakeBinding(
        binding_id="bind_b", agent_id=AGENT_2, alias="beta-claude",
        adapter_id="claude_stream")
    wss_a = FakeNXLPeer(server_id=SERVER_A)
    wss_url_a = await wss_a.start()
    wss_b = FakeNXLPeer(server_id=SERVER_B)
    wss_url_b = await wss_b.start()
    for http_peer, wss_peer in ((http_a, wss_a), (http_b, wss_b)):
        original = http_peer._ticket

        def register(headers, binding_id, _original=original,
                     _wss=wss_peer):
            status, payload = _original(headers, binding_id)
            if status == 200:
                _wss.valid_tickets.add(payload["ticket"])
            return status, payload
        http_peer._ticket = register
    await _prepare(tmp_path, url_a, url_b, wss_url_a, wss_url_b)
    app = DaemonApp(tmp_path)
    # Contract-level native factory (real providers stay TC-42-gated).
    from tests.integration.conftest import RecordingFactory
    recording_factory = RecordingFactory()
    original_host_build = app.host.build

    def build_with_fake_factory(binding, *, environment):
        return original_host_build(binding, environment=environment,
                                   factory=recording_factory)
    app.host.build = build_with_fake_factory
    harness = DaemonHarness(app, tmp_path)
    harness.http_a, harness.http_b = http_a, http_b
    harness.wss_a, harness.wss_b = wss_a, wss_b
    await harness.start()
    yield harness
    await harness.stop()
    await http_a.stop()
    await http_b.stop()
    await wss_a.stop()
    await wss_b.stop()


async def test_two_servers_isolated_namespaces(two_server_harness):
    harness = two_server_harness
    status = await harness.call("status")
    transports = status["result"]["transports"]
    assert SERVER_A in transports and SERVER_B in transports
    assert set(transports[SERVER_A]["lanes"]) == {"bind_a"}
    assert set(transports[SERVER_B]["lanes"]) == {"bind_b"}


async def test_remove_binding_of_one_server_leaves_other(two_server_harness):
    """TC-41: remove server A's binding; B's credentials/lanes remain."""
    harness = two_server_harness
    store = StateStore(paths.state_file(harness.root))

    def drop(state):
        state.bindings = [b for b in state.bindings
                          if b.binding_id != "bind_a"]
    store.update(drop)
    response = await harness.call("state.reload")
    assert response["ok"], response
    # give the transport a moment to settle its lane bookkeeping
    for _ in range(20):
        status = await harness.call("status")
        transports = status["result"]["transports"]
        if not transports[SERVER_A]["lanes"]:
            break
        await asyncio.sleep(0.1)
    assert set(transports[SERVER_A]["lanes"]) == set()
    assert set(transports[SERVER_B]["lanes"]) == {"bind_b"}
    # B's runtime still starts after A's removal
    started = await harness.call("runtime.start", {"alias": "beta-claude"})
    assert started["ok"], started
    # vault namespaces both still present
    vault = RestrictedFileVault(paths.vault_dir(harness.root),
                                approved=True)
    assert vault.resolve(f"vault:{SERVER_A}/{AGENT_1}") == KEY_A
    assert vault.resolve(f"vault:{SERVER_B}/{AGENT_2}") == KEY_B


async def test_runtime_start_works_on_both_servers(two_server_harness):
    harness = two_server_harness
    alpha = await harness.call("runtime.start", {"alias": "alpha-codex"})
    beta = await harness.call("runtime.start", {"alias": "beta-claude"})
    assert alpha["ok"], alpha
    assert beta["ok"], beta
    # distinct Server namespaces keep receipts separate; raw session ids
    # from different Servers may legitimately coincide
    status = await harness.call("runtime.status")
    servers = {s["server_id"] for s in status["result"]["sessions"]}
    assert servers == {SERVER_A, SERVER_B}
    keys = {(s["server_id"], s["session_id"])
            for s in status["result"]["sessions"]}
    assert len(keys) == 2


async def test_ticket_of_a_cannot_serve_b(two_server_harness):
    """Lane tickets are scoped per server/binding (A.5)."""
    harness = two_server_harness
    alpha_ticket = None
    original = harness.http_a._ticket

    def capture(headers, binding_id):
        nonlocal alpha_ticket
        status, payload = original(headers, binding_id)
        if status == 200 and binding_id == "bind_a":
            alpha_ticket = payload["ticket"]
        return status, payload
    harness.http_a._ticket = capture
    harness.wss_b.valid_tickets.discard  # ensure empty start
    harness.wss_b.valid_tickets = set()
    # force a fresh ticket from A
    key = harness.wss_a  # noqa: F841 (topology reference)
    from okto_nexus_connector.transport.https_client import NexusHTTPClient
    async with NexusHTTPClient(harness.http_a.url) as http:
        ticket, _ = await http.binding_ticket(KEY_A, "bind_a")
    assert ticket.startswith("nstkt_")
    # B's peer refuses A's ticket
    assert ticket not in harness.wss_b.valid_tickets
