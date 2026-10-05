"""Contract: identity import flow against the fake peer (TC-03/TC-04)."""

from __future__ import annotations

from pathlib import Path

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.identity.import_flow import (
    import_identity, read_secret_from_mcp_entry, remove_identity,
    replace_credential,
)
from okto_nexus_connector.identity.vault import RestrictedFileVault
from okto_nexus_connector.storage.state_store import StateStore
from okto_nexus_connector.transport.https_client import NexusHTTPClient
from tests.fakes.http_peer import FakeAgent, FakeNexusHTTPPeer

KEY_A = "nxs_key_agent_a"


@pytest.fixture
async def peer():
    server = FakeNexusHTTPPeer()
    server.add_agent(FakeAgent(agent_id="ag_111", key=KEY_A))
    url = await server.start()
    yield server, url
    await server.stop()


@pytest.fixture
def env(tmp_path: Path):
    store = StateStore(tmp_path / "state.json")
    vault = RestrictedFileVault(tmp_path / "vault", approved=True)
    (tmp_path / "vault").mkdir()
    return store, vault


async def test_import_creates_namespaced_identity(peer, env):
    server, url = peer
    store, vault = env
    async with NexusHTTPClient(url) as http:
        result = await import_identity(
            http, vault, store, key=KEY_A, alias="work",
            agent_hint="ag_111")
    assert result.created is True
    assert result.identity.agent_id == "ag_111"
    assert result.identity.server_id == "srv_fake"
    assert result.identity.secret_handle == "vault:srv_fake/ag_111"
    # server-side /me recorded nothing new: canonical agent unchanged
    state = store.load()
    assert len(state.identities) == 1
    assert state.connector_id.startswith("conn_")


async def test_reimport_same_key_reuses_identity(peer, env):
    """TC-03: canonical key reused without rotation or duplication."""
    server, url = peer
    store, vault = env
    async with NexusHTTPClient(url) as http:
        first = await import_identity(http, vault, store, key=KEY_A,
                                      alias="work", agent_hint="ag_111")
        second = await import_identity(http, vault, store, key=KEY_A,
                                       alias="work", agent_hint="ag_111")
    assert second.created is False
    assert second.identity.agent_id == first.identity.agent_id
    assert len(store.load().identities) == 1


async def test_hint_mismatch_aborts_without_changes(peer, env):
    """TC-04: key A with hint B aborts; no identity swap, no echo."""
    server, url = peer
    store, vault = env
    async with NexusHTTPClient(url) as http:
        with pytest.raises(ConnectorError) as error:
            await import_identity(http, vault, store, key=KEY_A,
                                  alias="work", agent_hint="ag_999")
    assert error.value.code == "AGENT_ID_MISMATCH"
    assert store.load().identities == []
    assert vault.list_namespaces() == []
    assert KEY_A not in str(error.value)


async def test_alias_conflict_between_identities(peer, env):
    server, url = peer
    store, vault = env
    server.add_agent(FakeAgent(agent_id="ag_222", key="nxs_key_b"))
    async with NexusHTTPClient(url) as http:
        await import_identity(http, vault, store, key=KEY_A, alias="work",
                              agent_hint="ag_111")
        with pytest.raises(ConnectorError) as error:
            await import_identity(http, vault, store, key="nxs_key_b",
                                  alias="work", agent_hint="ag_222")
    assert error.value.code == "AMBIGUOUS_BINDING"


async def test_replace_credential_rotates_epoch(peer, env):
    server, url = peer
    store, vault = env
    async with NexusHTTPClient(url) as http:
        await import_identity(http, vault, store, key=KEY_A, alias="work",
                              agent_hint="ag_111")
    server.agents.pop(KEY_A)
    server.add_agent(FakeAgent(agent_id="ag_111", key="nxs_new_key", credential_epoch=7))
    async with NexusHTTPClient(url) as http:
        result = await replace_credential(http, vault, store, alias="work",
                                          key="nxs_new_key")
    assert result.identity.credential_epoch == 7
    assert vault.resolve("vault:srv_fake/ag_111") == "nxs_new_key"


async def test_remove_is_local_not_central(peer, env):
    server, url = peer
    store, vault = env
    async with NexusHTTPClient(url) as http:
        await import_identity(http, vault, store, key=KEY_A, alias="work",
                              agent_hint="ag_111")
    result = remove_identity(vault, store, alias="work",
                             revoke_globally_hint=False)
    assert result["revoked_globally"] is False
    assert KEY_A in server.agents  # canonical key still valid server-side
    assert store.load().identities == []
    assert vault.list_namespaces() == []


async def test_mcp_entry_import_requires_matching_origin(tmp_path: Path):
    """Explicitly selected MCP entry: origin must match; nothing scanned."""
    config = tmp_path / "mcp.json"
    config.write_text(
        '{"mcpServers": {"nexus": {"type": "http", '
        '"url": "https://nexus.example/mcp", '
        '"headers": {"Authorization": "Bearer nxs_from_mcp_entry"}}, '
        '"other": {"type": "http", "url": "https://elsewhere/route"}}}')
    key = read_secret_from_mcp_entry(
        config, entry_name="nexus", server_origin="https://nexus.example")
    assert key == "nxs_from_mcp_entry"
    with pytest.raises(ConnectorError) as error:
        read_secret_from_mcp_entry(
            config, entry_name="other",
            server_origin="https://nexus.example")
    assert error.value.code == "PROFILE_DRIFT"
    with pytest.raises(ConnectorError):
        read_secret_from_mcp_entry(
            config, entry_name="missing",
            server_origin="https://nexus.example")


async def test_mcp_entry_env_ref_resolution(tmp_path: Path):
    import os
    config = tmp_path / "mcp.json"
    config.write_text(
        '{"mcpServers": {"nexus": {"type": "http", '
        '"url": "https://nexus.example/mcp", '
        '"bearer_token_env_var": "TEST_NEXUS_KEY_VAR"}}}')
    os.environ["TEST_NEXUS_KEY_VAR"] = "nxs_env_resolved"
    try:
        key = read_secret_from_mcp_entry(
            config, entry_name="nexus",
            server_origin="https://nexus.example")
        assert key == "nxs_env_resolved"
    finally:
        del os.environ["TEST_NEXUS_KEY_VAR"]
