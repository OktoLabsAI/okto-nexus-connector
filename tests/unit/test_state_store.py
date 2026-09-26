"""Unit: state store atomicity, aliases, schema guard."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.storage.state_store import (
    BindingRecord, ConnectorState, IdentityRecord, ServerProfileRecord,
    StateStore, state_from_json, state_to_json,
)


def test_roundtrip(tmp_path: Path):
    store = StateStore(tmp_path / "state.json")
    def mutate(state: ConnectorState):
        state.connector_id = "conn_1"
        state.servers["srv"] = ServerProfileRecord(
            "srv", "https://nexus.example", "https://nexus.example", "now")
        state.identities.append(IdentityRecord(
            "work", "srv", "ag_1", "vault:srv/ag_1", 1, "now"))
        state.bindings.append(BindingRecord(
            binding_id="bind_1", alias="codex", server_id="srv",
            agent_id="ag_1", adapter_id="codex_app_server",
            executor_id="conn_1", workspace_id="ws_1",
            workspace_root=str(tmp_path)))
    store.update(mutate)
    loaded = store.load()
    assert loaded.connector_id == "conn_1"
    assert loaded.identity_by_alias("work").agent_id == "ag_1"
    assert loaded.binding_by_alias("codex").binding_id == "bind_1"
    # no secrets in the state file
    raw = (tmp_path / "state.json").read_text()
    assert "vault:srv/ag_1" in raw  # handle only (not a secret)


def test_newer_schema_refused(tmp_path: Path):
    payload = {"schema_version": 99}
    (tmp_path / "state.json").write_text(json.dumps(payload))
    with pytest.raises(ConnectorError) as error:
        StateStore(tmp_path / "state.json").load()
    assert error.value.code == "VERSION_INCOMPATIBLE"


def test_duplicate_alias_lookup_is_ambiguous(tmp_path: Path):
    state = ConnectorState()
    record = BindingRecord(
        binding_id="bind_1", alias="codex", server_id="srv",
        agent_id="ag_1", adapter_id="codex_app_server",
        executor_id="conn_1", workspace_id="ws_1", workspace_root="/x")
    state.bindings.append(record)
    duplicate = BindingRecord(
        binding_id="bind_2", alias="codex", server_id="srv2",
        agent_id="ag_2", adapter_id="pi_rpc",
        executor_id="conn_1", workspace_id="ws_1", workspace_root="/y")
    state.bindings.append(duplicate)
    assert state.binding_by_alias("codex") is None  # ambiguous → None


def test_file_permissions_restricted(tmp_path: Path):
    import os
    store = StateStore(tmp_path / "state.json")
    store.update(lambda s: setattr(s, "connector_id", "conn_x"))
    if os.name != "nt":
        mode = (tmp_path / "state.json").stat().st_mode & 0o777
        assert mode == 0o600
