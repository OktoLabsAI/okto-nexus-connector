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


def test_reader_waits_for_concurrent_writer_snapshot(tmp_path):
    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    import threading
    path = tmp_path / "state.json"
    writer, reader = StateStore(path), StateStore(path)
    writer.save(ConnectorState(connector_id="old"))
    entered, release, reading = threading.Event(), threading.Event(), threading.Event()
    def change(state):
        entered.set()
        assert release.wait(5)
        state.connector_id = "new"
    def read():
        reading.set()
        return reader.load()
    with ThreadPoolExecutor(max_workers=2) as pool:
        writing = pool.submit(writer.update, change)
        assert entered.wait(5)
        observing = pool.submit(read)
        try:
            assert reading.wait(5)
            with pytest.raises(TimeoutError):
                observing.result(timeout=.2)
        finally:
            release.set()
        writing.result(timeout=5)
        assert observing.result(timeout=5).connector_id == "new"


def test_readers_and_writers_share_cross_instance_lock(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    path = tmp_path / "state.json"
    StateStore(path).save(ConnectorState())
    def write():
        for _ in range(40):
            StateStore(path).update(lambda state: state.preferences.update(
                counter=state.preferences.get("counter", 0) + 1))
    def read():
        previous = 0
        for _ in range(80):
            count = StateStore(path).load().preferences.get("counter", 0)
            assert count >= previous
            previous = count
    with ThreadPoolExecutor(max_workers=4) as pool:
        tasks = [pool.submit(fn) for fn in (write, read, write, read)]
        for task in tasks:
            task.result(timeout=15)
    assert StateStore(path).load().preferences["counter"] == 80


def test_oversized_state_is_refused_by_read_and_update(tmp_path):
    path = tmp_path / "state.json"
    path.write_bytes(b" " * (4 * 1024 * 1024 + 1))
    store = StateStore(path)
    for operation in (store.load, lambda: store.update(lambda _: None)):
        with pytest.raises(ConnectorError, match="CAPACITY_EXCEEDED"):
            operation()
