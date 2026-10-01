"""Persisted executor discovery through CLI and the automatic daemon."""
import io
from dataclasses import asdict
from types import SimpleNamespace

import pytest

from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.cli.commands.executor import run_executor
from okto_nexus_connector.cli.commands.discover import run_discover
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.platform import paths
from okto_nexus_connector.services import discovery_configuration as service
from okto_nexus_connector.storage.state_store import (
    ConnectorState, ExecutionExecutorRecord, StateStore, state_from_json,
)
from tests.unit.test_r4_daemon_control import control, eventually, dispose


@pytest.fixture
def registered(tmp_path):
    store = StateStore(paths.state_file(tmp_path))
    state = ConnectorState(connector_id="connector")
    for server in ("first", "second"):
        state.execution_executors.append(ExecutionExecutorRecord(server, "connector", "agent",
            "register", "Host", executor_id="executor-" + server, state="REGISTERED"))
    store.save(state)
    return store


async def test_cli_persists_scoped_choice_and_preview_uses_same_public_core(registered, tmp_path, monkeypatch):
    parser = build_parser()
    args = parser.parse_args(["executor", "configure-discovery", "--server-id", "first",
                              "--harness-root", str(tmp_path)])
    output = Output(json_mode=True, stream=io.StringIO())
    result = await run_executor(args, output, tmp_path)
    value = result["discovery_configuration"]
    loaded = StateStore(registered.path).load()
    assert loaded.execution_executors[0].discovery_configuration == value
    assert loaded.execution_executors[1].discovery_configuration == {}
    assert not loaded.execution_bindings and not loaded.launch_configurations
    calls = []
    def discover(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(candidates=())
    monkeypatch.setattr(service, "discover_installations", discover)
    result = await run_discover(parser.parse_args(["discover", "--server-id", "first"]), output, tmp_path)
    assert result["candidates"] == [] and len(calls) == 1
    assert calls[0]["trusted_roots"] == (tmp_path.resolve(),)
    await run_executor(parser.parse_args(["executor", "configure-discovery", "--server-id", "first"]), output, tmp_path)
    assert service.configuration_arguments(registered.load().execution_executors[0].discovery_configuration)["trusted_roots"] == ()


def test_schema_eight_migrates_without_implicit_discovery_authority(registered):
    record = asdict(registered.load().execution_executors[0])
    record.pop("discovery_configuration")
    state = state_from_json(dict(schema_version=8, execution_executors=[record]))
    assert state.schema_version == 12
    assert state.execution_executors[0].discovery_configuration == {}


@pytest.mark.parametrize("kwargs", [
    {"roots": ["relative"]}, {"roots": ["x"] * 33},
    {"pi_node": "missing"}, {"pi_install_root": "missing"},
])
def test_invalid_configuration_leaves_state_unchanged(registered, kwargs):
    before = registered.path.read_bytes()
    with pytest.raises(ConnectorError):
        service.configure_discovery(registered, server_id="first", **kwargs)
    assert registered.path.read_bytes() == before


def test_unregistered_executor_is_refused(registered, tmp_path):
    with pytest.raises(ConnectorError):
        service.configure_discovery(registered, server_id="unknown", roots=[tmp_path])


async def test_pi_pair_and_full_candidate_are_retained(registered, tmp_path, monkeypatch):
    node = tmp_path / "node.exe"
    node.write_bytes(b"not executed")
    value = service.configure_discovery(registered, server_id="first",
        pi_install_root=tmp_path, pi_node=node)
    complete = object()
    def discover(**kwargs):
        assert kwargs == dict(adapter_ids=None, trusted_roots=(tmp_path.resolve(),),
            pi_install_root=tmp_path.resolve(), pi_node=node.resolve())
        return SimpleNamespace(candidates=(complete,))
    monkeypatch.setattr(service, "discover_installations", discover)
    assert (await service.configured_candidates(value))[0] is complete


async def test_replaced_root_is_refused_before_discovery(registered, tmp_path, monkeypatch):
    directory = tmp_path / "approved"
    directory.mkdir()
    value = service.configure_discovery(registered, server_id="first", roots=[directory])
    directory.rename(tmp_path / "retained")
    directory.mkdir()
    monkeypatch.setattr(service, "discover_installations", lambda **_: pytest.fail("Discovery must not run"))
    with pytest.raises(ConnectorError) as error:
        await service.configured_candidates(value)
    assert error.value.code == "PROFILE_DRIFT"


async def test_root_change_during_discovery_discards_candidates(registered, tmp_path, monkeypatch):
    directory = tmp_path / "approved"
    directory.mkdir()
    value = service.configure_discovery(registered, server_id="first", roots=[directory])
    def discover(**_):
        directory.rename(tmp_path / "retained")
        directory.mkdir()
        return SimpleNamespace(candidates=(object(),))
    monkeypatch.setattr(service, "discover_installations", discover)
    with pytest.raises(ConnectorError) as error:
        await service.configured_candidates(value)
    assert error.value.code == "PROFILE_DRIFT"


async def test_automatic_daemon_uses_persisted_configuration(control, tmp_path, monkeypatch):
    owner, peer, store, host, _ = control
    store.update(lambda state: (setattr(state.execution_executors[0], "state", "REGISTERED"),
                                setattr(state.execution_executors[0], "executor_id", "executor")))
    value = service.configure_discovery(store, server_id="srv", roots=[tmp_path])
    owner.discover = None
    observed = []
    def discover(**kwargs):
        observed.append(kwargs)
        return SimpleNamespace(candidates=())
    monkeypatch.setattr(service, "discover_installations", discover)
    owner.start()
    try:
        await eventually(lambda: owner.status()["control_ready"])
        assert observed[0]["trusted_roots"] == (tmp_path.resolve(),)
        assert store.load().execution_executors[0].discovery_configuration == value
        snapshot = owner._snapshot()
        service.configure_discovery(store, server_id="srv")
        with pytest.raises(ConnectorError) as error:
            await owner._require(snapshot)
        assert error.value.code == "STALE_GENERATION"
    finally:
        await dispose(owner, host)


async def test_changed_configuration_during_discovery_is_not_published(control, tmp_path, monkeypatch):
    owner, peer, store, host, _ = control
    owner.discover = None
    def discover(**_):
        service.configure_discovery(store, server_id="srv", roots=[tmp_path])
        return SimpleNamespace(candidates=())
    monkeypatch.setattr(service, "discover_installations", discover)
    try:
        with pytest.raises(ConnectorError) as error:
            await owner._attempt()
        assert error.value.code == "STALE_GENERATION"
        assert not peer.publications
    finally:
        await host.shutdown_all()


async def test_public_core_discovers_real_file_without_launch_or_credential(registered, tmp_path, monkeypatch):
    import os
    import shutil
    import subprocess
    import sys
    from okto_nexus_connector.services.discovery_service import executor_inventory_snapshot
    executable = tmp_path / ("codex.exe" if os.name == "nt" else "codex")
    shutil.copy2(sys.executable, executable)
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: pytest.fail("Discovery must not execute a provider"))
    value = service.configure_discovery(registered, server_id="first", roots=[tmp_path])
    candidates = await service.configured_candidates(value, adapter_ids=("codex_app_server",))
    assert len(candidates) == 1
    selected = candidates[0]
    assert selected.executable == str(executable.resolve()) and selected.fingerprint
    assert selected.installation_ref and selected.architecture
    projection = executor_inventory_snapshot(candidates, server_id="first", executor_id="executor-first",
        producer_instance_id="test", publication_sequence=1)
    assert str(tmp_path) not in str(projection)
    assert not registered.load().execution_bindings


async def test_pi_release_with_explicit_node_outside_release_root(registered, tmp_path, monkeypatch):
    import json
    import os
    import shutil
    import subprocess
    import sys
    node = tmp_path / ("node.exe" if os.name == "nt" else "node")
    shutil.copy2(sys.executable, node)
    releases = tmp_path / "pi"
    package = releases / "releases/1/node_modules/@earendil-works/pi-coding-agent"
    script = package / "dist/bundle/cli.js"
    script.parent.mkdir(parents=True)
    script.write_text("console.log('This file must not execute');", encoding="utf-8")
    (package / "package.json").write_text(json.dumps(dict(
        name="@earendil-works/pi-coding-agent", version="0.87.1")), encoding="utf-8")
    monkeypatch.setenv("PATH", "")
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: pytest.fail("Discovery must not execute Node"))
    value = service.configure_discovery(registered, server_id="first", pi_install_root=releases, pi_node=node)
    arguments = service.configuration_arguments(value)
    assert node in arguments["trusted_roots"] and tmp_path not in arguments["trusted_roots"]
    candidates = await service.configured_candidates(value, adapter_ids=("pi_rpc",))
    assert len(candidates) == 1
    selected = candidates[0]
    assert selected.executable == str(node.resolve())
    assert selected.launch_script == str(script.resolve())
    assert selected.version == "0.87.1" and selected.build_identity and selected.installation_ref
