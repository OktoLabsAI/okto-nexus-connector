"""Approved local configuration and default R4 execution composition."""
import asyncio
import threading
from dataclasses import replace

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services.execution_selection import acknowledge_execution_binding
from okto_nexus_connector.services.core_host import CoreRuntimeHost
from okto_nexus_connector.services.launch_configuration import stage_launch_configuration, approved_launch_setup
from okto_nexus_connector.platform.paths import state_dir
from okto_nexus_connector.storage.state_store import StateStore, state_from_json, state_to_json
from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_execution import execution, observed, failed


def stage(store, **overrides):
    args = dict(server_id="srv", executor_id="exe", agent_id="agent",
        local_consent_id="consent", adapter_id="codex_app_server", profile_revision=1,
        secret_bindings={"OPENAI_API_KEY": "vault:provider-demo"})
    args.update(overrides)
    return stage_launch_configuration(store, **args)


def test_staging_persists_only_references_and_is_idempotent(tmp_path):
    store = StateStore(tmp_path / "state.json")
    first = stage(store)
    assert stage(store) == first
    first.secret_bindings["OPENAI_API_KEY"] = "vault:changed"
    assert store.load().launch_configurations[0].secret_bindings == {
        "OPENAI_API_KEY": "vault:provider-demo"}
    assert len(store.load().launch_configurations) == 1
    assert not store.load().execution_bindings
    assert stage(store, profile_revision=2).configuration_digest != first.configuration_digest
    assert len(store.load().launch_configurations) == 2


@pytest.mark.parametrize("bindings", [[], "", 1, [["KEY", "vault:ref"]],
    {"KEY": "vault:"}, {"KEY": "provider: "}, {"KEY": "raw-secret"},
    {"KEY": "vault:nxs_secret"}, {"KEY": "vault:bad\nref"}, {"INVALID-KEY": "vault:ref"}])
def test_invalid_secret_binding_is_rejected_without_persistence(tmp_path, bindings):
    store = StateStore(tmp_path / "state.json")
    with pytest.raises(ConnectorError, match="VALIDATION_ERROR"):
        stage(store, secret_bindings=bindings)
    assert not store.path.exists()


def test_schema_seven_upgrade_preserves_existing_state():
    old = {"schema_version": 7, "connector_id": "existing",
           "preferences": {"keep": True}, "session_capabilities": []}
    state = state_from_json(old)
    assert state.schema_version == 15
    assert state.connector_id == "existing" and state.preferences == {"keep": True}
    assert not state.launch_configurations and not state.session_capabilities
    assert state_to_json(state)["schema_version"] == 15


def test_configuration_capacity_retains_existing_records(tmp_path):
    store = StateStore(tmp_path / "state.json")
    record = stage(store)
    store.update(lambda s: setattr(s, "launch_configurations",
        [replace(record, configuration_digest=str(i)) for i in range(256)]))
    with pytest.raises(ConnectorError, match="CAPACITY_EXCEEDED"):
        stage(store)
    assert len(store.load().launch_configurations) == 256


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_default_owner_uses_approved_references_and_real_core_prepare(execution, selection):
    owner, connection, factory, receipts, opening = execution
    store, candidate, *_ = selection
    owner.launch_provider = None
    await connection.emit(opening)
    await observed(receipts, owner)
    assert len(factory.opened) == 1 and connection.lease_count == 1
    prepared = factory.opened[0]
    assert tuple(prepared.secret_refs) == ("vault:provider-demo",)
    class Vault:
        def resolve(self, reference):
            assert reference == "vault:provider-demo"
            return "provider-test-secret"
    setup = await approved_launch_setup(store, Vault(), frame=opening, candidates=[candidate])
    environment = await setup.environment(prepared)
    assert environment["OPENAI_API_KEY"] == "provider-test-secret"
    assert environment["HOME"] == str(store.path.parent / "provider-home")
    assert "provider-test-secret" not in store.path.read_text()


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_host_approved_environment_qualifies_once_after_materialization(
        execution, selection, tmp_path, monkeypatch):
    from okto_nexus_connector.services import core_host, execution_selection, launch_configuration

    owner, connection, factory, receipts, opening = execution
    store, candidate, *_ = selection
    owner.launch_provider = None
    await connection.emit(opening)
    await observed(receipts, owner)
    prepared = factory.opened[0]

    class Vault:
        def resolve(self, reference):
            assert reference == "vault:provider-demo"
            return "provider-test-secret"

    host = CoreRuntimeHost(state_dir(tmp_path / "qualified-runtime"), Vault())
    captured = []
    original_create = core_host.create_runtime
    def create(**options):
        captured.append(options["environment"])
        return original_create(**options)
    monkeypatch.setattr(core_host, "create_runtime", create)
    full = execution_selection.resolve_execution_selection
    full_calls = []
    def count_full(*args, **kwargs):
        full_calls.append(kwargs["frame"]["session_id"])
        return full(*args, **kwargs)
    monkeypatch.setattr(execution_selection, "resolve_execution_selection", count_full)
    monkeypatch.setattr(launch_configuration, "resolve_execution_selection", count_full)
    try:
        setup = await host.approved_launch(store, frame=opening, candidates=[candidate])
        await host.build_r4(store, frame=opening, candidates=[candidate],
            environment=setup.environment, opening_selection=setup.opening_selection)
        assert full_calls == ["session"]
        environment = await captured.pop()(prepared)
        assert environment["OPENAI_API_KEY"] == "provider-test-secret"
        assert full_calls == ["session", "session"]

        # An arbitrary environment callback retains the host's own full
        # post-callback qualification, including after its possible awaits.
        async def arbitrary(_):
            return {}
        other_host = CoreRuntimeHost(state_dir(tmp_path / "arbitrary-runtime"), Vault())
        try:
            await other_host.build_r4(store, frame=opening,
                candidates=[candidate], environment=arbitrary)
            assert full_calls == ["session"] * 3
            assert await captured.pop()(prepared) == {}
            assert full_calls == ["session"] * 4
        finally:
            await other_host.shutdown_all()
    finally:
        await host.shutdown_all()


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_host_approved_environment_refuses_post_secret_executable_drift(
        execution, selection, tmp_path, monkeypatch):
    from pathlib import Path
    from okto_nexus_connector.services import core_host

    owner, connection, factory, receipts, opening = execution
    store, candidate, *_ = selection
    owner.launch_provider = None
    await connection.emit(opening)
    await observed(receipts, owner)

    class Vault:
        def resolve(self, reference):
            assert reference == "vault:provider-demo"
            Path(candidate.executable).write_bytes(b"Executable changed during secret resolution")
            return "provider-test-secret"

    host = CoreRuntimeHost(state_dir(tmp_path / "drift-runtime"), Vault())
    captured = []
    original_create = core_host.create_runtime
    def create(**options):
        captured.append(options["environment"])
        return original_create(**options)
    monkeypatch.setattr(core_host, "create_runtime", create)
    try:
        setup = await host.approved_launch(store, frame=opening, candidates=[candidate])
        await host.build_r4(store, frame=opening, candidates=[candidate],
            environment=setup.environment, opening_selection=setup.opening_selection)
        with pytest.raises(ConnectorError, match="PROFILE_DRIFT"):
            await captured.pop()(factory.opened[0])
    finally:
        await host.shutdown_all()


async def test_default_owner_missing_configuration_never_opens(execution):
    owner, connection, factory, receipts, opening = execution
    owner.launch_provider = None
    await connection.emit(opening)
    await failed(owner)
    assert owner.failure.code == "BINDING_NOT_AUTHORIZED"
    assert not factory.opened and connection.lease_count == 0 and receipts.empty()


@pytest.mark.parametrize("selection", [True], indirect=True)
@pytest.mark.parametrize("change", ["scope", "profile", "secret", "missing", "duplicate", "home"])
async def test_configuration_drift_refuses_before_lease_or_open(execution, selection, change):
    owner, connection, factory, receipts, opening = execution
    store, *_ = selection
    owner.launch_provider = None
    def mutate(state):
        record = state.launch_configurations[0]
        if change == "scope":
            record.agent_id = "other"
        elif change == "profile":
            record.profile_revision += 1
        elif change == "secret":
            record.secret_bindings["OPENAI_API_KEY"] = "vault:other"
        elif change == "missing":
            state.launch_configurations.clear()
        elif change == "duplicate":
            state.launch_configurations.append(record)
    if change == "home":
        home = store.path.parent / "provider-home"
        home.rename(home.with_name("previous-home"))
        home.mkdir()
    else:
        store.update(mutate)
    await connection.emit(opening)
    await failed(owner)
    assert not factory.opened and connection.lease_count == 0 and receipts.empty()


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_vault_wait_does_not_block_loop_and_rechecks_authority(execution, selection):
    owner, connection, factory, receipts, opening = execution
    store, candidate, *_ = selection
    owner.launch_provider = None
    await connection.emit(opening)
    await observed(receipts, owner)
    entered, release = threading.Event(), threading.Event()
    class Vault:
        def resolve(self, reference):
            entered.set()
            assert release.wait(5)
            return "provider-test-secret"
    setup = await approved_launch_setup(store, Vault(), frame=opening, candidates=[candidate])
    task = asyncio.create_task(setup.environment(factory.opened[0]))
    try:
        async with asyncio.timeout(2):
            while not entered.is_set():
                await asyncio.sleep(0.005)
        store.update(lambda s: setattr(s.execution_bindings[0], "state", "REVOKED"))
        release.set()
        with pytest.raises(ConnectorError):
            await task
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
