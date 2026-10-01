"""Automatic approved lanes, per-agent credentials and retained execution."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace
import time

import pytest

from okto_nexus_connector.daemon.r4_execution import R4DaemonExecution
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.identity.vault import RestrictedFileVault
from okto_nexus_connector.services.core_host import CoreRuntimeHost, ExecutionRuntimeKey
from okto_nexus_connector.services.execution_selection import acknowledge_execution_binding
from okto_nexus_connector.storage.state_store import ServerProfileRecord, IdentityRecord
from okto_nexus_connector.transport.https_client import R4BindingTicket, R4SessionCapability
from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_execution import Connection, Factory, observed


@pytest.fixture
async def lifecycle(selection, tmp_path):
    store, candidate, binding, frame, *_ = selection
    acknowledge_execution_binding(store, binding=binding)
    vault = RestrictedFileVault(tmp_path, approved=True)
    handle = vault.store("agent", "agent-key")
    vault.store("provider-demo", "provider-secret")
    def seed(state):
        state.servers["srv"] = ServerProfileRecord("srv", "https://nexus.test", "https://nexus.test", "now")
        state.identities.append(IdentityRecord("agent", "srv", "agent", handle, 1, "now"))
    store.update(seed)
    connection = Connection(frame)
    attached, published, keys = [], asyncio.Queue(), []
    async def attach(**kwargs):
        attached.append(kwargs)
    connection.attach_binding = attach
    connection.is_attached = lambda **scope: any(all(row.get(k)==v for k,v in scope.items()) for row in attached)
    host = CoreRuntimeHost(tmp_path / "runtime", vault)
    async def discover():
        return [candidate]
    control = SimpleNamespace(store=store, vault=vault, host=host, connection=connection,
        server_id="srv", executor_id="exe", discover=discover, cleanup_pending=False)
    class HTTP:
        origin = base_url = "https://nexus.test"
        after_ticket = None
        async def request_r4_binding_ticket(self, key, **kwargs):
            keys.append(key)
            if self.after_ticket:
                self.after_ticket()
            binding = next(r for r in store.load().execution_bindings
                           if r.server_id == "srv" and r.binding_id == kwargs["binding_id"])
            return R4BindingTicket("ept_lane", "nxt4_lane", "exe", kwargs["binding_id"], binding.agent_id,
                600, 1, 2, tuple(kwargs["scopes"]))
        async def request_r4_session_capability(self, key, *, frame, **kwargs):
            keys.append(key)
            scope = {k: frame[k] for k in (
                "server_id", "executor_id", "binding_id", "agent_id", "workspace_id",
                "workspace_binding_id", "session_id", "session_owner_generation",
                "authorization_revision", "configuration_revision", "binding_revision", "credential_epoch")}
            return R4SessionCapability("cap", "mcp-cap:cap", "nxc4_" + "x" * 40,
                scope, kwargs["audience"], tuple(kwargs["actions"]), 60,
                time.monotonic() + 60, self.base_url + "/mcp")
        async def publish_operation_receipt(self, ticket, *, frame):
            assert ticket == "nxt4_lane"
            await published.put(frame)
    http, native = HTTP(), Factory()
    lifecycle = R4DaemonExecution(control, http, native_factory=native)
    try:
        yield lifecycle, http, store, frame, native, attached, published, keys
    finally:
        await lifecycle.close()
        await host.shutdown_all()


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_automatic_lane_and_default_launch_publish_one_receipt(lifecycle):
    owner, _, store, frame, native, attached, published, keys = lifecycle
    await owner.sync()
    assert owner.ready and len(attached) == 1
    await owner.sync()
    assert len(attached) == 1
    await owner.connection.emit(frame)
    receipt = await observed(published, owner.owner)
    assert receipt["operation_id"] == frame["operation_id"]
    assert len(native.opened) == 1
    assert keys == ["agent-key", "agent-key"]
    assert len(store.load().session_capabilities) == 1
    raw = store.path.read_text()
    assert "agent-key" not in raw and "nxt4_lane" not in raw and "nxc4_" not in raw


@pytest.mark.parametrize("selection", [True], indirect=True)
@pytest.mark.parametrize("fault", ["revoke", "identity", "origin"])
async def test_change_during_ticket_reply_never_attaches_or_opens(lifecycle, fault):
    owner, http, store, _, native, attached, _, _ = lifecycle
    def mutate():
        def alter(state):
            if fault == "revoke":
                state.execution_bindings[0].state = "REVOKED"
            elif fault == "identity":
                state.identities[0].credential_epoch += 1
            else:
                state.servers["srv"].base_url = state.servers["srv"].origin = "https://other.test"
        store.update(alter)
    http.after_ticket = mutate
    with pytest.raises(ConnectorError):
        await owner.sync()
    assert not attached and not native.opened and owner.owner is None


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_revoked_binding_and_wrong_connection_refuse_publication(lifecycle):
    owner, _, store, frame, _, _, published, _ = lifecycle
    await owner.sync()
    with pytest.raises(ConnectorError, match="SCOPE_MISMATCH"):
        await owner._current(dict(frame, server_id="other"))
    store.update(lambda s: setattr(s.execution_bindings[0], "state", "REVOKED"))
    with pytest.raises(ConnectorError, match="RECONCILIATION_REQUIRED"):
        await owner.sync()
    with pytest.raises(ConnectorError):
        await owner._publish(frame)
    assert published.empty()


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_executor_shutdown_preserves_other_runtime_owner(lifecycle):
    owner, _, _, frame, _, _, published, _ = lifecycle
    await owner.sync()
    await owner.connection.emit(frame)
    await observed(published, owner.owner)
    class OtherRuntime:
        async def shutdown(self, policy):
            raise AssertionError("A different executor was stopped.")
    key = ExecutionRuntimeKey("other", "executor", "binding", "session")
    other = OtherRuntime()
    owner.host._runtimes[key] = other
    try:
        await owner.close()
        assert owner.host._runtimes == {key: other}
        assert owner.host._journal is not None
    finally:
        owner.host._runtimes.pop(key)


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_cancelled_cleanup_observer_keeps_pending_owner_alive(lifecycle, monkeypatch):
    owner, _, _, _, _, _, _, _ = lifecycle
    await owner.sync()
    entered, release = asyncio.Event(), asyncio.Event()
    original = owner.host.shutdown_executor
    async def held(**kwargs):
        entered.set()
        await release.wait()
        return await original(**kwargs)
    monkeypatch.setattr(owner.host, "shutdown_executor", held)
    closing = asyncio.create_task(owner.close())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert not owner._close_task.done()
        release.set()
        await owner.close()
        assert owner._close_task.done()
    finally:
        release.set()


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_each_lane_uses_its_agent_identity_without_cross_server_fallback(lifecycle):
    owner, _, store, _, _, attached, _, keys = lifecycle
    second = owner.vault.store("second-agent", "second-agent-key")
    def add(state):
        state.identities.append(IdentityRecord("second", "srv", "other", second, 1, "now"))
        state.execution_bindings.append(replace(state.execution_bindings[0],
                                                binding_id="second", agent_id="other"))
        state.execution_bindings.append(replace(state.execution_bindings[0], server_id="foreign"))
    store.update(add)
    await owner.sync()
    assert keys == ["agent-key", "second-agent-key"]
    assert [(row["binding_id"], row["agent_id"]) for row in attached] == [
        ("binding", "agent"), ("second", "other")]

@pytest.mark.parametrize("selection", [True], indirect=True)
@pytest.mark.parametrize("finish", ["expiry", "explicit_stop", "inspection_failure"])
async def test_disconnected_cleanup_preserves_the_installed_core_lease(lifecycle, monkeypatch, finish):
    from nexus_connector_core import R4_PREVIEW_REVISION, SessionKey

    owner, _, _, frame, native, _, published, _ = lifecycle
    await owner.sync()
    async def lease(runtime, *, scope, grant_id):
        attempt = await runtime.begin_r4_lease_request(scope=scope, grant_id=grant_id,
            connection_id=owner.connection.state.connection_id,
            connection_generation=owner.connection.state.connection_generation, purpose="initial")
        return await runtime.install_r4_lease(attempt, dict(protocol_major=1,
            contract_revision=R4_PREVIEW_REVISION, type="lease.granted",
            request_id=attempt.request_id, grant_id=grant_id, scope=scope,
            lease_id="lease", lease_serial=1, valid_for_ms=1500,
            allowed_actions=["runtime.open", "turn.submit", "runtime.close"]))
    monkeypatch.setattr(owner.connection, "apply_lease", lease)
    await owner.connection.emit(frame)
    await observed(published, owner.owner)
    runtime = next(iter(owner.host._runtimes.values()))
    session = SessionKey(frame["server_id"], frame["executor_id"], frame["session_id"])
    assert (await runtime.inspect(session)).lease_state == "ACTIVE"
    inspected = asyncio.Event()
    inspect = runtime.inspect
    attempts = []
    async def inspection(key):
        attempts.append(key)
        inspected.set()
        if finish == "inspection_failure" and len(attempts) == 1:
            raise OSError("Injected ownership observation failure.")
        return await inspect(key)
    monkeypatch.setattr(runtime, "inspect", inspection)
    stop = asyncio.Event()
    closing = asyncio.create_task(owner.close(preserve_leases=True, stop_event=stop))
    try:
        await asyncio.wait_for(inspected.wait(), 3)
        assert not native.native.stopped
        assert owner.control.cleanup_pending and not closing.done()
        assert not owner.connection.online
        assert owner.host._journal is not None and len(native.opened) == 1
        if finish == "explicit_stop":
            closing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await closing
            assert not owner._close_task.done()
            stop.set()
        await asyncio.wait_for(owner.close(), 5)
        assert native.native.stopped and not owner.control.cleanup_pending
        assert len(native.opened) == 1 and not native.native.sent
        if finish == "inspection_failure":
            assert len(attempts) > 1
    finally:
        stop.set()
        await owner.close()


async def test_lease_wait_observes_only_the_selected_executor(tmp_path):
    from nexus_connector_core import RuntimeSnapshot

    host = CoreRuntimeHost(tmp_path, None)
    class Selected:
        async def inspect(self, session):
            assert session.server_id == "selected"
            return RuntimeSnapshot(session.session_id, "RUNNING", "IDLE", "owned", 0, "EXPIRED")
    class Foreign:
        async def inspect(self, session):
            raise AssertionError("An unrelated executor was inspected.")
    selected = ExecutionRuntimeKey("selected", "exe", "binding", "session")
    foreign = ExecutionRuntimeKey("foreign", "exe", "binding", "session")
    host._runtimes.update({selected: Selected(), foreign: Foreign()})
    await host.wait_executor_leases(server_id="selected", executor_id="exe", stop_event=asyncio.Event())
    assert len(host._runtimes) == 2


async def test_unknown_lease_observation_does_not_authorize_early_cleanup(tmp_path):
    from nexus_connector_core import RuntimeSnapshot
    host = CoreRuntimeHost(tmp_path, None)
    entered = asyncio.Event()
    state = ["UNKNOWN"]
    class Runtime:
        async def inspect(self, session):
            entered.set()
            return RuntimeSnapshot(session.session_id, "UNKNOWN", "UNKNOWN", "unknown", 0, state[0])
    host._runtimes[ExecutionRuntimeKey("srv", "exe", "binding", "session")] = Runtime()
    stop = asyncio.Event()
    waiter = asyncio.create_task(host.wait_executor_leases(server_id="srv", executor_id="exe", stop_event=stop))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert not waiter.done()
        state[0] = "EXPIRED"
        await asyncio.wait_for(waiter, 2)
    finally:
        stop.set()
        await waiter


async def test_failed_inspection_keeps_other_started_inspections_owned(tmp_path):
    host = CoreRuntimeHost(tmp_path, None)
    entered, release = asyncio.Event(), asyncio.Event()
    class Failed:
        async def inspect(self, session):
            raise OSError("Injected failed inspection.")
    class Pending:
        async def inspect(self, session):
            entered.set()
            await release.wait()
            raise OSError("Injected pending inspection completion.")
    host._runtimes[ExecutionRuntimeKey("srv", "exe", "binding", "one")] = Failed()
    host._runtimes[ExecutionRuntimeKey("srv", "exe", "binding", "two")] = Pending()
    waiter = asyncio.create_task(host.wait_executor_leases(
        server_id="srv", executor_id="exe", stop_event=asyncio.Event()))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert not waiter.done()
    finally:
        release.set()
        with pytest.raises(OSError):
            await waiter


@pytest.mark.parametrize("selection", [True], indirect=True)
@pytest.mark.parametrize("drift", [False, True])
async def test_default_execution_discovery_uses_persisted_scope_and_revalidates(lifecycle, monkeypatch, drift):
    from okto_nexus_connector.daemon import r4_execution
    owner, _, _, frame, native, _, published, _ = lifecycle
    candidates = await owner.control.discover()
    owner.control.discover = None
    configuration = {"approved_scope": "fixture"}
    snapshot = (SimpleNamespace(discovery_configuration=configuration), None, None)
    owner.control._snapshot = lambda: snapshot
    calls = []
    async def discover(value):
        assert value == configuration
        calls.append("discover")
        return candidates
    async def require(value):
        assert value is snapshot
        calls.append("revalidate")
        if drift:
            raise ConnectorError("PROFILE_DRIFT", "discovery", "The approved discovery scope changed.")
    owner.control._require = require
    monkeypatch.setattr(r4_execution, "configured_candidates", discover)
    await owner.sync()
    if drift:
        with pytest.raises(ConnectorError, match="PROFILE_DRIFT"):
            await owner._candidates(frame)
        assert not native.opened and published.empty()
    else:
        await owner.connection.emit(frame)
        receipt = await observed(published, owner.owner)
        assert receipt["operation_id"] == frame["operation_id"]
        assert len(native.opened) == 1
    assert calls and calls[0:2] == ["discover", "revalidate"]
