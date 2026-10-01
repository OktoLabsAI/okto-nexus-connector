"""Keep a running native handle when a different approved binding is replaced."""
from dataclasses import replace

import pytest

from okto_nexus_connector.errors import ConnectorError
from tests.unit.test_r4_binding_replacement_lanes import replace_binding
from tests.unit.test_r4_daemon_execution import lifecycle, selection
from tests.unit.test_r4_execution import observed


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_idle_replacement_does_not_stop_another_running_native_session(lifecycle):
    execution, _, store, frame, native, attached, published, _ = lifecycle
    await execution.sync()
    await execution.connection.emit(frame)
    await observed(published, execution.owner)
    assert len(native.opened) == 1 and not native.native.stopped
    owner = execution.owner
    running_lane = execution.lanes["binding"]
    def add_target(state):
        state.execution_bindings.insert(0, replace(state.execution_bindings[0], binding_id="target"))
    store.update(add_target)
    await execution.sync()
    replace_binding(store)
    await execution.sync()
    assert execution.ready and execution.owner is owner and execution.connection.online
    assert execution.lanes["binding"] is running_lane
    assert execution.lanes["target"].binding.binding_revision == running_lane.binding.binding_revision + 1
    assert len(native.opened) == 1 and not native.native.stopped
    assert len(attached) == 2


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_change_during_target_observation_refuses_adoption(lifecycle, monkeypatch):
    execution, _, store, _, _, _, _, _ = lifecycle
    await execution.sync()
    old_lane = execution.lanes["binding"]
    replace_binding(store)
    async def changed(**scope):
        assert scope["binding_id"] == "binding"
        store.update(lambda state: setattr(state.execution_bindings[0], "binding_revision", 99))
        return True
    monkeypatch.setattr(execution.host, "binding_sessions_closed", changed)
    with pytest.raises(ConnectorError, match="RECONCILIATION_REQUIRED"):
        await execution.sync()
    assert execution.lanes["binding"] is old_lane
