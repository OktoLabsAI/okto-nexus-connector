"""Remote launch respects a registered adapter's explicit tool contract."""
import sys

import pytest
from nexus_connector_core import ControlTargeting
from nexus_connector_core.native import registry
from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_daemon_execution import lifecycle
from tests.unit.test_r4_execution import observed, operation


@pytest.fixture(autouse=True)
def registered_adapter(monkeypatch):
    adapter = 'fixture.additional.v1'
    monkeypatch.setitem(registry._SPECS, adapter, registry.AdapterSpec(
        adapter, adapter, __name__, 'UnusedNativeFactory', 'managed', None,
        frozenset({sys.platform}),
        (ControlTargeting('turn.steer', False, 'forbidden', False),
         ControlTargeting('turn.interrupt', False, 'forbidden', False)),
        managed_contract=1, transport_binding_contract=1))


@pytest.mark.parametrize('selection', ['registered'], indirect=True)
async def test_registered_remote_launch(lifecycle):
    owner, _, store, frame, native, attached, published, keys = lifecycle
    from okto_nexus_connector.platform.paths import state_dir
    state_dir(owner.host.root)
    await owner.sync()
    assert owner.ready and len(attached) == 1
    await owner.connection.emit(frame)
    receipt = await observed(published, owner.owner)
    assert receipt['stage'] == 'SUBMITTED'
    assert len(native.opened) == 1
    assert native.opened[0].intent.adapter_id == 'fixture.additional.v1'
    assert keys == ['agent-key']  # Binding ticket only; no unused MCP secret.
    assert not store.load().session_capabilities
    turn = operation(frame, 'turn.submit', {'text': 'registered remote message'})
    await owner.connection.emit(turn)
    receipt = await observed(published, owner.owner)
    assert receipt['operation_id'] == turn['operation_id'] and receipt['stage'] == 'SUBMITTED'
    assert native.native.sent == [('send_turn', {'text': 'registered remote message'}, turn['operation_id'], None)]
