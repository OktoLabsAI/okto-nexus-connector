"""Reviewed idle binding replacement must preserve other execution lanes."""
from dataclasses import asdict, replace

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.storage.state_store import BindingIntentRecord
from tests.unit.test_r4_daemon_execution import lifecycle, selection


def replace_binding(store, *, fault=None):
    def change(state):
        old = state.execution_bindings[0]
        current = replace(old, binding_revision=old.binding_revision + 1,
            realization_ref="replacement", realization_revision=1,
            workspace_binding_id="replacement-workspace",
            realization_snapshot_digest="replacement-digest")
        if fault == "authority":
            current.authorization_revision += 1
        if fault == "identity":
            state.identities[0].credential_epoch += 1
        proof = BindingIntentRecord(old.server_id, old.executor_id, old.agent_id,
            "agent", "replacement-intent", "assistant", current.realization_ref,
            "scope", status="APPLIED", binding_id=old.binding_id,
            replace_binding_id=old.binding_id, replacement_snapshot=asdict(old),
            applied_binding={k: v for k, v in asdict(current).items() if k != "realization_snapshot_digest"})
        if fault == "unconfirmed":
            proof.status = "APPLY_PENDING"
        if fault == "partial_reply":
            proof.applied_binding.pop("workspace_binding_id")
        if fault == "old_snapshot":
            proof.replacement_snapshot["binding_revision"] += 1
        state.binding_intents.append(proof)
        state.execution_bindings[0] = current
    store.update(change)


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_reviewed_idle_replacement_preserves_owner_connection_and_ticket(lifecycle):
    execution, _, store, _, _, attached, _, keys = lifecycle
    await execution.sync()
    owner = execution.owner
    old_lane = execution.lanes["binding"]
    replace_binding(store)
    await execution.sync()
    assert execution.ready and execution.owner is owner
    assert execution.connection.online
    assert execution.lanes["binding"].binding.binding_revision == old_lane.binding.binding_revision + 1
    assert execution.lanes["binding"].ticket is old_lane.ticket
    assert execution.lanes["binding"].deadline == old_lane.deadline
    assert len(attached) == 1 and keys == ["agent-key"]


@pytest.mark.parametrize("selection", [True], indirect=True)
@pytest.mark.parametrize("fault", ["authority", "identity", "unconfirmed", "old_snapshot", "partial_reply", "expired"])
async def test_unreviewed_or_authority_change_does_not_adopt_replacement(lifecycle, fault):
    execution, _, store, _, _, _, _, _ = lifecycle
    await execution.sync()
    old_lane = execution.lanes["binding"]
    replace_binding(store, fault=fault)
    if fault == "expired":
        execution.clock = lambda: old_lane.deadline
    with pytest.raises(ConnectorError, match="RECONCILIATION_REQUIRED"):
        await execution.sync()
    assert execution.lanes["binding"] is old_lane


@pytest.mark.parametrize("selection", [True], indirect=True)
@pytest.mark.parametrize("lease,ownership", [("ACTIVE", "owned"), ("UNKNOWN", "unknown"), ("CLOSED", "released")])
async def test_replacement_observes_only_target_and_requires_released_closed_sessions(lifecycle, lease, ownership):
    from nexus_connector_core import RuntimeSnapshot
    from okto_nexus_connector.services.core_host import ExecutionRuntimeKey
    execution, _, store, _, _, _, _, _ = lifecycle
    await execution.sync()
    old_lane = execution.lanes["binding"]
    class Target:
        async def inspect(self, session):
            assert session.session_id == "target"
            return RuntimeSnapshot("target", "UNKNOWN", "UNKNOWN", ownership, 0, lease)
    class Other:
        async def inspect(self, session):
            raise AssertionError("An unrelated session was inspected.")
        async def shutdown(self, policy):
            raise AssertionError("An unrelated session was shut down.")
    target = ExecutionRuntimeKey("srv", "exe", "binding", "target")
    other = ExecutionRuntimeKey("srv", "exe", "other", "running")
    execution.host._runtimes.update({target: Target(), other: Other()})
    replace_binding(store)
    try:
        if lease == "CLOSED":
            await execution.sync()
            assert execution.ready and execution.lanes["binding"].binding.binding_revision == old_lane.binding.binding_revision + 1
        else:
            with pytest.raises(ConnectorError, match="RECONCILIATION_REQUIRED"):
                await execution.sync()
            assert execution.lanes["binding"] is old_lane
        assert execution.connection.online
        assert set(execution.host._runtimes) == {target, other}
    finally:
        execution.host._runtimes.pop(target)
        execution.host._runtimes.pop(other)
