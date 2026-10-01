"""Running tool authority retains its approved selection without reopening."""
from dataclasses import replace
from pathlib import Path
import pytest
from nexus_connector_core import calculate_inventory_revision, r4_submit_intent_hash
from nexus_connector_core.build_identity import executable_build_identity
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services.execution_selection import acknowledge_execution_binding
from okto_nexus_connector.services.launch_configuration import _resolve
from tests.unit.test_execution_selection import selection


@pytest.mark.parametrize("selection", [True], indirect=True)
def test_running_checks_preserve_launch_qualification_boundary(selection, monkeypatch):
    store, candidate, binding, frame, *_ = selection
    candidate = replace(candidate, build_identity=executable_build_identity(candidate.executable))
    revision = calculate_inventory_revision([candidate])
    def update(state):
        state.realizations[0].candidate_build_identity = candidate.build_identity
        state.realizations[0].inventory_revision = revision
    store.update(update)
    binding = replace(binding, inventory_revision=revision)
    acknowledge_execution_binding(store, binding=binding)
    frame["payload"]["inventory_revision"] = revision
    frame["intent_hash"] = r4_submit_intent_hash(frame)
    expected = _resolve(store, frame, [candidate])
    from okto_nexus_connector.services import execution_selection
    def unavailable(*args):
        raise AssertionError("Full qualification is only needed for a new launch.")
    monkeypatch.setattr(execution_selection, "executable_build_identity", unavailable)
    assert _resolve(store, frame, [candidate], running=expected) == expected
    with pytest.raises(AssertionError, match="new launch"):
        _resolve(store, frame, [candidate])


@pytest.mark.parametrize("selection", [True], indirect=True)
@pytest.mark.parametrize("change", ["binding", "realization", "configuration", "secret",
                                    "workspace", "home", "binary", "session"])
def test_running_checks_reject_changed_scope_and_physical_resources(selection, change):
    store, candidate, binding, frame, *_ = selection
    acknowledge_execution_binding(store, binding=binding)
    expected = _resolve(store, frame, [candidate])
    if change in ("workspace", "home"):
        directory = store.path.parent / ("workspace" if change == "workspace" else "provider-home")
        assert directory.resolve().parent == store.path.parent.resolve()
        directory.rename(directory.with_name(directory.name + "-previous"))
        directory.mkdir()
    elif change == "binary":
        Path(candidate.executable).write_bytes(b"Replaced executable")
    elif change == "session":
        frame = {**frame, "session_id": "other-session"}
        frame["intent_hash"] = r4_submit_intent_hash(frame)
    else:
        def mutate(state):
            if change == "binding":
                state.execution_bindings[0].authorization_revision += 1
            elif change == "realization":
                state.realizations[0].local_consent_id = "another-consent"
            elif change == "configuration":
                state.launch_configurations[0].profile_revision += 1
            else:
                state.launch_configurations[0].secret_bindings["OPENAI_API_KEY"] = "vault:other"
        store.update(mutate)
    with pytest.raises(ConnectorError):
        _resolve(store, frame, [candidate], running=expected)
