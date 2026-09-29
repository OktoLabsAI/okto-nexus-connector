"""Local R4 realization staging retains physical evidence only on the executor."""

from pathlib import Path

import pytest

from nexus_connector_core import InstallationCandidate, calculate_inventory_revision
from nexus_connector_core.discovery import fingerprint, installation_ref

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services.realization_service import (
    acknowledge_local_realization, publication_body,
    stage_local_realization,
)
from okto_nexus_connector.storage.state_store import StateStore
from okto_nexus_connector.transport.https_client import R4Realization


def test_local_realization_replay_and_drift_are_effect_free(tmp_path):
    root = tmp_path / "local-project"
    root.mkdir()
    binary = tmp_path / "codex.exe"
    binary.write_bytes(b"selected binary")
    candidate = InstallationCandidate(
        "codex_app_server", str(binary), fingerprint(binary),
        "explicit", "selected",
        installation_ref=installation_ref("codex_app_server", str(binary)),
    )
    revision = calculate_inventory_revision([candidate])
    store = StateStore(tmp_path / "connector-state.json")
    args = dict(
        server_id="server", executor_id="executor", agent_id="agent",
        client_intent_id="intent", candidates=[candidate],
        adapter_id="codex_app_server",
        candidate_ref=candidate.installation_ref,
        inventory_revision=revision, workspace_root=root,
        workspace_id=None, workspace_label="Project Alpha",
        configuration_digest="sha256:" + "a" * 64,
        local_consent_id="explicit-consent",
    )
    staged = stage_local_realization(store, **args)
    body = publication_body(staged)
    assert str(root) not in str(body)
    assert str(binary) not in str(body)
    assert staged.status == "LOCAL_VALIDATED"
    assert stage_local_realization(store, **args).local_realization_ref == (
        staged.local_realization_ref)
    assert len(store.load().realizations) == 1
    with pytest.raises(ConnectorError):
        stage_local_realization(store, **{**args,
                                         "configuration_digest": "sha256:" + "b" * 64})

    published = R4Realization(
        server_id="server", executor_id="executor",
        realization_ref="real_canonical",
        local_realization_ref=staged.local_realization_ref,
        realization_revision=1, agent_id="agent",
        workspace_id="ws_canonical", workspace_binding_id="wxb_canonical",
        inventory_revision=revision,
        configuration_digest=args["configuration_digest"],
    )
    acknowledged = acknowledge_local_realization(
        store, record=staged, published=published)
    assert acknowledged.canonical_workspace_id == "ws_canonical"
    assert publication_body(stage_local_realization(store, **args)) == body
    assert acknowledge_local_realization(
        store, record=staged, published=published).realization_ref == (
        "real_canonical")

    binary.write_bytes(b"changed binary")
    with pytest.raises(ConnectorError) as drift:
        stage_local_realization(store, **args)
    assert drift.value.code in {"PROFILE_DRIFT", "STALE_GENERATION"}
