"""Connector inventory derives its revision from the shared Core artifact."""

from __future__ import annotations

from nexus_connector_core import InstallationCandidate, verify_executor_inventory_snapshot

from okto_nexus_connector.services.discovery_service import (
    availability_snapshot, executor_inventory_snapshot,
)


def test_local_preview_and_publish_use_same_full_revision(tmp_path):
    candidates = [
        InstallationCandidate(
            adapter_id="codex_app_server", executable=str(tmp_path / "codex"),
            fingerprint="sha256:content", source="path", trust="selected",
            version="0.157.0", architecture="x86_64",
            build_identity="sha256:build",
        )
    ]
    preview = availability_snapshot(candidates)
    published = executor_inventory_snapshot(
        candidates, server_id="server-a", executor_id="executor-a",
        producer_instance_id="producer-a", publication_sequence=1,
    )
    assert preview["executor_revision"] == published["inventory_revision"]
    assert len(preview["executor_revision"]) == 71
    assert str(tmp_path) not in str(published)
    verify_executor_inventory_snapshot(published)
