"""Connector inventory derives its revision from the shared Core artifact."""

from __future__ import annotations

from dataclasses import replace

import pytest

from nexus_connector_core import (
    InstallationCandidate, installation_ref, verify_executor_inventory_snapshot,
)

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services.discovery_service import (
    availability_snapshot, executor_inventory_snapshot,
    resolve_executor_installation,
)


def test_local_preview_and_publish_use_same_full_revision(tmp_path):
    candidates = [
        InstallationCandidate(
            adapter_id="codex_app_server", executable=str(tmp_path / "codex"),
            fingerprint="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", source="path", trust="selected",
            version="0.157.0", architecture="x86_64",
            build_identity="sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
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


def test_remote_executor_resolves_exact_displayed_installation(tmp_path):
    candidates = [
        InstallationCandidate(
            adapter_id="codex_app_server", executable=str(tmp_path / name),
            fingerprint="sha256:" + "a" * 64, source="path", trust="selected",
            version="0.157.0", architecture="x86_64",
            build_identity="sha256:" + "b" * 64,
        )
        for name in ("copy-a", "copy-b")
    ]
    revision = executor_inventory_snapshot(
        candidates, server_id="srv", executor_id="exe",
        producer_instance_id="producer", publication_sequence=1,
    )["inventory_revision"]
    ref_b = installation_ref("codex_app_server", candidates[1].executable)
    selected = resolve_executor_installation(
        list(reversed(candidates)), adapter_id="codex_app_server",
        candidate_ref=ref_b, expected_inventory_revision=revision)
    assert selected.executable == candidates[1].executable
    with pytest.raises(ConnectorError):
        resolve_executor_installation(
            candidates, adapter_id="codex_app_server",
            candidate_ref=candidates[0].fingerprint,
            expected_inventory_revision=revision)
    with pytest.raises(ConnectorError):
        resolve_executor_installation(
            [replace(candidates[0], fingerprint="sha256:" + "c" * 64),
             candidates[1]], adapter_id="codex_app_server",
            candidate_ref=ref_b, expected_inventory_revision=revision)


@pytest.mark.parametrize("change", [None, "server", "executor", "revision", "freshness"])
async def test_authenticated_inventory_view_checks_scope_and_core_evidence(change):
    import httpx
    from okto_nexus_connector.transport.https_client import NexusHTTPClient, MANAGEMENT_REVISION
    snapshot = executor_inventory_snapshot([], server_id="server", executor_id="executor",
        producer_instance_id="daemon", publication_sequence=1)
    payload = dict(snapshot=snapshot, freshness="OFFLINE", eligible_for_new_start=False)
    if change == "server": snapshot["server_id"] = "other"
    elif change == "executor": snapshot["executor_id"] = "other"
    elif change == "revision": snapshot["inventory_revision"] = "sha256:" + "a" * 64
    elif change == "freshness": payload["freshness"] = "READY"
    def respond(request):
        assert request.method == "GET" and request.headers["Authorization"] == "Bearer technical-key"
        return httpx.Response(200, json=payload, headers={"X-Nexus-Connections-Revision": MANAGEMENT_REVISION})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        async with NexusHTTPClient("https://nexus.test", client=client) as http:
            if change is None:
                view = await http.read_r4_inventory("technical-key", server_id="server", executor_id="executor")
                assert view["freshness"] == "OFFLINE" and view["eligible_for_new_start"] is False
            else:
                with pytest.raises(ConnectorError) as error:
                    await http.read_r4_inventory("technical-key", server_id="server", executor_id="executor")
                assert error.value.code == "VERSION_INCOMPATIBLE"
