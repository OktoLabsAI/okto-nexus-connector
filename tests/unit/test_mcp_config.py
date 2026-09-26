"""Unit: direct-HTTP MCP client configuration (plan C06, TC-24/TC-25)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services.mcp_config_service import (
    apply_persistent_entry, plan_direct_entry, remove_persistent_entry,
    tools_only_status,
)


def test_codex_plan_is_direct_http_only():
    plan = plan_direct_entry(
        adapter_id="codex_app_server",
        server_url="https://nexus.example/mcp",
        capability_ref="mcp-cap:rs_1", approved_origins={
            "https://nexus.example"})
    assert plan.server_url == "https://nexus.example/mcp"
    assert plan.preview.startswith("[mcp_servers.")
    assert "url" in plan.preview
    assert "command" not in plan.preview  # no stdio/command entry ever
    assert tools_only_status(plan)["requires_daemon"] is False


def test_claude_plan_uses_http_type_entry():
    plan = plan_direct_entry(
        adapter_id="claude_stream",
        server_url="https://nexus.example/mcp",
        capability_ref="mcp-cap:rs_2", approved_origins={
            "https://nexus.example"})
    entry = json.loads(plan.preview)["mcpServers"]["nexus"]
    assert entry["type"] == "http"
    assert entry["url"].startswith("https://nexus.example")
    assert "Bearer ${" in entry["headers"]["Authorization"]


def test_pi_has_no_mcp_http_client_diagnostic():
    """TC-25: stdio-only harness gets a clear diagnostic, no fallback."""
    with pytest.raises(ConnectorError) as error:
        plan_direct_entry(
            adapter_id="pi_rpc", server_url="https://nexus.example/mcp",
            capability_ref="mcp-cap:rs_3", approved_origins={
                "https://nexus.example"})
    assert error.value.code == "CAPABILITY_UNSUPPORTED"
    assert "no stdio fallback" in (error.value.action or "")


def test_unapproved_origin_refused():
    with pytest.raises(ConnectorError) as error:
        plan_direct_entry(
            adapter_id="claude_stream",
            server_url="https://rogue.example/mcp",
            capability_ref="mcp-cap:rs_4",
            approved_origins={"https://nexus.example"})
    assert error.value.code == "BINDING_NOT_AUTHORIZED"


def test_capability_ref_shape_required():
    with pytest.raises(ConnectorError):
        plan_direct_entry(
            adapter_id="claude_stream",
            server_url="https://nexus.example/mcp",
            capability_ref="not-a-cap-ref", approved_origins={
                "https://nexus.example"})


def test_apply_preserves_third_party_entries(tmp_path: Path):
    """TC-13/TC-39: apply/remove touch only the owned entry."""
    target = tmp_path / "claude.json"
    target.write_text(json.dumps({
        "mcpServers": {
            "other": {"type": "http", "url": "https://elsewhere/route"},
            "nexus": {"type": "http", "url": "https://old/mcp",
                      "headers": {"Authorization": "Bearer ${OLD}"}}}}))
    plan = plan_direct_entry(
        adapter_id="claude_stream",
        server_url="https://127.0.0.1:9/mcp",
        capability_ref="mcp-cap:rs_9",
        approved_origins={"https://127.0.0.1:9"}, harness_is_local=True,
        loopback_reachable=True)
    owned_before = {"type": "http", "url": "https://old/mcp",
                    "headers": {"Authorization": "Bearer ${OLD}"}}
    result = apply_persistent_entry(plan, target,
                                    previously_owned=owned_before)
    assert result["changed"] is True
    document = json.loads(target.read_text())
    assert document["mcpServers"]["other"]["url"] == "https://elsewhere/route"
    assert document["mcpServers"]["nexus"]["url"] == "https://127.0.0.1:9/mcp"
    backup = Path(result["backup_path"])
    assert backup.exists()
    # idempotent re-apply with the recorded owned entry
    again = apply_persistent_entry(
        plan, target, previously_owned=result["owned_entry"])
    document2 = json.loads(target.read_text())
    assert document2["mcpServers"]["nexus"]["url"] == "https://127.0.0.1:9/mcp"
    # removal keeps third parties
    removed = remove_persistent_entry(
        "claude_stream", target, entry_name="nexus",
        previously_owned=result["owned_entry"])
    assert removed["changed"] is True
    document3 = json.loads(target.read_text())
    assert "nexus" not in document3["mcpServers"]
    assert document3["mcpServers"]["other"]["url"] == "https://elsewhere/route"


def test_apply_refuses_foreign_entry_without_ownership(tmp_path: Path):
    """An entry we never wrote fails closed instead of being claimed."""
    target = tmp_path / "claude.json"
    target.write_text(json.dumps({
        "mcpServers": {"nexus": {"type": "http",
                                 "url": "https://foreign/mcp"}}}))
    plan = plan_direct_entry(
        adapter_id="claude_stream",
        server_url="https://127.0.0.1:9/mcp",
        capability_ref="mcp-cap:rs_10",
        approved_origins={"https://127.0.0.1:9"}, harness_is_local=True,
        loopback_reachable=True)
    with pytest.raises(ConnectorError):
        apply_persistent_entry(plan, target, previously_owned=None)
