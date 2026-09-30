"""MCP configuration reaches Core launch through approved session references."""
import time
from dataclasses import replace
from pathlib import Path
import json
import tomllib

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.identity.vault import RestrictedFileVault
from okto_nexus_connector.services.session_capabilities import SessionCapabilityOwner, R4ToolServices
from okto_nexus_connector.services.mcp_launch import mcp_template, session_mcp_home, MCP_SESSION_ACTIONS
from okto_nexus_connector.transport.https_client import R4SessionCapability
from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_execution import execution, observed, failed


@pytest.mark.parametrize("selection", [True, "claude"], indirect=True)
@pytest.mark.parametrize("fault", [None, "scope", "origin", "expired"])
async def test_default_owner_mcp_configuration_and_secret_environment(execution, selection, tmp_path, fault):
    owner, connection, factory, receipts, opening = execution
    store, candidate, *_ = selection
    vault = RestrictedFileVault(tmp_path, approved=True)
    vault.store("provider-demo", "provider-test-secret")
    owner.host._vault = vault
    cap_owner = SessionCapabilityOwner(store, vault)
    issued = []
    class HTTP:
        origin = "https://nexus.test"
        async def request_r4_session_capability(self, key, *, frame, **kwargs):
            assert kwargs["audience"] == "nexus-mcp-session"
            scope = {k: frame[k] for k in (
                "server_id", "executor_id", "binding_id", "agent_id", "workspace_id",
                "workspace_binding_id", "session_id", "session_owner_generation",
                "authorization_revision", "configuration_revision", "binding_revision", "credential_epoch")}
            if fault == "scope":
                scope["agent_id"] = "other"
            cap = R4SessionCapability("mcp", "mcp-cap:mcp", "nxc4_" + "m" * 40,
                scope, "nexus-mcp-session", tuple(kwargs["actions"]), 60,
                time.monotonic() + (-1 if fault == "expired" else 60),
                "https://other.test/mcp" if fault == "origin" else self.origin + "/mcp")
            issued.append(cap)
            return cap
    http = HTTP()
    owner.launch_provider = None
    owner.native_tools = R4ToolServices(cap_owner, http, "operator-key")
    try:
        await connection.emit(opening)
        if fault is not None:
            await failed(owner)
            assert not factory.opened and connection.lease_count == 0
            return
        await observed(receipts, owner)
        assert len(factory.opened) == 1 and not owner.host._native_action_owners
        setup = await owner.host.approved_launch(store, frame=opening, candidates=[candidate],
                                                capability=issued[0], http=http)
        env = await setup.environment(factory.opened[0])
        assert env["OPENAI_API_KEY"] == "provider-test-secret"
        home = Path(env["HOME"])
        assert home != tmp_path / "provider-home"
        assert list((tmp_path / "provider-home").iterdir()) == []
        if candidate.adapter_id == "codex_app_server":
            path = home / ".codex/config.toml"
            entry = tomllib.loads(path.read_text())["mcp_servers"]["nexus"]
            bearer = entry["bearer_token_env_var"]
        else:
            path = home / ".claude.json"
            entry = json.loads(path.read_text())["mcpServers"]["nexus"]
            bearer = entry["headers"]["Authorization"][9:-1]
        assert entry["url"] == "https://nexus.test/mcp"
        assert env[bearer] == issued[0].capability
        assert tuple(sorted(factory.opened[0].secret_refs)) == ("mcp-cap:mcp", "vault:provider-demo")
        for path_in_home in home.rglob("*"):
            if path_in_home.is_file():
                assert issued[0].capability not in path_in_home.read_text()
                assert "provider-test-secret" not in path_in_home.read_text()
        original = path.read_bytes()
        path.write_bytes(original + b"changed")
        with pytest.raises(ConnectorError, match="PROFILE_DRIFT"):
            await setup.environment(factory.opened[0])
        assert path.read_bytes() == original + b"changed"
    finally:
        await cap_owner.close()


def test_session_home_owner_reuse_is_exact_and_foreign_content_is_not_overwritten(tmp_path):
    cap = R4SessionCapability("one", "mcp-cap:one", "nxc4_" + "s" * 40,
        {}, "nexus-mcp-session", MCP_SESSION_ACTIONS,
        60, time.monotonic() + 60, "https://nexus.test/mcp")
    template = mcp_template(cap, adapter_id="codex_app_server", approved_origin="https://nexus.test")
    frame = dict(server_id="srv", executor_id="exe", binding_id="binding", session_id="session",
                 session_owner_generation=1)
    kwargs = dict(frame=frame, configuration_digest="sha256:" + "a" * 64, template=template)
    first = session_mcp_home(tmp_path, **kwargs)
    assert session_mcp_home(tmp_path, **kwargs) == first
    second = session_mcp_home(tmp_path, **dict(kwargs, frame=dict(frame, server_id="other")))
    assert first.home != second.home
    marker = first.home / ".owner.json"
    marker.write_text("foreign")
    with pytest.raises(ConnectorError, match="PROFILE_DRIFT"):
        session_mcp_home(tmp_path, **kwargs)
    assert marker.read_text() == "foreign"


@pytest.mark.parametrize("kind", ["home", "config_parent"])
def test_replaced_configuration_directory_is_rejected(tmp_path, kind):
    cap = R4SessionCapability("one", "mcp-cap:one", "nxc4_" + "s" * 40,
        {}, "nexus-mcp-session", MCP_SESSION_ACTIONS, 60,
        time.monotonic() + 60, "https://nexus.test/mcp")
    template = mcp_template(cap, adapter_id="codex_app_server", approved_origin="https://nexus.test")
    frame = dict(server_id="srv", executor_id="exe", binding_id="binding", session_id="session",
                 session_owner_generation=1)
    layout = session_mcp_home(tmp_path, frame=frame, configuration_digest="sha256:" + "a" * 64,
                              template=template)
    target = layout.home if kind == "home" else layout.config.parent
    previous = target.with_name(target.name + "-previous")
    target.rename(previous)
    import shutil
    shutil.copytree(previous, target)
    with pytest.raises(ConnectorError, match="PROFILE_DRIFT"):
        layout.require_current()
