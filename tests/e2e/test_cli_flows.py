"""End-to-end CLI flows against a real daemon subprocess and fake peers.

The CLI runs as a real OS process (piped stdin for protected credential
entry); the daemon is spawned detached by ``daemon start`` exactly as in
production. The fake binary is intentionally not a qualified provider
build: the runtime-start failure path must produce the honest typed
diagnostic, never a silent success (TC-12/TC-28 + honest limits).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from okto_nexus_connector.daemon.lock import InstanceLock
from okto_nexus_connector.platform import paths

from tests.fakes.http_peer import FakeAgent, FakeBinding, FakeNexusHTTPPeer

KEY = "nxs_e2e_key"
SERVER_ID = "srv_fake"
AGENT = "ag_e2e"

SRC = Path(__file__).parents[2] / "src"


def _env(root: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["OKTO_NEXUS_CONNECTOR_STATE"] = str(root)
    env["OKTO_NEXUS_CONNECTOR_VAULT"] = "file"
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    return env


async def run_cli(root: Path, *args: str, stdin: str = "") -> subprocess.CompletedProcess:
    """Run the real CLI process without blocking the test event loop
    (the fake peers serve on that loop)."""
    command = [sys.executable, "-m", "okto_nexus_connector.cli.main",
               "--state-dir", str(root), *args]
    process = await asyncio.create_subprocess_exec(
        *command,
        env=_env(root), stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await asyncio.wait_for(
        process.communicate(stdin.encode()), timeout=120)
    return subprocess.CompletedProcess(
        args=command, returncode=process.returncode,
        stdout=stdout, stderr=stderr)


@pytest.fixture
async def fake_server():
    peer = FakeNexusHTTPPeer(server_id=SERVER_ID)
    peer.add_agent(FakeAgent(agent_id=AGENT, key=KEY, server_id=SERVER_ID))
    url = await peer.start()
    yield peer, url
    await peer.stop()


async def test_cli_identity_connect_and_daemon(tmp_path: Path, fake_server):
    peer, url = fake_server
    root = paths.state_dir(tmp_path)
    project = root / "project"
    project.mkdir(parents=True, exist_ok=True)
    binary = root / "fake-codex.exe"
    binary.write_bytes(b"e2e synthetic binary")
    binary.chmod(binary.stat().st_mode | 0o100)

    # 1. identity add via protected stdin entry (with vault approval)
    result = await run_cli(root, "--json", "identity", "add", "--server", url,
                           "--agent", AGENT, "--alias", "work",
                           "--credential-stdin",
                           stdin=f"y\n{KEY}\n")
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["agent_id"] == AGENT and payload["created"] is True

    # idempotent re-import (same key, same alias)
    result = await run_cli(root, "--json", "identity", "add", "--server", url,
                           "--agent", AGENT, "--alias", "work",
                           "--credential-stdin", stdin=f"{KEY}\n")
    assert result.returncode == 0
    assert json.loads(result.stdout)["created"] is False

    # wrong hint aborts (TC-04)
    result = await run_cli(root, "--json", "identity", "add", "--server", url,
                           "--agent", "ag_other", "--alias", "other",
                           "--credential-stdin", stdin=f"{KEY}\n")
    assert result.returncode != 0
    error = json.loads(result.stdout)["error"]
    assert error["code"] == "AGENT_ID_MISMATCH"
    assert KEY.encode() not in result.stderr  # secret never echoed

    # 2. identity list/show
    result = await run_cli(root, "--json", "identity", "list")
    listing = json.loads(result.stdout)
    assert [i["alias"] for i in listing["identities"]] == ["work"]
    result = await run_cli(root, "--json", "identity", "show", "work")
    assert json.loads(result.stdout)["identity"]["agent_id"] == AGENT

    # 3. connect: discovery selection via --executable, confirmations piped
    #    (vault approval already recorded by 'identity add')
    result = await run_cli(
        root, "--json", "connect", "--server", url, "--agent", AGENT,
        "--alias", "work", "--harness", "codex_app_server",
        "--executable", str(binary), "--credential-stdin",
        "--project", str(project),
        stdin=f"{KEY}\ny\n")
    assert result.returncode == 0, result.stderr
    connected = json.loads(result.stdout)
    assert connected["connected"] is True
    binding = connected["binding"]
    assert binding["alias"] == "codex"
    assert binding["binding_id"].startswith("bind_")

    # the daemon was ensured and is reusable (TC-12: second use)
    result = await run_cli(root, "--json", "daemon", "status")
    status = json.loads(result.stdout)
    assert status["running"] is True

    # repeating connect is idempotent (no duplicate binding)
    result = await run_cli(
        root, "--json", "connect", "--server", url, "--agent", AGENT,
        "--alias", "work", "--harness", "codex_app_server",
        "--executable", str(binary), "--credential-stdin",
        "--project", str(project), stdin=f"{KEY}\ny\n")
    assert result.returncode == 0
    again = json.loads(result.stdout)
    assert again["binding"]["binding_id"] == binding["binding_id"]
    assert again["created"] is False

    # 4. bind list/show
    result = await run_cli(root, "--json", "bind", "list")
    bindings = json.loads(result.stdout)["bindings"]
    assert [b["alias"] for b in bindings] == ["codex"]

    # 5. runtime start with an unqualified binary fails honestly
    result = await run_cli(root, "--json", "runtime", "start", "codex",
                           "--project", str(project))
    assert result.returncode != 0
    error = json.loads(result.stdout)["error"]
    assert error["code"] in ("NATIVE_VERSION_UNQUALIFIED", "UNKNOWN",
                             "PROFILE_DRIFT", "NEEDS_REDISCOVERY")

    # runtime status lists zero sessions; logs of unknown session is typed
    result = await run_cli(root, "--json", "runtime", "status")
    assert json.loads(result.stdout)["sessions"] == []
    result = await run_cli(root, "--json", "runtime", "logs", "rs_nope")
    assert result.returncode != 0
    assert b"VALIDATION_ERROR" in result.stdout

    # 6. doctor runs layered diagnostics without side effects
    result = await run_cli(root, "--json", "doctor")
    assert result.returncode == 0
    checks = json.loads(result.stdout)["checks"]
    layers = {c["layer"] for c in checks}
    assert {"environment", "core", "vault", "daemon"} <= layers

    # 7. daemon stop drains and exits
    result = await run_cli(root, "--json", "daemon", "stop")
    stopped = json.loads(result.stdout)
    assert stopped["stopped"] is True
    deadline = time.monotonic() + 30
    lock = InstanceLock(paths.pid_dir(root))
    while time.monotonic() < deadline and lock.live_owner() is not None:
        await asyncio.sleep(0.2)
    assert lock.live_owner() is None


async def test_cli_mcp_config_plan_and_apply(tmp_path: Path, fake_server):
    peer, url = fake_server
    root = paths.state_dir(tmp_path)
    result = await run_cli(root, "--json", "mcp-config", "plan", "--server",
                           url + "/mcp",
                           "--harness", "claude_stream",
                           "--capability-ref", "mcp-cap:rs_1")
    assert result.returncode == 0, result.stderr
    plan = json.loads(result.stdout)
    assert plan["transport"].startswith("direct-http")
    assert plan["requires_daemon"] is False

    # Pi gets the unsupported-transport diagnostic, no stdio fallback
    result = await run_cli(root, "--json", "mcp-config", "plan", "--server",
                           url + "/mcp",
                           "--harness", "pi_rpc",
                           "--capability-ref", "mcp-cap:rs_2")
    assert result.returncode != 0
    assert b"CAPABILITY_UNSUPPORTED" in result.stdout

    # apply composes with third-party entries (backup + ownership)
    target = root / "claude-settings.json"
    target.write_text(json.dumps({
        "mcpServers": {"other": {"type": "http",
                                 "url": "https://elsewhere/route"}}}))
    result = await run_cli(root, "--json", "mcp-config", "apply", "--server",
                           url + "/mcp",
                           "--harness", "claude_stream",
                           "--capability-ref", "mcp-cap:rs_1",
                           "--file", str(target), stdin="y\n")
    assert result.returncode == 0, result.stderr
    document = json.loads(target.read_text())
    assert document["mcpServers"]["other"]["url"] == \
        "https://elsewhere/route"
    assert document["mcpServers"]["nexus"]["url"] == url + "/v1/..." \
        or document["mcpServers"]["nexus"]["url"].startswith(
            url)  # direct to the Server

    # removal keeps third parties
    result = await run_cli(root, "--json", "mcp-config", "remove",
                           "--harness", "claude_stream", "--file", str(target))
    assert result.returncode == 0
    document = json.loads(target.read_text())
    assert "nexus" not in document["mcpServers"]
    assert "other" in document["mcpServers"]


async def test_cli_discover_and_doctor_fresh(tmp_path: Path):
    root = paths.state_dir(tmp_path)
    result = await run_cli(root, "--json", "discover")
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert "candidates" in payload
    result = await run_cli(root, "--json", "doctor")
    assert result.returncode == 0
    summary = json.loads(result.stdout)["summary"]
    assert summary["version"]
