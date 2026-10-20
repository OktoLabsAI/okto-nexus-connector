"""Lifecycle with real OS processes: singleton and terminal independence.

TC-07: two concurrent daemon starters yield exactly one owner. TC-09:
closing the CLI that started the daemon leaves daemon and sessions
running. These spawn real detached subprocesses of the daemon module.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import okto_nexus_connector

from okto_nexus_connector.daemon import manager
from okto_nexus_connector.daemon.lock import InstanceLock
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.ipc.client import IPCClient
from okto_nexus_connector.platform import paths

START_TIMEOUT = 60


def _env(root: Path) -> dict[str, str]:
    env = dict(os.environ)
    # Children qualify the same package as their parent. An installed campaign
    # must not silently import the checkout or an inherited user PYTHONPATH.
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONHOME", None)
    env["PYTHONNOUSERSITE"] = "1"
    env["OKTO_NEXUS_CONNECTOR_STATE"] = str(root)
    env["OKTO_NEXUS_CONNECTOR_VAULT"] = "file"
    package_root = Path(okto_nexus_connector.__file__).resolve().parents[1]
    if package_root == Path(__file__).resolve().parents[2] / "src":
        env["PYTHONPATH"] = str(package_root)
    return env


def _spawn_daemon(root: Path) -> subprocess.Popen:
    kwargs: dict[str, object] = {
        "env": _env(root), "cwd": root, "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL, "stdin": subprocess.DEVNULL}
    if sys.platform == "win32":
        kwargs["creationflags"] = (
            getattr(subprocess, "DETACHED_PROCESS", 0x8) |
            getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x200))
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen(
        [sys.executable, "-m", "okto_nexus_connector.daemon"], **kwargs)


async def _wait_live(lock: InstanceLock, timeout: float = START_TIMEOUT):
    def check():
        readiness = lock.live_owner()
        return readiness
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        readiness = check()
        if readiness is not None:
            return readiness
        await asyncio.sleep(0.1)
    raise AssertionError("daemon process did not become ready")


@pytest.mark.process
async def test_concurrent_starts_yield_one_daemon(tmp_path: Path):
    """TC-07: race of two starters → one lock owner, one readiness."""
    root = paths.state_dir(tmp_path)
    lock = InstanceLock(paths.pid_dir(root))
    first = _spawn_daemon(root)
    second = _spawn_daemon(root)
    try:
        readiness = await _wait_live(lock)
        # Either contender may win. Windows venv launchers can publish a child
        # PID, so compare the owned starter handles, not their PID to readiness.
        # This fresh private state directory and authenticated IPC identify the
        # daemon; one starter must remain while its competitor exits.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and all(p.poll() is None for p in (first, second)):
            await asyncio.sleep(0.1)
        assert sum(p.poll() is None for p in (first, second)) == 1
        # exactly one live owner at any moment
        owners = [lock.live_owner()]
        assert all(o is not None for o in owners)
        assert lock.live_owner().pid == readiness.pid
        # daemon answers IPC
        assert manager.status(root).running is True
    finally:
        # Drain the actual daemon through this test's authenticated IPC before
        # cleaning launcher handles. Killing a Windows launcher alone can leave
        # its native child alive; clearing readiness would then hide the leak.
        await asyncio.to_thread(lambda: manager.stop(root, timeout=30.0))
        for process in (first, second):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=20)
        assert lock.live_owner() is None


def test_child_imports_parent_package_instead_of_inherited_shadow(tmp_path, monkeypatch):
    shadow = tmp_path / 'shadow'
    shadow.mkdir()
    (shadow / 'okto_nexus_connector.py').write_text("raise RuntimeError('untrusted shadow package')\n", encoding='utf-8')
    monkeypatch.setenv('PYTHONPATH', str(shadow))
    result = subprocess.run([sys.executable, '-c',
        'import json, okto_nexus_connector; print(json.dumps(okto_nexus_connector.__file__))'],
        cwd=tmp_path, env=_env(tmp_path), capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert Path(json.loads(result.stdout)).resolve() == Path(okto_nexus_connector.__file__).resolve()


@pytest.mark.process
async def test_cli_exit_does_not_stop_daemon(tmp_path: Path):
    """TC-09: the spawning client exits; the daemon keeps running."""
    root = paths.state_dir(tmp_path)
    lock = InstanceLock(paths.pid_dir(root))
    try:
        env = _env(root)
        # Exercise the actual CLI starter, including its independent lifetime.
        # A separate status client exiting is not proof that a daemon survives
        # the process that created it.
        started = await asyncio.to_thread(subprocess.run,
            [sys.executable, "-m", "okto_nexus_connector.cli.main",
             "--state-dir", str(root), "--json", "daemon", "start"],
            env=env, cwd=root, capture_output=True, text=True, timeout=60)
        if os.name == "nt" and started.returncode == 3:
            try:
                refusal = json.loads(started.stdout)["error"]
            except (ValueError, KeyError, TypeError):
                refusal = {}
            if (refusal.get("code") == "DAEMON_UNAVAILABLE" and
                    "Job Object breakaway" in refusal.get("message", "")):
                pytest.skip("The Windows runner prohibits independent Job Object breakaway")
        assert started.returncode == 0, started.stderr or started.stdout
        readiness = await _wait_live(lock)
        pid = readiness.pid
        result = await asyncio.to_thread(subprocess.run,
            [sys.executable, "-m", "okto_nexus_connector.cli.main",
             "--state-dir", str(root), "--json", "daemon", "status"],
            env=env, cwd=root, capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stderr
        assert '"running": true' in result.stdout
        # An unrelated client cannot turn terminal independence into an
        # unauthenticated shutdown capability, even on the local transport.
        def unauthorized_shutdown():
            with IPCClient(readiness.transport, readiness.address, 'invalid-test-token') as client:
                client.call('shutdown')
        with pytest.raises(ConnectorError) as denied:
            await asyncio.to_thread(unauthorized_shutdown)
        assert denied.value.code == 'AGENT_AUTH_REQUIRED'
        # daemon still alive with same identity
        still = lock.live_owner()
        assert still is not None and still.pid == pid
        assert manager.status(root).running is True
    finally:
        await asyncio.to_thread(lambda: manager.stop(root, timeout=30.0))
        assert lock.live_owner() is None


@pytest.mark.process
async def test_daemon_stop_drains_and_exits(tmp_path: Path):
    """daemon stop asks for drain and the process exits (TC-37 shape)."""
    root = paths.state_dir(tmp_path)
    lock = InstanceLock(paths.pid_dir(root))
    process = _spawn_daemon(root)
    try:
        readiness = await _wait_live(lock)
        result = await asyncio.to_thread(lambda: manager.stop(root, timeout=45.0))
        assert result["stopped"] is True
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and process.poll() is None:
            await asyncio.sleep(0.1)
        assert process.poll() is not None
        assert process.returncode in (0, 1)
    finally:
        await asyncio.to_thread(lambda: manager.stop(root, timeout=30.0))
        if process.poll() is None:
            process.kill()
        process.wait(timeout=20)
        assert lock.live_owner() is None
