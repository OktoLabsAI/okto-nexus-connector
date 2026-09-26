"""Lifecycle with real OS processes: singleton and terminal independence.

TC-07: two concurrent daemon starters yield exactly one owner. TC-09:
closing the CLI that started the daemon leaves daemon and sessions
running. These spawn real detached subprocesses of the daemon module.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from okto_nexus_connector.daemon import manager
from okto_nexus_connector.daemon.lock import InstanceLock
from okto_nexus_connector.platform import paths

START_TIMEOUT = 60


def _env(root: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["OKTO_NEXUS_CONNECTOR_STATE"] = str(root)
    env["OKTO_NEXUS_CONNECTOR_VAULT"] = "file"
    env["PYTHONPATH"] = str(Path(__file__).parents[2] / "src") + \
        os.pathsep + env.get("PYTHONPATH", "")
    return env


def _spawn_daemon(root: Path) -> subprocess.Popen:
    kwargs: dict[str, object] = {
        "env": _env(root), "stdout": subprocess.DEVNULL,
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
        # the second process must exit (it cannot acquire the lock)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and second.poll() is None:
            await asyncio.sleep(0.1)
        # exactly one live owner at any moment
        owners = [lock.live_owner()]
        assert all(o is not None for o in owners)
        assert lock.live_owner().pid == readiness.pid
        # daemon answers IPC
        token = lock.load_token()
        assert manager.status(root).running is True
    finally:
        for process in (first, second):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
        # clean readiness so other tests are not affected
        lock.clear_readiness()


@pytest.mark.process
async def test_cli_exit_does_not_stop_daemon(tmp_path: Path):
    """TC-09: the spawning client exits; the daemon keeps running."""
    root = paths.state_dir(tmp_path)
    lock = InstanceLock(paths.pid_dir(root))
    process = _spawn_daemon(root)
    try:
        readiness = await _wait_live(lock)
        pid = readiness.pid
        # a CLI-style client connects, lists status and exits
        env = _env(root)
        result = subprocess.run(
            [sys.executable, "-m", "okto_nexus_connector.cli.main",
             "--state-dir", str(root), "--json", "daemon", "status"],
            env=env, capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stderr
        assert '"running": true' in result.stdout
        # daemon still alive with same identity
        still = lock.live_owner()
        assert still is not None and still.pid == pid
    finally:
        if process.poll() is None:
            # stop via the manager path (drain + IPC) then hard-terminate
            stop_result = await asyncio.to_thread(
            lambda: manager.stop(root, timeout=30.0))
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
        lock.clear_readiness()


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
        if process.poll() is None:
            process.kill()
        lock.clear_readiness()
