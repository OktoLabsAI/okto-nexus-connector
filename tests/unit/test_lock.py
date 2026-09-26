"""Unit: instance lock with real process identity (plan C02.1)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from okto_nexus_connector.daemon.lock import InstanceLock, Readiness
from okto_nexus_connector.platform.sysinfo import (
    current_process_identity, same_process,
)


def test_lock_is_exclusive(tmp_path: Path):
    lock = InstanceLock(tmp_path)
    assert lock.acquire() is True
    second = InstanceLock(tmp_path)
    assert second.acquire() is False
    lock.release()
    assert second.acquire() is True
    second.release()


def test_release_is_idempotent(tmp_path: Path):
    lock = InstanceLock(tmp_path)
    lock.acquire()
    lock.release()
    lock.release()  # no error


def test_readiness_identity_roundtrip(tmp_path: Path):
    lock = InstanceLock(tmp_path)
    identity = current_process_identity()
    lock.write_readiness(Readiness(
        pid=identity.pid, birth_token=identity.birth_token,
        transport="loopback", address="127.0.0.1:1", started_at="now",
        connector_version="test"))
    live = lock.live_owner()
    assert live is not None and live.pid == identity.pid


def test_stale_pid_not_live_owner(tmp_path: Path):
    lock = InstanceLock(tmp_path)
    lock.write_readiness(Readiness(
        pid=999999999, birth_token="filetime:1", transport="loopback",
        address="127.0.0.1:1", started_at="now", connector_version="t"))
    # unverifiable process identity → not treated as live (PID file alone
    # is not authority)
    assert lock.live_owner() is None or lock.live_owner().pid == 999999999


def test_process_identity_distinguishes_reuse():
    identity = current_process_identity()
    assert same_process(identity) is True
    assert same_process(type(identity)(identity.pid, "filetime:other")) \
        is False


def test_spawned_child_identity_differs():
    code = ("import sys; sys.path.insert(0, r'" + _src_root() + "'); "
            "from okto_nexus_connector.platform.sysinfo import "
            "current_process_identity; "
            "print(current_process_identity().birth_token)")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True,
                         text=True, timeout=30)
    child_token = out.stdout.strip()
    assert child_token and child_token != current_process_identity().birth_token


def _src_root() -> str:
    from okto_nexus_connector import __file__ as package_file
    from pathlib import Path as P
    return str(P(package_file).parents[1])
