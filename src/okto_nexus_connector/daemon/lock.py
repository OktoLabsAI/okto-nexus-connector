"""Interprocess singleton lock with real process identity (plan C02.1).

The lock is an OS-level exclusive byte range (msvcrt/fcntl) held open for
the daemon's whole life. A readiness sidecar records PID plus a process
birth token and the IPC endpoint; reusing an existing instance requires a
live lock, a matching birth token and a successful authenticated IPC ping.
A stale PID file alone is never authority (plan 4.1).
"""

from __future__ import annotations

import json
import errno
import os
import secrets
import stat
import time
from dataclasses import dataclass
from pathlib import Path

from ..errors import ConnectorError
from ..platform.sysinfo import ProcessIdentity, current_process_identity, process_identity, same_process

LOCK_POLL_SECONDS = 0.05


@dataclass(frozen=True, slots=True)
class Readiness:
    pid: int
    birth_token: str
    transport: str  # "unix" | "loopback"
    address: str  # socket path or "127.0.0.1:port"
    started_at: str
    connector_version: str
    instance_id: str = ''
    generation: str = ''
    protocol_revision: int = 1

    def to_json(self) -> dict[str, object]:
        return {
            "pid": self.pid,
            "birth_token": self.birth_token,
            "transport": self.transport,
            "address": self.address,
            "started_at": self.started_at,
            "connector_version": self.connector_version,
            "instance_id": self.instance_id,
            "generation": self.generation,
            "protocol_revision": self.protocol_revision,
        }


class InstanceLock:
    """Exclusive daemon lock for one state directory."""

    def __init__(self, run_dir: Path):
        run_dir = Path(run_dir).resolve()
        self.run_dir = run_dir
        self.lock_path = run_dir / "daemon.lock"
        self.readiness_path = run_dir / "daemon.json"
        self.token_path = run_dir / "ipc-token"
        self._fd: int | None = None

    # -- lock ------------------------------------------------------------

    def acquire(self, *, create: bool = True) -> bool:
        """Try to become the lock owner; False when another owner exists."""
        if self.held:
            raise RuntimeError('This lock is already held')
        if create:
            self.run_dir.mkdir(parents=True, exist_ok=True)
        flags = (os.O_CREAT if create else 0) | os.O_RDWR | getattr(os, "O_BINARY", 0) | getattr(os, 'O_NOFOLLOW', 0)
        fd = os.open(self.lock_path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise OSError('Daemon lock must be a regular non-hardlinked file')
            if os.name == "nt":
                import msvcrt
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            os.close(fd)
            if error.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                return False
            raise
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd
        return True

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            if os.name == "nt":
                import msvcrt
                os.lseek(self._fd, 0, os.SEEK_SET)
                msvcrt.locking(self._fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fd, fcntl.LOCK_UN)
        except OSError:
            pass
        finally:
            os.close(self._fd)
            self._fd = None

    @property
    def held(self) -> bool:
        return self._fd is not None

    def locked(self) -> bool | None:
        """Read-only ownership probe. Inaccessible is unknown, never free."""
        if self.held:
            return True
        probe = InstanceLock(self.run_dir)
        try:
            if not probe.acquire(create=False):
                return True
        except FileNotFoundError:
            return False
        except OSError:
            return None
        else:
            probe.release()
            return False

    def owner_identity(self) -> ProcessIdentity | None:
        """Identity of the process holding the lock, if observable."""
        try:
            raw = self.readiness_path.read_bytes()
        except (FileNotFoundError, NotADirectoryError, PermissionError):
            return None
        try:
            payload = json.loads(raw.decode("utf-8"))
            pid = int(payload["pid"])
        except (ValueError, KeyError, TypeError, UnicodeError):
            return None
        return process_identity(pid)

    # -- readiness ---------------------------------------------------------

    def write_readiness(self, readiness: Readiness) -> None:
        blob = json.dumps(readiness.to_json(), indent=1).encode("utf-8")
        _atomic_write(self.readiness_path, blob)

    def read_readiness(self) -> Readiness | None:
        try:
            raw = self.readiness_path.read_bytes()
        except (FileNotFoundError, NotADirectoryError, PermissionError):
            return None
        try:
            payload = json.loads(raw.decode("utf-8"))
            return Readiness(
                pid=int(payload["pid"]),
                birth_token=str(payload["birth_token"]),
                transport=str(payload["transport"]),
                address=str(payload["address"]),
                started_at=str(payload.get("started_at", "")),
                connector_version=str(payload.get("connector_version", "")),
                instance_id=str(payload.get('instance_id', '')),
                generation=str(payload.get('generation', '')),
                protocol_revision=int(payload.get('protocol_revision', 0)),
            )
        except (ValueError, KeyError, TypeError, UnicodeError):
            return None

    def clear_readiness(self) -> None:
        try:
            self.readiness_path.unlink()
        except FileNotFoundError:
            pass

    def live_owner(self) -> Readiness | None:
        """Readiness proven to belong to a live process with same birth."""
        readiness = self.read_readiness()
        if readiness is None or self.locked() is not True:
            return None
        identity = ProcessIdentity(readiness.pid, readiness.birth_token)
        return readiness if same_process(identity) else None

    # -- IPC token -----------------------------------------------------------

    def ensure_token(self) -> str:
        try:
            token = self.token_path.read_text(encoding="utf-8").strip()
            if len(token) >= 32:
                return token
        except (FileNotFoundError, NotADirectoryError, PermissionError):
            pass
        token = secrets.token_urlsafe(32)
        _atomic_write(self.token_path, token.encode("ascii"))
        if os.name != "nt":
            os.chmod(self.token_path, 0o600)
        return token

    def load_token(self) -> str:
        try:
            token = self.token_path.read_text(encoding="utf-8").strip()
        except (FileNotFoundError, NotADirectoryError, PermissionError) as exc:
            raise ConnectorError("DAEMON_UNAVAILABLE", "ipc_token",
                                 f"cannot read IPC token: {exc}",
                                 action="Start the daemon with "
                                        "'okto-nexus-connector daemon start'."
                                 ) from exc
        if len(token) < 32:
            raise ConnectorError("DAEMON_UNAVAILABLE", "ipc_token",
                                 "IPC token file is invalid")
        return token


def _atomic_write(path: Path, data: bytes) -> None:
    import tempfile
    fd, temp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp-")
    try:
        if os.name != "nt":
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        temp = None
    finally:
        if temp is not None:
            try:
                os.unlink(temp)
            except OSError:
                pass


def wait_for_readiness(lock: InstanceLock, *, timeout: float = 30.0
                       ) -> Readiness:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        readiness = lock.live_owner()
        if readiness is not None:
            return readiness
        time.sleep(0.1)
    raise ConnectorError("DAEMON_UNAVAILABLE", "start",
                         "daemon did not become ready in time",
                         action="Run 'okto-nexus-connector daemon run' in "
                                "the foreground to see diagnostics.")


def current_identity() -> ProcessIdentity:
    return current_process_identity()
