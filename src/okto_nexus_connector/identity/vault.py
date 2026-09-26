"""Canonical key vault: namespaced refs, OS keyring preferred, restricted
file fallback only under explicit approval (plan C01.1/C01.5).

Handles are opaque references shaped ``vault:<namespace>`` and contain no
secret material. Secrets are never written to the project manifests, the
technical journal, logs, IPC responses or child argv. The restricted-file
backend stores plaintext inside the per-user, 0o600 state directory and
says so honestly; it is used only after the operator recorded explicit
approval in the connector preferences.
"""

from __future__ import annotations

import json
import os
import stat
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Protocol

from ..errors import ConnectorError

_HANDLE_PREFIX = "vault:"


def handle_for(server_id: str, agent_id: str) -> str:
    return f"{_HANDLE_PREFIX}{server_id}/{agent_id}"


def namespace_of(handle: str) -> str:
    if not handle.startswith(_HANDLE_PREFIX):
        raise ConnectorError("VALIDATION_ERROR", "vault_handle",
                             "not a vault handle")
    return handle[len(_HANDLE_PREFIX):]


class SecretVault(Protocol):
    def backend_name(self) -> str: ...
    def store(self, namespace: str, secret: str) -> str: ...
    def resolve(self, handle: str) -> str: ...
    def replace(self, namespace: str, secret: str) -> str: ...
    def remove(self, namespace: str) -> None: ...
    def list_namespaces(self) -> list[str]: ...


@dataclass(slots=True)
class VaultStatus:
    backend: str
    approved_fallback: bool
    namespaces: int
    path: str = ""
    note: str = ""

    def to_json(self) -> dict[str, object]:
        return {
            "backend": self.backend,
            "approved_fallback": self.approved_fallback,
            "namespaces": self.namespaces,
            "path": self.path,
            "note": self.note,
        }


class RestrictedFileVault:
    """Plaintext store restricted by per-user filesystem permissions.

    This is not encryption. On POSIX the file is 0o600 inside the 0o700
    state directory; on Windows it inherits the user-profile ACL. The
    effective protection equals the OS account boundary — the same trust
    domain the daemon runs in — and this is stated to the operator.
    """

    def __init__(self, directory: Path, *, approved: bool):
        self.directory = directory
        self.path = directory / "secrets.json"
        self._approved = approved

    @staticmethod
    def approval_key() -> str:
        return "vault.fallback_file.approved"

    def backend_name(self) -> str:
        return "restricted-file"

    def _require_approval(self) -> None:
        if not self._approved:
            raise ConnectorError(
                "APPROVAL_REQUIRED", "vault",
                "the restricted-file secret fallback was not approved",
                action="Run 'okto-nexus-connector identity add' "
                       "interactively and approve the restricted-file "
                       "fallback, or install an OS keyring backend.")

    def _load(self) -> dict[str, dict[str, object]]:
        try:
            raw = self.path.read_bytes()
        except FileNotFoundError:
            return {}
        if len(raw) > 1024 * 1024:
            raise ConnectorError("CAPACITY_EXCEEDED", "vault")
        return json.loads(raw.decode("utf-8"))

    def _save(self, data: dict[str, dict[str, object]]) -> None:
        blob = json.dumps(data, indent=1, sort_keys=True).encode("utf-8")
        fd, temp = tempfile.mkstemp(dir=str(self.directory),
                                    prefix=".secrets-", suffix=".tmp")
        try:
            if os.name != "nt":
                os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(blob)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, self.path)
            temp = None
        finally:
            if temp is not None:
                try:
                    os.unlink(temp)
                except OSError:
                    pass
        if os.name != "nt":
            mode = stat.S_IMODE(self.path.stat().st_mode)
            if mode != 0o600:
                os.chmod(self.path, 0o600)

    @contextmanager
    def _locked(self) -> Iterator[None]:
        lock_path = self.directory / "secrets.lock"
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_BINARY", 0)
        fd = os.open(lock_path, flags, 0o600)
        try:
            deadline = time.monotonic() + 10
            while True:
                try:
                    if os.name == "nt":
                        import msvcrt
                        os.lseek(fd, 0, os.SEEK_SET)
                        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise ConnectorError("CONFIG_LOCK_BUSY", "vault",
                                             retry_safe=True)
                    time.sleep(0.05)
            try:
                yield
            finally:
                if os.name == "nt":
                    import msvcrt
                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def store(self, namespace: str, secret: str) -> str:
        self._require_approval()
        _validate(namespace, secret)
        with self._locked():
            data = self._load()
            if namespace in data:
                raise ConnectorError("VALIDATION_ERROR", "vault",
                                     "namespace already stored; use replace")
            data[namespace] = {"secret": secret,
                               "updated_at": _now()}
            self._save(data)
        return f"{_HANDLE_PREFIX}{namespace}"

    def resolve(self, handle: str) -> str:
        self._require_approval()
        namespace = namespace_of(handle)
        data = self._load()
        entry = data.get(namespace)
        if entry is None:
            raise ConnectorError("AGENT_AUTH_REQUIRED", "vault",
                                 f"no secret stored for {namespace}",
                                 action="Import the canonical key again.")
        return str(entry["secret"])

    def replace(self, namespace: str, secret: str) -> str:
        self._require_approval()
        _validate(namespace, secret)
        with self._locked():
            data = self._load()
            if namespace not in data:
                raise ConnectorError("VALIDATION_ERROR", "vault",
                                     "namespace not stored; use store")
            data[namespace] = {"secret": secret,
                               "updated_at": _now()}
            self._save(data)
        return f"{_HANDLE_PREFIX}{namespace}"

    def remove(self, namespace: str) -> None:
        with self._locked():
            data = self._load()
            data.pop(namespace, None)
            self._save(data)

    def list_namespaces(self) -> list[str]:
        return sorted(self._load())

    def status(self) -> VaultStatus:
        return VaultStatus(
            "restricted-file", self._approved, len(self._load()),
            str(self.path),
            "Plaintext protected only by the OS account boundary; "
            "not encryption.")


class KeyringVault:
    """OS keyring backend used when the optional ``keyring`` package works."""

    SERVICE = "okto-nexus-connector"

    def __init__(self, keyring_module) -> None:
        self._keyring = keyring_module

    def backend_name(self) -> str:
        return f"keyring:{getattr(self._keyring.get_keyring(), 'name', 'unknown')}"

    def _check(self) -> None:
        try:
            backend = self._keyring.get_keyring()
            viable = getattr(backend, "viability", None)
            if viable is not None and not backend.viability:
                raise ConnectorError("AGENT_AUTH_REQUIRED", "vault",
                                     "OS keyring is locked or unavailable",
                                     action="Unlock the OS keyring or "
                                            "approve the restricted-file "
                                            "fallback explicitly.")
        except ConnectorError:
            raise
        except Exception as exc:  # pragma: no cover - backend specific
            raise ConnectorError("AGENT_AUTH_REQUIRED", "vault",
                                 f"OS keyring unusable: {exc}") from exc

    def store(self, namespace: str, secret: str) -> str:
        _validate(namespace, secret)
        self._check()
        try:
            self._keyring.set_password(self.SERVICE, namespace, secret)
        except Exception as exc:
            raise ConnectorError("AGENT_AUTH_REQUIRED", "vault",
                                 f"keyring write failed: {exc}") from exc
        return f"{_HANDLE_PREFIX}{namespace}"

    def resolve(self, handle: str) -> str:
        namespace = namespace_of(handle)
        self._check()
        try:
            value = self._keyring.get_password(self.SERVICE, namespace)
        except Exception as exc:
            raise ConnectorError("AGENT_AUTH_REQUIRED", "vault",
                                 f"keyring read failed: {exc}") from exc
        if value is None:
            raise ConnectorError("AGENT_AUTH_REQUIRED", "vault",
                                 f"no secret stored for {namespace}")
        return value

    def replace(self, namespace: str, secret: str) -> str:
        return self.store(namespace, secret)

    def remove(self, namespace: str) -> None:
        try:
            self._keyring.delete_password(self.SERVICE, namespace)
        except Exception:
            pass

    def list_namespaces(self) -> list[str]:
        return []


def open_vault(directory: Path, *, approved_fallback: bool) -> SecretVault:
    """Prefer a working OS keyring; fall back only to the approved file.

    Set ``OKTO_NEXUS_CONNECTOR_VAULT=file`` to force the restricted-file
    backend (containers/tests where no keyring daemon exists).
    """
    import os
    if os.environ.get("OKTO_NEXUS_CONNECTOR_VAULT", "").strip().lower() \
            == "file":
        return RestrictedFileVault(directory, approved=approved_fallback)
    try:
        import keyring  # type: ignore[import-not-found]
        vault = KeyringVault(keyring)
        vault._check()
        return vault
    except ImportError:
        pass
    except ConnectorError:
        raise
    return RestrictedFileVault(directory, approved=approved_fallback)


def vault_backend_names() -> list[str]:
    try:
        import keyring  # type: ignore[import-not-found]
        return [f"keyring:{getattr(keyring.get_keyring(), 'name', 'unknown')}"]
    except Exception:
        return []


def _validate(namespace: str, secret: str) -> None:
    if (not isinstance(namespace, str) or not namespace or
            len(namespace) > 256 or any(c in namespace for c in "\r\n\x00")):
        raise ConnectorError("VALIDATION_ERROR", "vault", "bad namespace")
    if (not isinstance(secret, str) or not secret or len(secret) > 4096 or
            "\x00" in secret or "\r" in secret or "\n" in secret):
        raise ConnectorError("VALIDATION_ERROR", "vault", "bad secret shape")


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
