"""Per-user state layout and process identity for the local trust domain.

One OS account plus one state directory define the local trust domain. The
directory must be user-private; on POSIX we enforce 0o700, on Windows we
require the directory to live under a per-user root (user profile), whose
ACL the OS already restricts to the account and administrators.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

APP_NAME = "okto-nexus-connector"
ENV_STATE_DIR = "OKTO_NEXUS_CONNECTOR_STATE"

_WINDOWS_USER_ROOT_CANDIDATES = ("AppData", "Local Settings")


def default_state_dir() -> Path:
    override = os.environ.get(ENV_STATE_DIR)
    if override:
        return Path(override).expanduser().absolute()
    if sys.platform == "win32":
        local = os.environ.get("LOCALAPPDATA")
        if not local:
            raise RuntimeError("LOCALAPPDATA is not set; set "
                               f"{ENV_STATE_DIR} explicitly")
        return Path(local) / APP_NAME
    xdg = os.environ.get("XDG_STATE_HOME")
    if xdg:
        return Path(xdg) / APP_NAME
    return Path.home() / ".local" / "state" / APP_NAME


def state_dir(explicit: str | os.PathLike[str] | None = None) -> Path:
    """Resolve (and securely create) the per-user state directory.

    The default location is always inside the per-user profile. An
    explicit override is an operator decision (tests, containers, portable
    layouts) and is accepted as-is; only the default is enforced.
    """
    if explicit is not None:
        root = Path(explicit).expanduser().absolute()
        root.mkdir(parents=True, exist_ok=True)
        _restrict_best_effort(root)
        return root
    root = default_state_dir()
    root.mkdir(parents=True, exist_ok=True)
    _enforce_private(root)
    return root


def _restrict_best_effort(root: Path) -> None:
    """POSIX 0o700; on Windows explicit overrides keep their own ACL."""
    if os.name != "nt":
        os.chmod(root, 0o700)


def _enforce_private(root: Path) -> None:
    if os.name == "nt":
        # A per-user root under the profile already carries a user-only ACL.
        # We refuse world-writable-looking ancestors only heuristically here;
        # full ACL verification is documented in docs/runbook.md.
        profile = Path(os.environ.get("USERPROFILE", str(Path.home()))).resolve()
        resolved = root.resolve()
        if not resolved.is_relative_to(profile):
            raise RuntimeError(
                f"state directory {resolved} is outside the user profile; "
                f"set {ENV_STATE_DIR} to a user-private path")
    else:
        mode = root.stat().st_mode & 0o777
        if mode != 0o700:
            os.chmod(root, 0o700)
    marker = root / ".user-private"
    if not marker.exists():
        marker.write_text("user-private state directory\n", encoding="utf-8")


def journal_path(root: Path) -> Path:
    return root / "executor-journal.db"


def owned_slot_ledger_path(root: Path) -> Path:
    return root / "owned-slots.db"


def state_file(root: Path) -> Path:
    return root / "state.json"


def vault_dir(root: Path) -> Path:
    directory = root / "vault"
    directory.mkdir(parents=True, exist_ok=True)
    if os.name != "nt":
        os.chmod(directory, 0o700)
    return directory


def runtime_dir(root: Path) -> Path:
    directory = root / "runtime"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def logs_dir(root: Path) -> Path:
    directory = root / "logs"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def pid_dir(root: Path) -> Path:
    directory = root / "run"
    directory.mkdir(parents=True, exist_ok=True)
    return directory
