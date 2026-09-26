"""OS process identity without granting kill authority.

PID alone is not identity (plan 4.1: PID file não basta). We record PID plus
a process birth token: on Windows the process creation time from
``psutil``-free ``GetProcessTimes`` via ctypes; on POSIX ``/proc/<pid>/stat``
field 22 (starttime). A later comparison proves "same process" or exposes
PID reuse. Observing identity never authorizes signaling.
"""

from __future__ import annotations

import ctypes
import os
import sys
from dataclasses import dataclass

_UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    pid: int
    birth_token: str

    def to_json(self) -> dict[str, object]:
        return {"pid": self.pid, "birth_token": self.birth_token}


def current_process_identity() -> ProcessIdentity:
    return ProcessIdentity(os.getpid(), _birth_of(os.getpid()) or _UNKNOWN)


def process_identity(pid: int) -> ProcessIdentity | None:
    """Best-effort identity of another PID; None when unobservable."""
    birth = _birth_of(pid)
    if birth is None:
        return None
    return ProcessIdentity(pid, birth)


def same_process(identity: ProcessIdentity) -> bool:
    observed = _birth_of(identity.pid)
    return observed is not None and observed == identity.birth_token


def _birth_of(pid: int) -> str | None:
    if pid <= 0:
        return None
    try:
        if sys.platform == "win32":
            return _windows_birth(pid)
        return _posix_birth(pid)
    except (OSError, ValueError, AttributeError):
        return None


def _posix_birth(pid: int) -> str | None:
    with open(f"/proc/{pid}/stat", "rb") as stream:
        data = stream.read(4096)
    tail = data[data.rindex(b")") + 2:].split()
    return f"starttime:{int(tail[19])}"


def _windows_birth(pid: int) -> str | None:
    class FILETIME(ctypes.Structure):
        _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]

    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    handle = ctypes.windll.kernel32.OpenProcess(  # type: ignore[attr-defined]
        PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        return None
    try:
        created, exited, kernel, user = (FILETIME(), FILETIME(),
                                         FILETIME(), FILETIME())
        ok = ctypes.windll.kernel32.GetProcessTimes(  # type: ignore[attr-defined]
            handle, ctypes.byref(created), ctypes.byref(exited),
            ctypes.byref(kernel), ctypes.byref(user))
        if not ok:
            return None
        value = (created.high << 32) | created.low
        return f"filetime:{value}"
    finally:
        ctypes.windll.kernel32.CloseHandle(handle)  # type: ignore[attr-defined]
