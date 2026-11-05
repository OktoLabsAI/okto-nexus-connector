"""CLI-side daemon lifecycle: start/run/status/stop (plan 4.1).

``start`` returns only after local readiness (IPC answers ping); a Server
being offline is degradation, not a failed daemon start. Concurrent
starters race safely on the OS lock: exactly one process becomes the
daemon, the others detect the live owner and reuse it (TC-07). ``run`` is
the foreground variant for containers and diagnostics (TC-37).
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from .. import __version__
from ..errors import ConnectorError
from ..ipc.client import IPCClient
from ..platform.service_identity import service_instance_id
from .lock import InstanceLock, wait_for_readiness

START_TIMEOUT = 45.0


@dataclass(slots=True)
class DaemonStatus:
    running: bool
    pid: int | None
    transport: str
    address: str
    version: str
    started_at: str
    uptime: float | None
    ipc_ok: bool
    draining: bool = False
    state: str = 'unknown'
    reason: str | None = None
    instance_id: str = ''
    generation: str = ''

    def to_json(self) -> dict[str, object]:
        return {
            "running": self.running,
            "pid": self.pid,
            "transport": self.transport,
            "address": self.address,
            "version": self.version,
            "started_at": self.started_at,
            "uptime_seconds": self.uptime,
            "ipc_ok": self.ipc_ok,
            "draining": self.draining,
            "state": self.state,
            "reason": self.reason,
            "component": "connector",
            "instance_id": self.instance_id,
            "generation": self.generation,
            "installed_version": __version__,
            "active_version": self.version or None,
            "ready_for_queries": self.ipc_ok,
            "remote_link": "not_checked",
            "work_state": "not_checked",
        }


def _lock(root: Path) -> InstanceLock:
    return InstanceLock(root.resolve() / 'run')


def status(root: Path, *, probe: bool = True, timeout: float = 2.0) -> DaemonStatus:
    lock = _lock(root)
    instance_id = service_instance_id(root)
    locked = lock.locked()
    readiness = lock.live_owner()
    if readiness is None:
        return DaemonStatus(False, None, "", "", "", "", None, False,
            state='stopped' if locked is False else 'starting' if locked is True else 'unknown',
            reason='NO_SERVICE_OWNER' if locked is False else 'INSTANCE_OWNERSHIP_UNCONFIRMED',
            instance_id=instance_id)
    ipc_ok = False
    draining = False
    reason = None
    if (readiness.instance_id != instance_id or not readiness.generation):
        reason = 'INSTANCE_MISMATCH'
    elif readiness.connector_version != __version__ or readiness.protocol_revision != 1:
        reason = 'VERSION_INCOMPATIBLE'
    elif probe:
        try:
            token = lock.load_token()
            with IPCClient(readiness.transport, readiness.address,
                           token, connect_timeout=timeout) as client:
                response = client.call("ping", timeout=timeout)
                result = response.get("result", {})
                if isinstance(result, dict):
                    ipc_ok = (response.get('ok') is True and result.get('pong') is True
                        and result.get('pid') == readiness.pid
                        and result.get('instance_id') == instance_id
                        and result.get('generation') == readiness.generation
                        and result.get('version') == __version__ and result.get('protocol_revision') == 1)
                    draining = result.get("draining") is True
                if not ipc_ok:
                    reason = 'IPC_IDENTITY_MISMATCH'
        except (ConnectorError, OSError):
            reason = 'IPC_UNREACHABLE'
    return DaemonStatus(
        running=True,
        pid=readiness.pid,
        transport=readiness.transport,
        address=readiness.address,
        version=readiness.connector_version,
        started_at=readiness.started_at,
        uptime=None,
        ipc_ok=ipc_ok,
        draining=draining,
        state='stopping' if draining else 'ready' if ipc_ok else 'degraded',
        reason=reason,
        instance_id=instance_id, generation=readiness.generation)


def connect(root: Path, *, timeout: float = 5.0) -> IPCClient:
    """Return an authenticated IPC client to the live daemon."""
    lock = _lock(root)
    readiness = lock.live_owner()
    if readiness is None:
        raise ConnectorError("DAEMON_UNAVAILABLE", "connect",
                             "no live daemon in this state directory",
                             action="Run 'okto-nexus-connector daemon start'.")
    token = lock.load_token()
    client = IPCClient(readiness.transport, readiness.address, token)
    client.connect()
    return client


def start(root: Path, *, timeout: float = START_TIMEOUT) -> dict[str, object]:
    """Ensure the daemon is running; spawn detached when needed."""
    lock = _lock(root)
    current = status(root)
    if current.state != 'stopped':
        deadline = time.monotonic() + timeout
        while current.state in {'starting', 'degraded'} and current.reason in {
                'INSTANCE_OWNERSHIP_UNCONFIRMED', 'IPC_UNREACHABLE'} and time.monotonic() < deadline:
            time.sleep(.1)
            current = status(root, timeout=min(2, max(.01, deadline - time.monotonic())))
        if current.state != 'ready':
            raise ConnectorError('DAEMON_UNAVAILABLE', 'start',
                f'The existing instance is {current.state}: {current.reason}.',
                action='Inspect daemon status. No competing daemon was started or stopped.')
        return {"started": False, "already_running": True,
                "pid": current.pid, **{k: v for k, v in
                                       current.to_json().items()
                                       if k != "running"}}
    if os.environ.get("OKTO_NEXUS_CONNECTER_DAEMON_FG") == "1":
        # Reserved for tests that must run the daemon in-process.
        raise ConnectorError("DAEMON_UNAVAILABLE", "start",
                             "foreground daemon requested via env")
    env = dict(os.environ)
    env["OKTO_NEXUS_CONNECTOR_STATE"] = str(root)
    kwargs: dict[str, object] = {"env": env, "close_fds": True,
                                 "cwd": str(root),
                                 "stdout": subprocess.DEVNULL,
                                 "stderr": subprocess.DEVNULL,
                                 "stdin": subprocess.DEVNULL}
    if sys.platform == "win32":
        kwargs["creationflags"] = (
            getattr(subprocess, "DETACHED_PROCESS", 0x00000008) |
            getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200) |
            getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0x01000000))
    else:
        kwargs["start_new_session"] = True
    command = [sys.executable, "-m", "okto_nexus_connector.daemon"]
    try:
        process = subprocess.Popen(command, **kwargs)
    except OSError as exc:
        if sys.platform != "win32" or getattr(exc, "winerror", None) != 5:
            raise
        # Retrying inside a restrictive job can report readiness and then
        # lose the daemon when the CLI's supervisor exits. Do not weaken the
        # promised independent lifetime or bypass the supervisor's policy.
        raise ConnectorError(
            "DAEMON_UNAVAILABLE", "start",
            "Windows denied creation of an independent daemon process. "
            "The current supervisor may prohibit Job Object breakaway.",
            action="Run 'okto-nexus-connector daemon run' under a persistent "
                   "supervisor, or start from a terminal that permits an "
                   "independent daemon.") from exc
    readiness = wait_for_readiness(lock, timeout=timeout)
    observed = status(root)
    if observed.state != 'ready':
        raise ConnectorError("DAEMON_UNAVAILABLE", "start",
                             f"daemon pid {readiness.pid} did not answer IPC",
                             action="Inspect 'daemon run' output in the "
                                    "foreground.")
    return {"started": True, "pid": readiness.pid,
            "spawned_pid": process.pid,
            "transport": readiness.transport,
            "address": readiness.address, **observed.to_json()}


def stop(root: Path, *, timeout: float = 60.0) -> dict[str, object]:
    """Ask the daemon to drain and stop; report per-resource outcomes."""
    lock = _lock(root)
    readiness = lock.live_owner()
    if readiness is None:
        return {"stopped": False, "was_running": False,
                "state": 'stopped' if lock.locked() is False else 'unknown',
                "note": "No confirmed live owner; runtime work has not been inspected."}
    observed = status(root)
    if observed.state not in {'ready', 'stopping'}:
        return {'stopped': False, 'was_running': True, **observed.to_json(),
                'note': 'IPC identity/readiness is unconfirmed; shutdown was not requested.'}
    token = lock.load_token()
    shutdown_report: dict[str, object] = {}
    try:
        with IPCClient(readiness.transport, readiness.address,
                       token, ) as client:
            client.call("shutdown", {'instance_id': observed.instance_id,
                                      'generation': observed.generation}, timeout=min(5, timeout))
    except (ConnectorError, OSError):
        pass
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if lock.locked() is False:
            import json
            try:
                receipt = json.loads((root / 'run' / 'shutdown.json').read_text(encoding='utf-8'))
            except (OSError, ValueError):
                receipt = {}
            drained = receipt.get('generation') == readiness.generation and receipt.get('state') == 'DRAINED'
            return {"stopped": drained, "was_running": True,
                    'state': 'stopped', 'shutdown_outcome': 'drained' if drained else 'unconfirmed',
                    "report": receipt if receipt.get('generation') == readiness.generation else {}}
        time.sleep(0.2)
    return {"stopped": False, "was_running": True,
            "note": "daemon did not exit in time; it may still be draining",
            "report": shutdown_report}


async def run_foreground(root: Path) -> int:
    """Run the daemon in the foreground until Ctrl+C/SIGTERM (TC-37)."""
    from .logging_setup import configure_logging
    configure_logging(root)
    from .app import DaemonApp
    app = DaemonApp(root)
    loop = asyncio.get_running_loop()
    stop_requested = asyncio.Event()

    def _signal(*_args) -> None:
        stop_requested.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal)
        except (NotImplementedError, RuntimeError):
            # Windows ProactorEventLoop: fall back to signal.signal.
            try:
                signal.signal(sig, _signal)
            except (ValueError, OSError):
                pass

    async def _guard() -> None:
        await stop_requested.wait()
        app.request_stop()

    guard = asyncio.create_task(_guard())
    try:
        return await app.run_forever()
    finally:
        guard.cancel()
        try:
            await guard
        except asyncio.CancelledError:
            pass
