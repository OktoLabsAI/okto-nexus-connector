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
from dataclasses import dataclass
from pathlib import Path

from .. import __version__
from ..errors import ConnectorError
from ..ipc.client import IPCClient
from ..platform.service_identity import service_instance_id
from .lock import InstanceLock

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
    supervisor: str = 'foreground'
    user_service: dict | None = None
    next_action: str | None = None
    launch_recovery: dict | None = None

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
            "supervisor": self.supervisor,
            "user_service": self.user_service,
            "next_action": self.next_action,
            "launch_recovery": self.launch_recovery,
        }


def _lock(root: Path) -> InstanceLock:
    return InstanceLock(root.resolve() / 'run')


def _process_status(root: Path, *, probe: bool = True, timeout: float = 2.0) -> DaemonStatus:
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
    supervisor = 'foreground'
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
                    supervisor = result.get('supervisor', 'foreground')
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
        instance_id=instance_id, generation=readiness.generation, supervisor=supervisor)


def status(root: Path, *, probe: bool = True, timeout: float = 2.0, _launch_owned=False) -> DaemonStatus:
    from .lifecycle import observe
    return observe(root.resolve(), _process_status(root, probe=probe, timeout=timeout), launch_owned=_launch_owned)


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


def recover_start(root: Path, *, expected_revision: str) -> dict[str, object]:
    from .lifecycle import recover_start as recover
    return recover(root.resolve(), expected_revision=expected_revision)


def start(root: Path, *, timeout: float = START_TIMEOUT) -> dict[str, object]:
    from .lifecycle import start as ensure_running
    try:
        return ensure_running(root.resolve(), timeout=timeout)
    except (OSError, ValueError) as error:
        raise ConnectorError('DAEMON_UNAVAILABLE', 'start', 'Lifecycle persistence or supervisor response was not confirmed.',
            possible_effect=True, action='Inspect the retained launch and logs. No force or repeated launch was requested.') from error


def stop(root: Path, *, timeout: float = 60.0) -> dict[str, object]:
    from .lifecycle import stop as drain
    try:
        return drain(root.resolve(), timeout=timeout)
    except (OSError, ValueError) as error:
        raise ConnectorError('DAEMON_UNAVAILABLE', 'stop', 'The shutdown request or observation was not confirmed.',
            possible_effect=True, action='Observe the same generation and retained shutdown receipt; no force was requested.') from error


def restart(root: Path, *, timeout: float = START_TIMEOUT) -> dict[str, object]:
    stopped = stop(root, timeout=timeout)
    if stopped.get('state') != 'stopped' or stopped.get('shutdown_outcome') == 'unconfirmed':
        return stopped
    return start(root, timeout=timeout)


def _spawn(context, command):
    from ..platform.service_context import launch_environment
    options = {'creationflags': subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP |
               subprocess.CREATE_BREAKAWAY_FROM_JOB} if os.name == 'nt' else {'start_new_session': True}
    return subprocess.Popen(command, cwd=context['cwd'], env=launch_environment(Path(context['root']), context), close_fds=True,
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **options)


async def run_foreground(root: Path, *, acquired_lock=None, supervisor='foreground') -> int:
    """The production entry acquires exclusion before state/vault/Core bootstrap."""
    from ..platform.service_context import INSTALLATION, launch_exclusion
    root = root.resolve()
    owner = acquired_lock or _lock(root)
    if acquired_lock is None:
        with launch_exclusion(root):
            if (root / INSTALLATION).exists():
                raise ConnectorError('SUPERVISOR_REQUIRED', 'run',
                    'The selected instance has an installed supervisor.',
                    action='Use daemon start, or uninstall its user service before foreground execution.')
            if os.name == 'nt':
                from ..platform.user_service import UserService
                UserService(root).observe()
            if not owner.acquire():
                raise ConnectorError('DAEMON_ALREADY_RUNNING', 'run', 'An existing process owns this state directory.')
    elif not owner.held or owner.run_dir != root / 'run':
        raise ValueError('The supplied owner does not match the configured instance')
    guard = None
    try:
        from .logging_setup import configure_logging
        configure_logging(root)
        from .app import DaemonApp
        app = DaemonApp(root, acquired_lock=owner, supervisor=supervisor)
        loop = asyncio.get_running_loop()
        stop_requested = asyncio.Event()

        def _signal(*_args):
            stop_requested.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _signal)
            except (NotImplementedError, RuntimeError):
                try:
                    signal.signal(sig, _signal)
                except (ValueError, OSError):
                    pass

        async def _guard():
            await stop_requested.wait()
            app.request_stop()

        guard = asyncio.create_task(_guard())
        return await app.run_forever()
    finally:
        if guard is not None:
            guard.cancel()
            try:
                await guard
            except asyncio.CancelledError:
                pass
        if acquired_lock is None:
            owner.release()
