"""Short-lived host lifecycle orchestration; the daemon/Core own runtime work."""
from __future__ import annotations

from dataclasses import replace
import os
from pathlib import Path
import secrets
import time

from ..errors import ConnectorError
from ..ipc.client import IPCClient
from ..platform.service_context import (CONTEXT, INSTALLATION, LAUNCH, launch_exclusion,
    launch_guard, read_document, saved_context, validate_context, write_document)
from ..platform.user_service import SUPERVISOR, UserService


def observe(root, current, *, launch_owned=False):
    try:
        if (root / INSTALLATION).exists():
            service = UserService(root)
            receipt, task = service.observe()
            control = service.control(receipt)
            native = {'name': receipt['name'], 'state': task.state, 'enabled': task.enabled,
                'last_result': task.last_result, 'start_at_login': task.enabled,
                'available_before_login': False, 'available_after_logout': False}
            if current.running and current.ipc_ok and current.supervisor != SUPERVISOR:
                return replace(current, state='failed', reason='SUPERVISOR_MISMATCH', user_service=native)
            current = replace(current, supervisor=SUPERVISOR, user_service=native)
            if current.state == 'ready' and control['desired'] == 'draining':
                return replace(current, state='stopping', draining=True, reason='RUNTIME_DRAINING')
            if current.state == 'stopped' and (task.active or task.state == 0 or control.get('launch_requested')):
                return replace(current, state='stopping' if control['desired'] == 'draining' else 'starting',
                    reason='SUPERVISOR_PENDING')
            return current
        if current.state != 'stopped':
            return current
        if not launch_owned and launch_guard(root).locked() is True:
            return replace(current, state='starting', reason='CONCURRENT_START')
        try:
            receipt = read_document(root / 'run' / LAUNCH)
        except FileNotFoundError:
            return current
        if receipt.get('state') == 'pending':
            return replace(current, state='starting', reason='STARTUP_PENDING', supervisor='detached',
                next_action='Observe the retained launch. A wait timeout never launches a second process.')
        if receipt.get('state') == 'failed':
            return replace(current, state='failed', reason='STARTUP_FAILED', supervisor='detached',
                next_action='Inspect daemon logs, then start explicitly after resolving the failure.')
        return current
    except (ConnectorError, OSError, ValueError, ImportError) as error:
        return replace(current, state='failed', reason=getattr(error, 'code', 'LIFECYCLE_UNCONFIRMED'),
            next_action=getattr(error, 'action', None) or 'Inspect the retained lifecycle documents and selected supervisor.')


def start(root: Path, *, timeout: float):
    from . import manager
    if not 0 < timeout <= 300:
        raise ConnectorError('VALIDATION_ERROR', 'start', 'Timeout must be greater than zero and at most 300 seconds.')
    deadline, selected, spawned_pid = time.monotonic() + timeout, False, None
    root.mkdir(parents=True, exist_ok=True)
    try:
        with launch_exclusion(root):
            current = manager.status(root, _launch_owned=True)
            if current.state == 'stopped' or current.reason == 'STARTUP_FAILED':
                service = UserService(root) if os.name == 'nt' or (root / INSTALLATION).exists() else None
                if service is not None and service.start():
                    selected = True
                else:
                    if os.environ.get('OKTO_NEXUS_CONNECTER_DAEMON_FG') == '1':
                        raise ConnectorError('DAEMON_UNAVAILABLE', 'start', 'Foreground daemon requested via environment.')
                    context = validate_context(root, saved_context(root))
                    write_document(root / CONTEXT, context)
                    receipt = {'revision': 1, 'instance_id': current.instance_id,
                        'token': secrets.token_hex(16), 'state': 'pending', 'pid': None}
                    write_document(root / 'run' / LAUNCH, receipt)
                    command = [context['executable'], '-I', '-m', 'okto_nexus_connector.daemon.service_process',
                        '--state-dir', str(root), '--token', receipt['token']]
                    try:
                        process = manager._spawn(context, command)
                    except (OSError, ConnectorError) as error:
                        write_document(root / 'run' / LAUNCH, {**receipt, 'state': 'failed', 'reason': 'SPAWN_FAILED'})
                        if isinstance(error, ConnectorError):
                            raise
                        raise ConnectorError('DAEMON_UNAVAILABLE', 'start', 'The OS refused an independent daemon process.',
                            action="Inspect the executable and supervisor/job policy. Use 'daemon run' under your existing supervisor if required.") from error
                    spawned_pid, selected = process.pid, True
                    write_document(root / 'run' / LAUNCH, {**receipt, 'pid': spawned_pid})
    except ConnectorError as error:
        if error.code != 'LIFECYCLE_BUSY':
            raise
    current = manager.status(root, timeout=min(2, timeout))
    while current.state in {'starting', 'degraded', 'unknown'} and current.reason in {
            'CONCURRENT_START', 'STARTUP_PENDING', 'SUPERVISOR_PENDING',
            'INSTANCE_OWNERSHIP_UNCONFIRMED', 'IPC_UNREACHABLE'}:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(.1, remaining))
        current = manager.status(root, timeout=min(2, max(.01, deadline - time.monotonic())))
    if current.state != 'ready':
        raise ConnectorError('DAEMON_UNAVAILABLE', 'start', f'The selected instance is {current.state}: {current.reason}.',
            possible_effect=selected or current.state == 'starting', retry_safe=True,
            action='Inspect daemon status/logs. The existing launch was not stopped or repeated.')
    return {'started': selected, 'already_running': not selected, 'spawned_pid': spawned_pid, **current.to_json()}


def _shutdown_receipt(root, generation):
    try:
        receipt = read_document(root / 'run' / 'shutdown.json')
    except (OSError, ValueError):
        return {}
    return receipt if receipt.get('generation') == generation else {}


def stop(root: Path, *, timeout: float):
    from . import manager
    if not 0 < timeout <= 300:
        raise ConnectorError('VALIDATION_ERROR', 'stop', 'Timeout must be greater than zero and at most 300 seconds.')
    deadline = time.monotonic() + timeout
    before = manager.status(root)
    if before.state == 'stopped':
        if (root / INSTALLATION).exists():
            service = UserService(root)
            receipt, _ = service.observe()
            control = service.control(receipt)
            if control['desired'] == 'draining' and control.get('generation'):
                report = _shutdown_receipt(root, control['generation'])
                drained = report.get('state') == 'DRAINED'
                return {'stopped': drained, 'was_running': True, 'state': 'stopped',
                    'shutdown_outcome': 'drained' if drained else 'unconfirmed', 'report': report}
        return {'stopped': False, 'was_running': False, **before.to_json()}
    pending = before.reason == 'SUPERVISOR_PENDING' and not before.generation
    if before.state not in {'ready', 'stopping'} and not pending:
        return {'stopped': False, 'was_running': before.running, **before.to_json()}
    try:
        with launch_exclusion(root):
            current = manager._process_status(root, timeout=min(1, timeout))
            if current.generation != before.generation or (pending and current.state != 'stopped'):
                return {'stopped': False, 'was_running': current.running, **current.to_json(), 'reason': 'INSTANCE_CHANGED'}
            if (root / INSTALLATION).exists():
                UserService(root).begin_stop(before.generation or None)
            if not pending:
                lock = manager._lock(root)
                try:
                    with IPCClient(current.transport, current.address, lock.load_token(), connect_timeout=min(2, timeout)) as client:
                        client.call('shutdown', {'instance_id': current.instance_id, 'generation': current.generation},
                            timeout=min(5, max(.01, deadline - time.monotonic())))
                except (ConnectorError, OSError):
                    pass  # observe the same generation and its durable drain receipt
    except ConnectorError as error:
        if error.code != 'LIFECYCLE_BUSY':
            raise
        return {'stopped': False, 'was_running': before.running, **before.to_json(), 'reason': error.code}
    while True:
        current = manager.status(root, timeout=min(1, max(.01, deadline - time.monotonic())))
        if current.state == 'stopped':
            report = _shutdown_receipt(root, before.generation)
            drained = pending or report.get('state') == 'DRAINED'
            return {'stopped': drained, 'was_running': before.running, **current.to_json(),
                'shutdown_outcome': 'drained' if drained else 'unconfirmed', 'report': report}
        if current.generation and current.generation != before.generation:
            return {'stopped': False, **current.to_json(), 'reason': 'INSTANCE_CHANGED'}
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return {'stopped': False, 'was_running': before.running, **current.to_json(), 'wait_timed_out': True,
                'note': 'The same drain is pending or unconfirmed. No forced stop or restart was requested.'}
        time.sleep(min(.1, remaining))
