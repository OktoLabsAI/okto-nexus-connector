"""One foreground parent owns bounded retries of the existing daemon entry."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
import secrets
import subprocess
import time

from nexus_connector_core.user_service_supervision import RetryState, reserve_attempt, record_exit

from ..errors import ConnectorError
from ..platform.service_context import (CONTEXT, CONTROL, digest, launch_environment,
    launch_guard, read_document, validate_context, write_document)
from ..platform.service_supervision import (MODE, RECEIPT, TERMINAL, child_identity, guard,
    ProcessHandle, handshake_path, process_state, read_cycle, require_no_native_retry, valid_identity)
from ..platform.sysinfo import current_process_identity
from ..platform.user_service import UserService, windows_logon_session
from .lock import InstanceLock


@contextmanager
def selection(root, clock, sleep):
    """A concurrent CLI latch is contention, never an unexpected child crash."""
    lock = launch_guard(root)
    deadline = clock() + 30
    while not lock.acquire():
        if clock() >= deadline:
            raise ConnectorError('LIFECYCLE_BUSY', 'supervision', 'The lifecycle selection remains busy.')
        sleep(.05)
    try:
        yield
    finally:
        lock.release()


def acknowledge_child(root, record, installation, service, launcher, *, clock, sleep, open_runtime=ProcessHandle, retain=None):
    """Child declares identity; only parent writes ACK after independent OS proof."""
    deadline = clock() + 60
    while clock() < deadline:
        with selection(root, clock, sleep):
            current_installation = service.receipt()
            if not current_installation or current_installation['token'] != installation['token']:
                raise ValueError('Installation changed before identity acknowledgement')
            current = read_cycle(root, current_installation)
            if not current or current['phase'] != 'running' or any(current.get(key) != record.get(key)
                    for key in ('cycle_id', 'attempts_started', 'attempt_nonce', 'parent', 'launcher')):
                raise ValueError('Attempt changed before identity acknowledgement')
            control = service.control(installation)
            if control.get('cycle_id') != record['cycle_id']:
                raise ValueError('Cycle changed before identity acknowledgement')
            try:
                declaration = read_document(handshake_path(root, record))
            except FileNotFoundError:
                declaration = None
            if declaration is not None:
                expected = {key: record[key] for key in ('instance_id', 'cycle_id', 'attempts_started', 'attempt_nonce')}
                if declaration.get('revision') != 1 or any(declaration.get(key) != value for key, value in expected.items()):
                    raise ValueError('Runtime handshake scope changed')
                identity = {key: declaration.get(key) for key in ('pid', 'birth_token')}
                runtime = open_runtime(identity)
                transferred = False
                try:
                    observed_parent = runtime.parent_pid()
                    direct = identity == record['launcher'] and observed_parent == record['parent']['pid']
                    redirected = (identity != record['launcher'] and observed_parent == record['launcher']['pid']
                        and int(identity['birth_token'][9:]) >= int(record['launcher']['birth_token'][9:]))
                    if (not (direct or redirected) or declaration.get('parent_pid') != observed_parent
                            or launcher.poll() is not None or runtime.exited() or clock() >= deadline):
                        raise ValueError('Runtime ancestry or liveness changed')
                    if retain is not None:
                        retain(runtime)
                        transferred = True
                    record.update(child=identity, child_acknowledged=True)
                    write_document(root / 'run' / RECEIPT, record)
                    return runtime
                except BaseException:
                    if not transferred:
                        runtime.close()
                    raise
        if launcher.poll() is not None:
            raise ValueError('Launcher exited before runtime identity acknowledgement')
        sleep(.05)
    raise ValueError('Runtime identity handshake expired')


def run(root, token, *, service=None, spawn=subprocess.Popen, clock=time.monotonic,
        sleep=time.sleep, identify_child=child_identity, probe=process_state,
        acknowledge=acknowledge_child):
    root = Path(root).resolve()
    service = service or UserService(root)
    parent_lock = guard(root)
    if not parent_lock.acquire():
        return 2
    record, runtime, attempt = None, None, None
    def retain_runtime(handle):
        nonlocal runtime
        runtime = handle
    try:
        with selection(root, clock, sleep):
            installation, task = service.observe()
            if not installation or installation['token'] != token or installation.get('supervision_mode') != MODE:
                return 2
            require_no_native_retry(task)
            control = service.control(installation)
            logon = windows_logon_session()
            if not task.enabled or control['desired'] == 'draining' and control.get('logon_session') == logon:
                return 0
            prior = read_cycle(root, installation)
            if prior:
                if (prior['phase'] not in TERMINAL or not prior['child_exit_confirmed']
                        or probe(prior['parent']) != 'exited'
                        or prior.get('launcher') and probe(prior['launcher']) != 'exited'
                        or prior.get('child') and probe(prior['child']) != 'exited'):
                    return 2
                new_logon = prior.get('logon_session') != logon
                if not new_logon and control.get('cycle_id') == prior['cycle_id']:
                    return 2
            if InstanceLock(root / 'run').locked() is not False:
                return 2
            identity = current_process_identity().to_json()
            if not valid_identity(identity):
                return 2
            cycle = control.get('cycle_id')
            if not cycle or prior and cycle == prior['cycle_id']:
                cycle = secrets.token_hex(16)
            control = {**control, 'desired': 'running', 'cycle_id': cycle,
                'launch_requested': False, 'supervisor_claimed': True, 'logon_session': logon}
            state = RetryState()
            record = {'revision': 1, 'instance_id': installation['instance_id'], 'token': token,
                'cycle_id': cycle, 'logon_session': logon, 'parent': identity, 'child': None,
                'launcher': None, 'attempt_nonce': None, 'child_acknowledged': False,
                'child_exit_confirmed': True, 'launcher_exit_confirmed': True, **asdict(state)}
            write_document(root / 'run' / RECEIPT, record)
            write_document(root / CONTROL, control)

        while True:
            with selection(root, clock, sleep):
                current_installation, task = service.observe()
                if current_installation['token'] != token:
                    raise ValueError('Installation changed')
                require_no_native_retry(task)
                control = service.control(installation)
                if control.get('cycle_id') != cycle:
                    raise ValueError('Supervisor cycle changed')
                desired = control['desired'] == 'running' and task.enabled
                if state.phase == 'backoff' and desired and clock() < state.next_retry_monotonic:
                    attempt = None
                else:
                    state = reserve_attempt(state, clock(), desired)
                    record.update(asdict(state))
                    if state.phase == 'stopped':
                        write_document(root / 'run' / RECEIPT, record)
                        return 0
                    context = validate_context(root, read_document(root / CONTEXT))
                    if digest(context) != installation['context_digest']:
                        raise ValueError('Context changed')
                    # Durable reservation precedes Popen. A missing response can
                    # never be converted into another attempt by a new parent.
                    record.update(launcher=None, child=None, child_exit_confirmed=False,
                        launcher_exit_confirmed=False, child_acknowledged=False, attempt_nonce=secrets.token_hex(16))
                    write_document(root / 'run' / RECEIPT, record)
                    executable = Path(context['executable'])
                    if executable.name.lower() == 'python.exe':
                        executable = executable.with_name('pythonw.exe')
                    command = [str(executable), '-I', '-m', 'okto_nexus_connector.daemon.service_process',
                        '--state-dir', str(root), '--supervised', '--token', token,
                        '--supervisor-cycle', cycle, '--supervisor-nonce', record['attempt_nonce']]
                    # Stop's latch takes this same exclusion; child revalidates it.
                    attempt = spawn(command, cwd=context['cwd'], env=launch_environment(root, context),
                        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        creationflags=subprocess.CREATE_NO_WINDOW)
                    record['launcher'] = identify_child(attempt)
                    if not valid_identity(record['launcher']):
                        raise ValueError('Child identity unavailable')
                    write_document(root / 'run' / RECEIPT, record)
            if attempt is None:
                sleep(.1)
                continue
            runtime = acknowledge(root, record, installation, service, attempt,
                                  clock=clock, sleep=sleep, retain=retain_runtime)
            while attempt.poll() is None:
                sleep(.1)
            # poll/wait uses Popen's retained process handle, never a PID guess.
            launcher_code = attempt.wait()
            exit_code = runtime.wait()
            runtime.close()
            runtime = None
            with selection(root, clock, sleep):
                control = service.control(installation)
                if control.get('cycle_id') != cycle:
                    raise ValueError('Supervisor cycle changed')
                state = record_exit(state, clock(), exit_code, control['desired'] == 'running')
                record.update(asdict(state), child_exit_confirmed=True, launcher_exit_confirmed=True,
                              launcher_exit_code=launcher_code)
                write_document(root / 'run' / RECEIPT, record)
            if state.phase in TERMINAL:
                return 0 if state.phase == 'stopped' else 2
    except (Exception, SystemExit, KeyboardInterrupt):
        if record is not None:
            try:
                record.update(phase='unknown', next_retry_monotonic=None, reason='SUPERVISOR_OUTCOME_UNCONFIRMED')
                write_document(root / 'run' / RECEIPT, record)
            except (OSError, ValueError):
                pass
        return 2
    finally:
        if attempt is not None:
            while attempt.poll() is None:
                try:
                    attempt.wait()
                except (KeyboardInterrupt, SystemExit):
                    continue
        if runtime is not None:
            try:
                while True:
                    try:
                        runtime.wait()
                        break
                    except (KeyboardInterrupt, SystemExit):
                        continue
            finally:
                runtime.close()
        parent_lock.release()


def main(argv=None):
    parser = argparse.ArgumentParser(description='Supervise one persisted Connector user service.')
    parser.add_argument('--state-dir', required=True)
    parser.add_argument('--token', required=True)
    args = parser.parse_args(argv)
    return run(Path(args.state_dir), args.token)


if __name__ == '__main__':
    raise SystemExit(main())
