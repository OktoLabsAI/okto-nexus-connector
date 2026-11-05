"""Fenced foreground entry for detached and OS-supervised Connector launches."""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path
import sys
import time

from ..errors import ConnectorError
from ..platform.service_context import (CONTEXT, CONTROL, INSTALLATION, LAUNCH,
    digest, launch_exclusion, read_document, validate_context, write_document)
from ..platform.service_context import launch_environment
from ..platform.sysinfo import current_process_identity
from ..platform.user_service import SUPERVISOR, UserService, windows_logon_session
from .lock import InstanceLock


async def run(root, token, *, supervised=False, service=None):
    root = Path(root).resolve()
    owner = InstanceLock(root / 'run')
    receipt_path = root / (INSTALLATION if supervised else 'run/' + LAUNCH)
    service = service or (UserService(root) if supervised else None)
    deadline = time.monotonic() + 30
    while True:
        try:
            with launch_exclusion(root):
                receipt = service.receipt() if supervised else read_document(receipt_path)
                if (not receipt or receipt.get('token') != token
                        or receipt.get('state') != ('installed' if supervised else 'pending')):
                    return 2
                if supervised:
                    control = service.control(receipt)
                    logon = windows_logon_session()
                    if control['desired'] == 'draining' and control.get('logon_session') == logon:
                        return 0
                    _, task = service.observe(check_context=False)
                    if not task.enabled:
                        return 0
                context = validate_context(root, read_document(root / CONTEXT))
                if supervised:
                    if digest(context) != receipt['context_digest']:
                        return 2
                elif (root / INSTALLATION).exists():
                    return 2
                if not owner.acquire():
                    return 2
                if supervised:
                    write_document(root / CONTROL, {'revision': 1, 'token': token,
                        'desired': 'running', 'launch_requested': False, 'logon_session': logon})
                else:
                    identity = current_process_identity()
                    write_document(receipt_path, {**receipt, 'state': 'owned',
                        'pid': identity.pid, 'birth_token': identity.birth_token})
                break
        except ConnectorError as error:
            if error.code != 'LIFECYCLE_BUSY' or time.monotonic() >= deadline:
                owner.release()
                return 2
            await asyncio.sleep(.05)
        except (OSError, ValueError):
            owner.release()
            return 2
    previous_env, previous_cwd = dict(os.environ), Path.cwd()
    previous_streams = sys.stdin, sys.stdout, sys.stderr
    streams, code = [], 1
    try:
        for name in ('stdin', 'stdout', 'stderr'):
            if getattr(sys, name) is None:
                stream = open(os.devnull, 'r' if name == 'stdin' else 'w', encoding='utf-8')
                streams.append(stream)
                setattr(sys, name, stream)
        environment = launch_environment(root, context)
        os.environ.clear()
        os.environ.update(environment)
        os.chdir(context['cwd'])
        from .manager import run_foreground
        code = await run_foreground(root, acquired_lock=owner,
            supervisor=SUPERVISOR if supervised else 'detached')
    except (Exception, SystemExit) as error:
        logging.getLogger(__name__).error('Connector entry failed (%s)', getattr(error, 'code', type(error).__name__))
        code = 1
    finally:
        try:
            write_document(root / 'run' / 'service-exit.json', {'revision': 1, 'token': token, 'exit_code': code})
            if not supervised:
                write_document(receipt_path, {**receipt, 'state': 'exited', 'exit_code': code})
        except OSError:
            pass
        owner.release()
        os.chdir(previous_cwd)
        os.environ.clear()
        os.environ.update(previous_env)
        sys.stdin, sys.stdout, sys.stderr = previous_streams
        for stream in streams:
            stream.close()
    if supervised:
        try:
            if service.control(receipt)['desired'] == 'draining':
                return 0  # runtime uncertainty remains in the canonical receipt
        except (ConnectorError, OSError, ValueError):
            pass
    return code


def main(argv=None):
    parser = argparse.ArgumentParser(description='Run one persisted Connector lifecycle selection.')
    parser.add_argument('--state-dir', required=True)
    parser.add_argument('--token', required=True)
    parser.add_argument('--supervised', action='store_true')
    args = parser.parse_args(argv)
    return asyncio.run(run(Path(args.state_dir), args.token, supervised=args.supervised))


if __name__ == '__main__':
    raise SystemExit(main())
