"""Unregistered foreground Connector in disposable state; no provider or Server."""
import json
import os
import subprocess
import sys
import time

from okto_nexus_connector.daemon import manager
from okto_nexus_connector.ipc.client import IPCClient


def test_explicit_state_entrypoint_correlates_local_ipc_and_persists_drain(tmp_path):
    root = tmp_path / 'isolated connector'
    env = {key: value for key, value in os.environ.items() if key.upper() in {
        'SYSTEMROOT', 'WINDIR', 'COMSPEC', 'TEMP', 'TMP', 'USERPROFILE',
        'LOCALAPPDATA', 'APPDATA', 'PATH', 'LANG', 'LC_ALL', 'HOME'}}
    with (tmp_path / 'daemon.log').open('wb') as log:
        child = subprocess.Popen([sys.executable, '-m', 'okto_nexus_connector.daemon',
            '--state-dir', str(root)], cwd=tmp_path, env=env,
            stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                assert child.poll() is None, (tmp_path / 'daemon.log').read_text()
                status = manager.status(root)
                if status.state == 'ready':
                    break
                time.sleep(.1)
            else:
                raise AssertionError('The configured Connector did not become ready')
            # Windows venv python.exe may be a waiting redirector process.
            # Readiness names the birth-verified process holding the OS lock.
            assert status.pid == manager._lock(root).live_owner().pid
            assert status.to_json()['remote_link'] == 'not_checked'
            reused = manager.start(root)
            assert reused['already_running'] and reused['generation'] == status.generation
            with IPCClient(status.transport, status.address, manager._lock(root).load_token()) as client:
                stale = client.call('shutdown', {'instance_id': status.instance_id, 'generation': 'old'}, timeout=2)
                assert not stale['ok']
            assert manager.status(root).state == 'ready'
            stopped = manager.stop(root, timeout=20)
            assert stopped['stopped'] and stopped['shutdown_outcome'] == 'drained'
            assert child.wait(timeout=10) == 0
            receipt = json.loads((root / 'run' / 'shutdown.json').read_text())
            assert receipt['generation'] == status.generation and receipt['state'] == 'DRAINED'
            assert manager.status(root).state == 'stopped'
            assert (root / 'state.json').exists()
        finally:
            if child.poll() is None:
                manager.stop(root, timeout=10)
            if child.poll() is None:
                # Exact child handle created by this test, never a discovered PID.
                child.terminate()
            child.wait(timeout=10)
