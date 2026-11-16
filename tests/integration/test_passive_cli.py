"""Actual CLI diagnostics with disposable state and no provider credentials."""
import json
import os
import subprocess
import sys


def test_cli_observation_never_initializes_an_absent_instance(tmp_path):
    root = tmp_path / 'absent instance'
    env = {key: value for key, value in os.environ.items() if key.upper() in {
        'SYSTEMROOT', 'WINDIR', 'COMSPEC', 'TEMP', 'TMP', 'LANG', 'LC_ALL'}}
    env.update(PATH='', USERPROFILE=str(tmp_path), HOME=str(tmp_path),
        APPDATA=str(tmp_path / 'roaming'), LOCALAPPDATA=str(tmp_path / 'local'),
        OKTO_NEXUS_CONNECTOR_VAULT='file', OKTO_NEXUS_CONNECTOR_STATE=str(root))
    base = [sys.executable, '-I', '-m', 'okto_nexus_connector.cli.main']
    for command in ('status', 'logs', 'doctor'):
        result = subprocess.run([*base, command, '--json'], cwd=tmp_path, env=env,
            capture_output=True, text=True, timeout=45)
        assert result.returncode == 0, (result.stdout, result.stderr)
        payload = json.loads(result.stdout)
        assert not root.exists()
        if command == 'status':
            assert payload['daemon']['state'] == 'stopped'
            assert payload['daemon']['work_state'] == 'not_checked'
        elif command == 'logs':
            assert payload['state'] == 'missing'
        else:
            check = next(c for c in payload['checks'] if c['name'] == 'state-dir')
            assert check['status'] == 'fail'
    assert list(tmp_path.iterdir()) == []
