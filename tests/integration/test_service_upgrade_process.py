"""Real installed-environment replacement without Server/provider connections."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


def test_reviewed_upgrade_between_installed_environments_preserves_journal(tmp_path):
    target = os.environ.get('R3_UPGRADE_TARGET_PYTHON')
    if not target:
        pytest.skip('Requires a separately installed candidate Python environment')
    assert Path(target).absolute() != Path(sys.executable).absolute()
    root = tmp_path / 'connector'
    base = [sys.executable, '-I', '-m', 'okto_nexus_connector.cli.main', '--state-dir', str(root), '--json']
    env = {key: value for key, value in os.environ.items() if key.upper() in {
        'SYSTEMROOT', 'WINDIR', 'COMSPEC', 'TEMP', 'TMP', 'USERPROFILE', 'HOME',
        'LOCALAPPDATA', 'APPDATA', 'PATH', 'LANG', 'LC_ALL'}}
    def invoke(group, action, *args, expected=0):
        result = subprocess.run([*base, group, action, *args], cwd=tmp_path, env=env,
            capture_output=True, text=True, timeout=75)
        assert result.returncode == expected, (result.stdout, result.stderr)
        return json.loads(result.stdout)
    stopped = False
    try:
        initial = invoke('daemon', 'start')
        original_context = json.loads((root / 'service-context.json').read_text(encoding='utf-8'))
        plan = tmp_path / 'plan.json'
        document = invoke('service', 'upgrade-plan', '--python', target, '--output', str(plan))
        assert json.loads(plan.read_text(encoding='utf-8')) == document
        assert document['plan']['source_generation'] == initial['generation']
        apply_args = ('--plan-file', str(plan), '--expected-revision', document['plan_revision'])
        refusal = invoke('service', 'upgrade-apply', *apply_args, expected=1)
        assert refusal['error']['code'] == 'UPGRADE_OWNER_UNCONFIRMED'
        assert invoke('daemon', 'status')['generation'] == initial['generation']
        assert invoke('daemon', 'stop')['shutdown_outcome'] == 'drained'
        from okto_nexus_connector.platform.paths import owned_slot_ledger_path
        ledger = owned_slot_ledger_path(root)
        before = hashlib.sha256(ledger.read_bytes()).hexdigest()
        assert invoke('service', 'upgrade-apply', *apply_args)['applied']
        assert hashlib.sha256(ledger.read_bytes()).hexdigest() == before
        assert invoke('daemon', 'status')['state'] == 'stopped'
        assert not invoke('service', 'upgrade-apply', *apply_args)['changed']
        context = json.loads((root / 'service-context.json').read_text(encoding='utf-8'))
        assert context['executable'] == target and context['revision'] == 2
        assert context['environment'] == original_context['environment']
        restarted = invoke('daemon', 'start')
        assert restarted['generation'] != initial['generation']
        assert restarted['state'] == 'ready'
        assert json.loads((root / 'service-context.json').read_text(encoding='utf-8')) == context
        assert invoke('daemon', 'stop')['shutdown_outcome'] == 'drained'
        stopped = True
    finally:
        if not stopped and root.exists():
            subprocess.run([*base, 'daemon', 'stop'], cwd=tmp_path, env=env,
                capture_output=True, timeout=70)
