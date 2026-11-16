"""Explicit pending-launch fencing never starts/stops processes or runtime work."""
import asyncio
import json
import os
import subprocess
import sys
from unittest.mock import Mock

import pytest

from okto_nexus_connector.cli.main import main
from okto_nexus_connector.daemon import manager, service_process
from okto_nexus_connector.daemon.lock import InstanceLock
from okto_nexus_connector.platform.service_context import (
    LAUNCH, digest, launch_exclusion, read_document, write_document,
)
from okto_nexus_connector.platform.service_identity import service_instance_id


@pytest.fixture
def selected(tmp_path):
    root = tmp_path / 'connector'
    (root / 'run').mkdir(parents=True)
    receipt = {'revision': 1, 'instance_id': service_instance_id(root), 'state': 'pending',
               'token': 'pending-token', 'pid': os.getpid()}
    write_document(root / 'run' / LAUNCH, receipt)
    return root, receipt, digest(receipt)


def test_recovery_fences_late_child_without_bootstrap_and_is_idempotent(selected, monkeypatch):
    root, receipt, revision = selected
    assert manager.status(root).launch_recovery['receipt_revision'] == revision
    monkeypatch.setattr(subprocess, 'Popen', Mock(side_effect=AssertionError('No launch during recovery')))
    result = manager.recover_start(root, expected_revision=revision)
    assert result['recovered'] and result['changed'] and result['state'] == 'stopped'
    assert manager.recover_start(root, expected_revision=revision)['changed'] is False
    assert asyncio.run(service_process.run(root, receipt['token'])) == 2
    assert not (root / 'state.json').exists()
    assert not (root / 'vault').exists()
    assert manager.status(root).reason == 'STARTUP_ABORTED'


@pytest.mark.parametrize('guard', ['launch', 'owner'])
def test_recovery_refuses_launch_or_owner_exclusion(selected, guard):
    root, _, revision = selected
    before = (root / 'run' / LAUNCH).read_bytes()
    if guard == 'launch':
        with launch_exclusion(root):
            result = manager.recover_start(root, expected_revision=revision)
    else:
        lock = InstanceLock(root / 'run')
        assert lock.acquire()
        try:
            result = manager.recover_start(root, expected_revision=revision)
        finally:
            lock.release()
    assert not result['recovered'] and result['reason'] == 'RECOVERY_OWNER_UNCONFIRMED'
    assert result['state'] != 'stopped'
    assert (root / 'run' / LAUNCH).read_bytes() == before


@pytest.mark.parametrize('change,reason', [
    ({'token': 'new-token'}, 'LAUNCH_REVISION_CONFLICT'),
    ({'state': 'owned'}, 'RECOVERY_REQUIRES_PENDING'),
    ({'instance_id': 'foreign'}, 'INVALID_LAUNCH_RECEIPT'),
    ({'revision': 99}, 'INVALID_LAUNCH_RECEIPT'),
])
def test_changed_or_ineligible_receipt_is_preserved(selected, change, reason):
    root, receipt, revision = selected
    path = root / 'run' / LAUNCH
    write_document(path, {**receipt, **change})
    before = path.read_bytes()
    result = manager.recover_start(root, expected_revision=revision)
    assert not result['recovered'] and result['reason'] == reason
    if reason == 'LAUNCH_REVISION_CONFLICT':
        assert result['state'] == 'unknown'
    assert path.read_bytes() == before


def test_installed_supervisor_is_not_modified(selected):
    root, _, revision = selected
    path = root / 'service-installation.json'
    path.write_text('{"preserve":true}', encoding='utf-8')
    assert manager.recover_start(root, expected_revision=revision)['reason'] == 'RECOVERY_REQUIRES_DETACHED'
    assert path.read_text(encoding='utf-8') == '{"preserve":true}'


def test_response_loss_confirms_same_recovery_without_rewriting(selected, monkeypatch):
    from okto_nexus_connector.daemon import lifecycle
    root, _, revision = selected
    def lose(path, receipt):
        write_document(path, receipt)
        raise OSError('response lost')
    monkeypatch.setattr(lifecycle, 'write_document', lose)
    assert manager.recover_start(root, expected_revision=revision)['reason'] == 'RECOVERY_UNCONFIRMED'
    result = manager.recover_start(root, expected_revision=revision)
    assert result['recovered'] and not result['changed']


def test_absent_recovery_and_cli_validation_do_not_initialize(tmp_path, capsys):
    root = tmp_path / 'absent'
    args = ['--state-dir', str(root), 'daemon', 'recover-start', '--json']
    with pytest.raises(SystemExit):
        main(args)
    capsys.readouterr()
    assert main([*args, '--expected-revision', '0' * 64]) == 4
    assert not root.exists()
    assert json.loads(capsys.readouterr().out)['reason'] == 'NO_PENDING_LAUNCH'


def test_actual_cli_recovery_and_late_child_are_fenced(selected, tmp_path):
    root, receipt, revision = selected
    env = {key: value for key, value in os.environ.items() if key.upper() in {
        'SYSTEMROOT', 'WINDIR', 'COMSPEC', 'TEMP', 'TMP', 'USERPROFILE',
        'LOCALAPPDATA', 'APPDATA', 'PATH', 'LANG', 'LC_ALL', 'HOME'}}
    result = subprocess.run([sys.executable, '-I', '-m', 'okto_nexus_connector.cli.main',
        '--state-dir', str(root), 'daemon', 'recover-start', '--expected-revision', revision, '--json'],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert json.loads(result.stdout)['recovered']
    late = subprocess.run([sys.executable, '-I', '-m', 'okto_nexus_connector.daemon.service_process',
        '--state-dir', str(root), '--token', receipt['token']],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=20)
    assert late.returncode == 2, (late.stdout, late.stderr)
    assert not (root / 'state.json').exists()
    assert read_document(root / 'run' / LAUNCH)['state'] == 'aborted_before_bootstrap'
