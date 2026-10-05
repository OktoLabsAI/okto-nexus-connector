"""The public CLI summarizes discovery without changing machine-readable facts."""
import copy
import json

import pytest

from okto_nexus_connector.cli import commands
from okto_nexus_connector.cli.main import main


@pytest.fixture
def inventory(monkeypatch):
    result = {
        'candidates': [{'adapter_id': 'codex_app_server', 'executable': '/private/codex',
                        'fingerprint': 'sha256:private-fingerprint'},
                       {'adapter_id': 'codex_app_server', 'executable': '/second/codex',
                        'fingerprint': 'sha256:private-fingerprint'}],
        'availability': {'rows': [
            dict(adapter_id='codex_app_server', display_name='Codex (app-server)', state='NOT_PROBED',
                 reasons=['no_build_observation', 'selection_required'], containment='available'),
            dict(adapter_id='codex_app_server', display_name='Codex (app-server)', state='READY_FOR_RUNTIME',
                 reasons=[], containment='available'),
            dict(adapter_id='pi_rpc', display_name='Pi (Node RPC)', state='NOT_INSTALLED',
                 reasons=['no_installed_candidate'], containment='available'),
        ]},
        'note': 'Detailed diagnostic note',
    }
    async def dispatch(args, output):
        return copy.deepcopy(result)
    monkeypatch.setattr(commands, 'dispatch', dispatch)
    return result


def test_default_table_counts_copies_without_leaking_details(inventory, capsys):
    assert main(['discover']) == 0
    text = capsys.readouterr().out
    assert 'Harness' in text and 'Found' in text and 'Status' in text
    assert text.count('Codex (app-server)') == 1
    assert '2' in next(line for line in text.splitlines() if 'Codex (app-server)' in line)
    assert 'Mixed readiness' in text and 'Not detected' in text
    assert '/private/' not in text and 'sha256:' not in text and 'Detailed diagnostic note' not in text
    assert 'runtime approval is separate' in text


def test_verbose_preserves_full_diagnostic_payload(inventory, capsys):
    assert main(['discover', '--verbose']) == 0
    text = capsys.readouterr().out
    assert '/private/codex' in text and 'sha256:private-fingerprint' in text
    assert 'no_build_observation' in text and 'Detailed diagnostic note' in text


@pytest.mark.parametrize('verbose', [False, True])
def test_json_is_complete_and_unchanged(inventory, capsys, verbose):
    assert main(['--json', 'discover', *(['--verbose'] if verbose else [])]) == 0
    assert json.loads(capsys.readouterr().out) == inventory


def test_filter_and_unsupported_host_are_explicit(inventory, capsys):
    row = inventory['availability']['rows'][0]
    row['reasons'].append('containment_unavailable:platform')
    row['containment'] = 'unavailable'
    inventory['availability']['rows'].pop(1)
    assert main(['discover', '--harness', 'codex_app_server', '--server-id', 'first']) == 0
    text = capsys.readouterr().out
    assert 'Host containment unavailable; untrusted' in text
    assert 'docs/runbook.md#unsupported-executor-platform' in text
    assert 'Pi (Node RPC)' not in text
    assert 'Technically ready' not in text


@pytest.mark.parametrize('verbose', [False, True])
def test_found_untrusted_host_unsupported_keeps_json_facts(inventory, capsys, verbose):
    row = inventory['availability']['rows'][0]
    row.update(containment='unavailable')
    row['reasons'].append('containment_unavailable:platform')
    inventory['availability']['rows'].pop(1)
    inventory['candidates'] = [dict(inventory['candidates'][0], trust='untrusted')]
    assert main(['--json', 'discover', *(['--verbose'] if verbose else [])]) == 0
    assert json.loads(capsys.readouterr().out) == inventory
    assert main(['discover']) == 0
    text = capsys.readouterr().out
    line = next(line for line in text.splitlines() if 'Codex (app-server)' in line)
    assert '1' in line and 'Host containment unavailable; untrusted' in line
    assert 'Harness unsupported' not in text


def test_adapter_os_support_is_distinct_from_host_containment(inventory, capsys):
    row = inventory['availability']['rows'][0]
    row['reasons'] = ['platform_not_implemented:darwin']
    row['state'] = 'UNSUPPORTED_PLATFORM'
    inventory['availability']['rows'].pop(1)
    assert main(['discover']) == 0
    text = capsys.readouterr().out
    assert 'Harness unsupported on this OS' in text
    assert 'Host containment unavailable' not in text
    assert 'unsupported-executor-platform' not in text


def test_missing_installation_and_filtered_host_do_not_claim_harness_unsupported(inventory, capsys):
    inventory['availability']['rows'][0]['reasons'].append('containment_unavailable:platform')
    assert main(['discover', '--harness', 'pi_rpc']) == 0
    text = capsys.readouterr().out
    assert 'Not detected' in text
    assert 'containment' not in text and 'unsupported-executor-platform' not in text
