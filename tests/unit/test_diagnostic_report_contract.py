"""Opt-in proof reports use synthetic facts, never an actual vault or Server."""
from io import StringIO
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import nexus_connector_core as core
import pytest

from okto_nexus_connector.cli.commands import doctor
from okto_nexus_connector.cli.diagnostic_report import (
    CONTRACT_ARTIFACT_SHA256, DiagnosticReport, FINDINGS, STATUSES, aggregate, subject,
)
from okto_nexus_connector.cli.main import main
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services import discovery_service
from okto_nexus_connector.storage.state_store import ConnectorState, IdentityRecord, ServerProfileRecord


def forbidden(*args, **kwargs):
    pytest.fail('Diagnostic attempted a forbidden effect or real dependency')


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    calls = []
    state = ConnectorState(connector_id='synthetic-connector')
    monkeypatch.setattr(core, 'verify_r4_bundle', lambda: None)
    monkeypatch.setattr(core, 'R4_BUNDLE_EXECUTABLE', True)
    monkeypatch.setattr(core, 'R4_CONTRACT_REVISION', 'synthetic-revision')
    monkeypatch.setattr(doctor.StateStore, 'read_snapshot', lambda self: state)
    for name in ('load', 'save', 'update'):
        monkeypatch.setattr(doctor.StateStore, name, forbidden)
    monkeypatch.setattr(doctor, 'vault_backend_names', lambda: ['synthetic-keyring'])
    def vault(*args, **kwargs):
        calls.append('vault-metadata')
        return NS(backend_name=lambda: 'synthetic-keyring', resolve=forbidden, list_namespaces=forbidden)
    monkeypatch.setattr(doctor, 'open_vault', vault)
    monkeypatch.setattr(doctor, 'NexusHTTPClient', forbidden)
    async def inventory():
        calls.append('passive-inventory')
        return []
    monkeypatch.setattr(doctor, 'discover_inventory', inventory)
    monkeypatch.setattr(discovery_service, 'containment_status', lambda: {'synthetic': 'ok'})
    current = NS(ipc_ok=False, state='stopped', pid=None, reason=None, next_action=None)
    monkeypatch.setattr(doctor.manager, 'status', lambda _: current)
    monkeypatch.setattr(doctor.manager, 'connect', forbidden)
    monkeypatch.setattr(doctor.manager, 'start', forbidden)
    import subprocess
    monkeypatch.setattr(subprocess, 'Popen', forbidden)
    return NS(root=tmp_path, state=state, calls=calls, current=current)


async def run(isolated, *, probe=False, format='v1'):
    return await doctor.run_doctor(NS(probe=probe, diagnostic_format=format),
        Output(json_mode=True, stream=StringIO()), isolated.root)


def check(report, identifier, *, subject_id=None):
    matches = [row for row in report['checks'] if row['id'] == identifier
               and (subject_id is None or row['subject']['id'] == subject_id)]
    assert len(matches) == 1
    return matches[0]


def add_server(isolated):
    state = isolated.state
    state.servers['server'] = ServerProfileRecord('server', 'https://synthetic.invalid', '', '')
    state.identities.append(IdentityRecord(alias='synthetic', server_id='server', agent_id='agent',
        secret_handle='synthetic-handle', credential_epoch=1, added_at=''))
    return state.servers['server']


def fake_probe(monkeypatch, *, response=None, error=None):
    used = []
    def vault(*args, **kwargs):
        used.append('vault-object')
        def resolve(_):
            used.append('credential-resolution')
            return 'synthetic-key'
        return NS(backend_name=lambda: 'synthetic-keyring', resolve=resolve)
    monkeypatch.setattr(doctor, 'open_vault', vault)
    http = AsyncMock()
    http.__aenter__.return_value = http
    http.me.return_value = response or NS(agent_id='agent', server_id='server')
    http.me.side_effect = error
    monkeypatch.setattr(doctor, 'NexusHTTPClient', lambda _: http)
    return used, http


async def test_default_legacy_shape_and_call_graph_are_preserved(isolated):
    legacy = await run(isolated, format='legacy')
    initial_calls = list(isolated.calls)
    isolated.calls.clear()
    modern = await run(isolated)
    assert isolated.calls == initial_calls == ['vault-metadata', 'passive-inventory']
    assert set(legacy) == {'checks', 'summary'}
    assert set(legacy['summary']) == {'ok', 'warn', 'fail', 'unsupported', 'version'}
    assert all(set(row) == {'layer', 'name', 'status', 'detail', 'action'} for row in legacy['checks'])
    assert next(row for row in legacy['checks'] if row['layer'] == 'daemon')['status'] == 'warn'
    assert check(modern, 'local_service')['status'] == 'FAIL'
    assert modern['schema'] == 'okto.diagnostics' and modern['format_version'] == 1
    assert modern['component'] == 'connector' and modern['scope'] == 'execution_host'
    assert sum(modern['counts'].values()) == len(modern['checks'])
    assert modern['counts']['UNKNOWN'] > 0 and set(modern['counts']) == set(STATUSES)
    assert not any(modern['effects'].values())
    assert 'credential_read' not in modern['effects']
    assert list(isolated.root.iterdir()) == []


@pytest.mark.parametrize('state,status', [('stopped','FAIL'), ('failed','FAIL'),
    ('unknown','UNKNOWN'), ('starting','UNKNOWN'), ('stopping','UNKNOWN'), ('degraded','UNKNOWN')])
async def test_daemon_warn_is_classified_by_actual_state(isolated, state, status):
    isolated.current.state = state
    value = check(await run(isolated), 'local_service')
    assert value['status'] == status and value['evidence']['state'] == state


@pytest.mark.parametrize('preflight,code,status', [
    ({'platform':'synthetic unsupported platform'}, 'UNSUPPORTED_PLATFORM', 'FAIL'),
    ({'pidfd':'unavailable'}, 'PROCESS_CONTAINMENT_UNAVAILABLE', 'FAIL'),
    ({'job_objects':'ok'}, 'CONTAINMENT_PREFLIGHT_CONFIRMED', 'PASS'),
    ({}, 'CONTAINMENT_UNCONFIRMED', 'UNKNOWN'),
])
async def test_containment_unsupported_is_required_capability_failure(isolated, monkeypatch, preflight, code, status):
    monkeypatch.setattr(discovery_service, 'containment_status', lambda: preflight)
    value = check(await run(isolated), 'runtime_containment')
    assert (value['code'], value['status']) == (code, status)


async def test_responding_ipc_during_shutdown_does_not_claim_ready(isolated, monkeypatch):
    isolated.current.ipc_ok = True
    isolated.current.state = 'stopping'
    monkeypatch.setattr(doctor.manager, 'connect', lambda _: NS(
        call=lambda _: {'result': {'transports': {}}}, close=lambda: None))
    report = await run(isolated)
    assert check(report, 'local_service')['status'] == 'UNKNOWN'
    legacy = await run(isolated, format='legacy')
    assert next(row for row in legacy['checks'] if row['layer'] == 'daemon')['status'] == 'ok'


async def test_large_journal_warn_is_observed_size_with_advisory(isolated, monkeypatch):
    monkeypatch.setattr(doctor.paths, 'journal_path', lambda _: NS(exists=lambda: True,
        stat=lambda: NS(st_size=200*1024*1024)))
    value = check(await run(isolated), 'journal_size_observation')
    assert (value['code'], value['status'], value['severity']) == ('JOURNAL_SIZE_HIGH','PASS','warning')
    assert value['evidence']['bytes'] == 209715200


async def test_snapshot_schema_does_not_claim_an_existing_file(isolated):
    report = await run(isolated)
    assert check(report, 'state_schema')['code'] == 'STATE_SCHEMA_COMPATIBLE'
    assert 'in-memory default' in check(report, 'state_schema')['reason']
    assert check(report, 'managed_connection')['code'] == 'BINDING_NOT_CHECKED'
    assert check(report, 'runtime_eligibility')['status'] == 'UNKNOWN'
    assert check(report, 'provider_login')['status'] == 'UNKNOWN'


@pytest.mark.parametrize('code,expected', [('STATE_INVALID','FAIL'), ('VERSION_INCOMPATIBLE','FAIL'),
    ('STATE_UNAVAILABLE','UNKNOWN'), ('CAPACITY_EXCEEDED','UNKNOWN')])
async def test_snapshot_errors_keep_proof_and_unknown_separate(isolated, monkeypatch, code, expected):
    def unreadable(_):
        raise ConnectorError(code, 'state_read', 'arbitrary-private-text', action='do-not-export-this')
    monkeypatch.setattr(doctor.StateStore, 'read_snapshot', unreadable)
    report = await run(isolated)
    assert check(report, 'state_schema')['status'] == expected
    assert 'arbitrary-private-text' not in json.dumps(report)
    assert 'do-not-export-this' not in json.dumps(report)


async def test_no_probe_does_not_resolve_credentials_or_contact_server(isolated):
    add_server(isolated)
    report = await run(isolated)
    assert check(report, 'server_reachability')['code'] == 'SERVER_CHECK_NOT_REQUESTED'
    assert check(report, 'participant_authentication')['status'] == 'UNKNOWN'
    assert report['mode'] == 'passive'


@pytest.mark.parametrize('server,agent,identity_status,auth_status', [
    ('server','agent','PASS','PASS'), ('wrong-server','agent','FAIL','UNKNOWN'),
    ('server','wrong-agent','PASS','FAIL')])
async def test_probe_checks_server_and_agent_independently(isolated, monkeypatch, server, agent, identity_status, auth_status):
    add_server(isolated)
    used, http = fake_probe(monkeypatch, response=NS(server_id=server, agent_id=agent))
    report = await run(isolated, probe=True)
    assert report['mode'] == 'explicit_checks'
    assert check(report, 'server_reachability')['status'] == 'PASS'
    assert check(report, 'server_identity')['status'] == identity_status
    assert check(report, 'participant_authentication')['status'] == auth_status
    assert used.count('credential-resolution') == 1 and http.me.await_count == 1
    assert check(report, 'provider_login')['status'] == check(report, 'work_result')['status'] == 'UNKNOWN'


async def test_wrong_server_correction_also_applies_to_legacy(isolated, monkeypatch):
    record = add_server(isolated)
    fake_probe(monkeypatch, response=NS(server_id='other', agent_id='agent'))
    result = await doctor._probe_server(record, isolated.state, isolated.root)
    assert result[0] == 'fail' and 'SERVER_ID_MISMATCH' in result[1]
    assert 'other' not in result[1]


@pytest.mark.parametrize('code,reach,auth', [
    ('EXECUTOR_OFFLINE','UNKNOWN','UNKNOWN'), ('AGENT_AUTH_REQUIRED','UNKNOWN','UNKNOWN'),
    ('AUTH_FAILED','PASS','FAIL'), ('PERMISSION_DENIED','PASS','FAIL'),
    ('VERSION_INCOMPATIBLE','PASS','UNKNOWN'), ('ARBITRARY_UNKNOWN','UNKNOWN','UNKNOWN')])
async def test_probe_error_metadata_never_becomes_free_text_or_false_auth_failure(isolated, monkeypatch, code, reach, auth):
    add_server(isolated)
    fake_probe(monkeypatch, error=ConnectorError(code, 'http', 'arbitrary-private-text', action='private-action'))
    report = await run(isolated, probe=True)
    assert check(report, 'server_reachability')['status'] == reach
    assert check(report, 'participant_authentication')['status'] == auth
    assert 'arbitrary-private-text' not in json.dumps(report) and 'private-action' not in json.dumps(report)
    assert 'ARBITRARY_UNKNOWN' not in json.dumps(report)


async def test_unavailable_vault_does_not_attempt_http(isolated, monkeypatch):
    add_server(isolated)
    def locked(*args, **kwargs):
        raise ConnectorError('AGENT_AUTH_REQUIRED', 'vault', 'private-lock-details')
    monkeypatch.setattr(doctor, 'open_vault', locked)
    report = await run(isolated, probe=True)
    assert check(report, 'server_reachability')['status'] == 'UNKNOWN'
    assert check(report, 'participant_authentication')['status'] == 'UNKNOWN'
    assert 'private-lock-details' not in json.dumps(report)


async def test_unexpected_observation_failure_is_bounded_without_raw_exception(isolated, monkeypatch):
    async def failed_inventory():
        raise RuntimeError('unrecognized-private-text')
    monkeypatch.setattr(doctor, 'discover_inventory', failed_inventory)
    report = await run(isolated)
    assert check(report, 'diagnostic_observation')['code'] == 'CHECK_UNCONFIRMED'
    assert 'unrecognized-private-text' not in json.dumps(report)
    with pytest.raises(RuntimeError, match='unrecognized-private-text'):
        await run(isolated, format='legacy')


@pytest.mark.parametrize('format', ['legacy','v1'])
@pytest.mark.parametrize('flags', [[], ['--json'], ['--verbose']])
def test_cli_selector_preserves_connector_exit_and_renders_all_modes(isolated, capsys, format, flags):
    assert main(['--state-dir', str(isolated.root), 'doctor', '--diagnostic-format', format, *flags]) == 0
    value = capsys.readouterr().out
    if '--json' in flags:
        payload = json.loads(value)
        assert ('schema' in payload) == (format == 'v1')
    elif format == 'v1':
        assert 'FAIL' in value and 'SERVICE_NOT_READY' in value


def test_invalid_format_is_usage_before_any_dependencies(isolated, capsys):
    with pytest.raises(SystemExit) as result:
        main(['doctor', '--diagnostic-format', 'v9'])
    assert result.value.code == 2 and isolated.calls == []
    capsys.readouterr()


def test_report_empty_and_evidence_allowlist():
    report = DiagnosticReport()
    assert report.result()['state'] == 'incomplete'
    report.set('SERVICE_UNCONFIRMED', evidence={'state': [], 'bytes': True,
        'schema_version': -1, 'count': 2**54, 'support': {}, 'private':'do-not-copy'})
    assert 'evidence' not in report.result()['checks'][0]
    assert subject('server', 'nxs_'+'a'*32, fallback='server-1')['id'] == 'server-1'
    assert subject('binding', 'path/with/control\n', fallback='binding-1')['id'] == 'binding-1'
    for code, (identifier, status, reason, action) in FINDINGS.items():
        assert status in STATUSES and identifier and reason and action and code.isupper()


CONTRACT = Path(__file__).resolve().parents[1] / 'fixtures' / 'diagnostics_v1'


def test_shared_artifacts_match_the_installed_contract_pin():
    manifest = json.loads((CONTRACT / 'manifest.json').read_text(encoding='utf-8'))
    hashes = {name: hashlib.sha256((CONTRACT / name).read_bytes()).hexdigest()
              for name in manifest['files']}
    assert hashes == manifest['files']
    digest = hashlib.sha256(''.join(f'{hashes[name]}  {name}\n' for name in sorted(hashes)).encode()).hexdigest()
    assert digest == manifest['aggregate_sha256'] == CONTRACT_ARTIFACT_SHA256
    catalog = json.loads((CONTRACT / 'catalog.json').read_text(encoding='utf-8'))
    expected = catalog['components']['connector']['findings']
    assert set(expected) == set(FINDINGS)
    for code, (identifier, status, reason, action) in FINDINGS.items():
        assert expected[code] == dict(id=identifier, status=status, reason=reason, next_action=action)


def test_common_synthetic_vectors_are_exercised_without_runtime_claims():
    vectors = json.loads((CONTRACT / 'vectors.json').read_text(encoding='utf-8'))
    assert 'NOT_RUNTIME_ACCEPTANCE' in vectors['qualification']
    for case in vectors['aggregation']:
        assert aggregate([{'status': status} for status in case['statuses']]) == {
            'counts': case['expected_counts'], 'state': case['expected_state']}
    for case in vectors['reports']:
        expected = case['report']
        if expected['component'] != 'connector':
            continue
        report = DiagnosticReport()
        for row in expected['checks']:
            report.set(row['code'], scope=row['subject'], evidence=row.get('evidence'))
        assert report.result() == expected


async def test_all_emitted_findings_obey_the_shared_schema_vocabulary(isolated):
    schema = json.loads((CONTRACT / 'schema.json').read_text(encoding='utf-8'))
    report = await run(isolated)
    assert set(report) == set(schema['required'])
    row_schema = schema['properties']['checks']['items']
    for row in report['checks']:
        assert set(row_schema['required']) <= set(row) <= set(row_schema['properties'])
        for key in ('id', 'code', 'status', 'severity'):
            assert row[key] in row_schema['properties'][key]['enum']
        assert set(row.get('evidence', {})) <= set(row_schema['properties']['evidence']['properties'])
    with pytest.raises(ValueError, match='Unknown diagnostic'):
        aggregate([{'status':'warn'}])
