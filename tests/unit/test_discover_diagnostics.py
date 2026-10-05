"""Discovery explains an empty host and reports one consistent observation."""
import io
import json
import subprocess

import pytest
from nexus_connector_core import CoreError, InstallationCandidate

from okto_nexus_connector.cli.commands import discover
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.errors import ConnectorError
from tests.unit.test_discovery_configuration import registered


@pytest.mark.parametrize('json_mode', [False, True])
async def test_empty_discovery_explains_local_scope_without_effects(tmp_path, monkeypatch, json_mode):
    async def empty(*args, **kwargs):
        return []
    monkeypatch.setattr(discover, 'inventory_candidates', empty)
    monkeypatch.setattr(subprocess, 'Popen', lambda *a, **k: pytest.fail('Passive discovery executed a process'))
    stream = io.StringIO()
    output = Output(json_mode=json_mode, stream=stream)
    before = list(tmp_path.iterdir())
    result = await discover.run_discover(build_parser().parse_args(['discover']), output, tmp_path)
    if json_mode:
        output.result(result)
    else:
        discover.render_summary(result, output)
    assert result['candidates'] == []
    assert 'other computers on the network' in result['hint']
    assert 'Get-Command' in result['hint']
    assert 'discover --server-id' in result['hint']
    assert list(tmp_path.iterdir()) == before
    if json_mode:
        assert json.loads(stream.getvalue()) == result
    else:
        assert 'No installations detected' in stream.getvalue()
        assert 'Harness' in stream.getvalue() and '--verbose' in stream.getvalue()
        assert 'fingerprint' not in stream.getvalue()


async def test_configured_empty_discovery_points_to_persisted_roots(registered, tmp_path, monkeypatch):
    from okto_nexus_connector.services import discovery_configuration
    async def empty(*args, **kwargs):
        return []
    monkeypatch.setattr(discovery_configuration, 'configured_candidates', empty)
    before = registered.path.read_bytes()
    result = await discover.run_discover(
        build_parser().parse_args(['discover', '--server-id', 'first']),
        Output(json_mode=True, stream=io.StringIO()), tmp_path)
    assert 'executor show SERVER_ID' in result['hint']
    assert 'approved discovery roots must also be on PATH' in result['hint']
    assert registered.path.read_bytes() == before


async def test_candidates_and_assessment_share_one_filtered_observation(tmp_path, monkeypatch):
    candidate = InstallationCandidate('codex_app_server', str(tmp_path / 'codex.exe'),
                                      'sha256:' + 'a' * 64, 'explicit', 'selected')
    calls = []
    async def inventory(adapters, *, extra):
        calls.append((adapters, extra))
        assert len(calls) == 1, 'A second scan can disagree with displayed candidates'
        return [candidate]
    def assessment(candidates):
        assert candidates == [candidate]
        assert candidates[0] is candidate
        return {'observed': candidate.fingerprint}
    monkeypatch.setattr(discover, 'inventory_candidates', inventory)
    monkeypatch.setattr(discover, 'availability_snapshot', assessment)
    result = await discover.run_discover(
        build_parser().parse_args(['discover', '--harness', 'codex_app_server']),
        Output(json_mode=True, stream=io.StringIO()), tmp_path)
    assert calls == [(['codex_app_server'], [])]
    assert result['candidates'][0]['fingerprint'] == result['availability']['observed']
    assert 'hint' not in result


async def test_assessment_failure_is_not_silently_reported_as_success(tmp_path, monkeypatch):
    async def empty(*args, **kwargs):
        return []
    def failed(candidates):
        raise CoreError('CAPABILITY_UNSUPPORTED', 'discovery')
    monkeypatch.setattr(discover, 'inventory_candidates', empty)
    monkeypatch.setattr(discover, 'availability_snapshot', failed)
    with pytest.raises(ConnectorError) as error:
        await discover.run_discover(build_parser().parse_args(['discover']),
            Output(json_mode=True, stream=io.StringIO()), tmp_path)
    assert error.value.code == 'CAPABILITY_UNSUPPORTED'
