"""Report the executable R4 contract separately from legacy compatibility."""
from io import StringIO
from types import SimpleNamespace

import nexus_connector_core as core
import pytest

from okto_nexus_connector.cli.commands import doctor
from okto_nexus_connector.cli.output import Output


@pytest.mark.parametrize('condition', ['current', 'disabled', 'invalid', 'missing'])
async def test_doctor_verifies_r4_without_claiming_execution_readiness(tmp_path, monkeypatch, condition):
    async def inventory():
        return []
    monkeypatch.setattr(doctor, 'discover_inventory', inventory)
    if condition == 'disabled':
        monkeypatch.setattr(core, 'R4_BUNDLE_EXECUTABLE', False)
    elif condition == 'missing':
        monkeypatch.delattr(core, 'R4_CONTRACT_REVISION')
    elif condition == 'invalid':
        def invalid():
            raise ValueError('invalid bundle')
        monkeypatch.setattr(core, 'verify_r4_bundle', invalid)
    result = await doctor.run_doctor(SimpleNamespace(probe=False),
        Output(json_mode=True, stream=StringIO()), tmp_path)
    checks = {row['name']: row for row in result['checks'] if row['layer'] == 'core'}
    assert checks['legacy_contract']['detail'] == core.CONTRACT_REVISION
    assert checks['contract']['status'] == ('ok' if condition == 'current' else 'fail')
    if condition == 'current':
        assert checks['contract']['detail'] == core.R4_CONTRACT_REVISION
        assert 'qualification' in checks['execution_scope']['detail']
        assert 'applied lease are separate' in checks['execution_scope']['detail']
    else:
        assert 'execution_scope' not in checks
        assert 'pinned Core' in checks['contract']['action']
