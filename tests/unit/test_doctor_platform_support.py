"""Issue #1: distinguish unsupported platforms from a broken supported host."""
from io import StringIO
from types import SimpleNamespace

import pytest

from okto_nexus_connector.cli.commands import doctor
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.services import discovery_service


@pytest.mark.parametrize("preflight,expected", [
    ({"platform": "no qualified containment backend on darwin"}, "unsupported"),
    ({"pidfd": "unavailable"}, "fail"),
    ({"job_objects": "ok"}, "ok"),
])
async def test_doctor_distinguishes_platform_support(tmp_path, monkeypatch, preflight, expected):
    async def inventory():
        return []
    monkeypatch.setattr(doctor, "discover_inventory", inventory)
    monkeypatch.setattr(discovery_service, "containment_status", lambda: preflight)
    result = await doctor.run_doctor(SimpleNamespace(probe=False),
        Output(json_mode=True, stream=StringIO()), tmp_path)
    check, = (row for row in result["checks"] if row["layer"] == "containment")
    assert check["status"] == expected
    assert result["summary"]["unsupported"] == int(expected == "unsupported")
    if expected == "unsupported":
        assert "UNSUPPORTED_PLATFORM" in check["detail"] and "darwin" in check["detail"]
        assert "Windows/Linux" in check["action"]
        assert "runbook.md#unsupported-executor-platform" in check["action"]
    elif expected == "fail":
        assert "PROCESS_CONTAINMENT_UNAVAILABLE" in check["action"]
