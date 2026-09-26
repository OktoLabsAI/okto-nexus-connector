"""Unit: OS service plans are honest per mechanism (C09.1/C09.2)."""

from __future__ import annotations

import platform
from pathlib import Path

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.platform import service_install


def test_plan_matches_current_os(tmp_path: Path):
    plan = service_install.plan(tmp_path)
    system = platform.system()
    expected = {"Linux": "systemd-user", "Darwin": "launchd-user",
                "Windows": "schtasks-logon"}
    assert plan.mechanism == expected.get(system) or plan.mechanism in \
        expected.values()
    payload = service_install._plan_json(plan)
    assert payload["admin_required"] is False
    assert set(payload["survives"]) == {"terminal_close", "logout", "boot"}


def test_dry_run_install_changes_nothing(tmp_path: Path):
    result = service_install.install(tmp_path, dry_run=True)
    assert result["installed"] is False and result["dry_run"] is True
    assert service_install.status(tmp_path)["installed"] is False


def test_dry_run_uninstall_changes_nothing(tmp_path: Path):
    result = service_install.uninstall(tmp_path, dry_run=True)
    assert result["uninstalled"] is False and result["dry_run"] is True


def test_unsupported_platform_refuses(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(platform, "system", lambda: "SunOS")
    with pytest.raises(ConnectorError) as error:
        service_install.plan(tmp_path)
    assert error.value.code == "CAPABILITY_UNSUPPORTED"
    assert "foreground" in (error.value.action or "")


def test_systemd_unit_content(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(platform, "system", lambda: "Linux")
    plan = service_install.plan(tmp_path)
    unit = service_install._systemd_unit(tmp_path)
    assert "Restart=on-failure" in unit
    assert f"OKTO_NEXUS_CONNECTOR_STATE={tmp_path}" in unit
    assert "WantedBy=default.target" in unit
    assert plan.survives_logout is True


def test_windows_task_is_logon_scoped(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(platform, "system", lambda: "Windows")
    plan = service_install.plan(tmp_path)
    command = service_install._schtasks_create(tmp_path)
    assert "/SC" in command and "ONLOGON" in command
    assert "/RL" in command and "LIMITED" in command
    assert plan.survives_logout is False  # honest: runs at logon only
    assert plan.survives_boot is True
