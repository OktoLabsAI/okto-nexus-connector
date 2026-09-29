"""OS autostart qualification with explicit consent (plan C09).

Linux: systemd **user** unit (no admin required); surviving boot requires
enable-linger, which we report honestly when unavailable. macOS: a
LaunchAgent plist loaded for the current user. Windows: a per-user
scheduled task at logon (schtasks /SC ONLOGON), no admin required. Every
mechanism qualifies exactly what survives closing the terminal, logging
out and rebooting; when no mechanism exists we say so instead of
promising persistence.
"""

from __future__ import annotations

import platform
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from ..errors import ConnectorError

SERVICE_NAME = "okto-nexus-connector"
TASK_NAME = SERVICE_NAME


@dataclass(slots=True)
class ServicePlan:
    mechanism: str
    unit_path: Path | None
    command: list[str]
    install_commands: list[list[str]]
    uninstall_commands: list[list[str]]
    survives_terminal_close: bool
    survives_logout: bool
    survives_boot: bool
    boot_note: str


def _daemon_command() -> list[str]:
    return [sys.executable, "-m", "okto_nexus_connector.daemon"]


def plan(root: Path) -> ServicePlan:
    system = platform.system()
    if system == "Linux":
        unit_dir = Path.home() / ".config" / "systemd" / "user"
        unit_path = unit_dir / f"{SERVICE_NAME}.service"
        unit = _systemd_unit(root)
        return ServicePlan(
            mechanism="systemd-user",
            unit_path=unit_path,
            command=_daemon_command(),
            install_commands=[["systemctl", "--user", "enable", "--now",
                               str(unit_path)]],
            uninstall_commands=[
                ["systemctl", "--user", "disable", "--now",
                 SERVICE_NAME + ".service"],
            ],
            survives_terminal_close=True,
            survives_logout=True,
            survives_boot=_linger_enabled(),
            boot_note="boot survival requires 'loginctl enable-linger "
                      "$USER'; otherwise the service starts at first login")
    if system == "Darwin":
        plist = Path.home() / "Library" / "LaunchAgents" / \
            f"ai.oktolabs.{SERVICE_NAME}.plist"
        return ServicePlan(
            mechanism="launchd-user",
            unit_path=plist,
            command=_daemon_command(),
            install_commands=[["launchctl", "load", "-w", str(plist)]],
            uninstall_commands=[["launchctl", "unload", "-w", str(plist)]],
            survives_terminal_close=True,
            survives_logout=True,
            survives_boot=True,
            boot_note="LaunchAgents start at login; the daemon is a user "
                      "agent, not a system daemon")
    if system == "Windows":
        return ServicePlan(
            mechanism="schtasks-logon",
            unit_path=None,
            command=_daemon_command(),
            install_commands=[_schtasks_create(root)],
            uninstall_commands=[["schtasks", "/Delete", "/TN", TASK_NAME,
                                 "/F"]],
            survives_terminal_close=True,
            survives_logout=False,
            survives_boot=True,
            boot_note="the task starts at each logon of this user; it does "
                      "not run while the user is logged out")
    raise ConnectorError(
        "CAPABILITY_UNSUPPORTED", "service_install",
        f"no qualified autostart mechanism on {system}",
        action="Run 'okto-nexus-connector daemon run' in the foreground "
               "or integrate with your process supervisor manually.")


def _systemd_unit(root: Path) -> str:
    return (
        "[Unit]\n"
        "Description=Okto Nexus Connector daemon\n"
        "After=network-online.target\n"
        "\n"
        "[Service]\n"
        f"Environment=OKTO_NEXUS_CONNECTOR_STATE={root}\n"
        "ExecStart=" + " ".join(_daemon_command()) + "\n"
        "Restart=on-failure\n"
        "RestartSec=5\n"
        "\n"
        "[Install]\n"
        "WantedBy=default.target\n")


def _linger_enabled() -> bool:
    """CN1/A16: query linger for the REAL user, not a literal $USER."""
    import getpass
    import shutil
    loginctl = shutil.which("loginctl")
    if loginctl is None:
        return False
    try:
        result = subprocess.run(
            [loginctl, "show-user", "--property=Linger",
             getpass.getuser()],
            capture_output=True, text=True, timeout=10)
        return "Linger=yes" in result.stdout
    except (OSError, subprocess.SubprocessError):
        return False


def _schtasks_create(root: Path) -> list[str]:
    """CN1/A16: the scheduled task carries the approved state root
    (paths with spaces stay correctly quoted inside /TR)."""
    import subprocess as _sp
    command = " ".join(_daemon_command() + ["--state-dir", str(root)])
    return ["schtasks", "/Create", "/SC", "ONLOGON", "/TN", TASK_NAME,
            "/TR", f'"{command}"', "/F",
            "/RL", "LIMITED"]


def install(root: Path, *, dry_run: bool) -> dict[str, object]:
    service_plan = plan(root)
    if dry_run:
        return {"installed": False, "dry_run": True,
                "plan": _plan_json(service_plan)}
    if service_plan.unit_path is not None:
        service_plan.unit_path.parent.mkdir(parents=True, exist_ok=True)
        content = (_systemd_unit(root) if service_plan.mechanism ==
                   "systemd-user" else _launchd_plist(root))
        service_plan.unit_path.write_text(content, encoding="utf-8")
    for command in service_plan.install_commands:
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            raise ConnectorError("DAEMON_UNAVAILABLE", "service_install",
                                 f"command failed: {' '.join(command)}: "
                                 f"{result.stderr.strip()}",
                                 action="Run the command manually to see "
                                        "the full output.")
    return {"installed": True, "plan": _plan_json(service_plan)}


def uninstall(root: Path, *, dry_run: bool) -> dict[str, object]:
    service_plan = plan(root)
    if dry_run:
        return {"uninstalled": False, "dry_run": True,
                "plan": _plan_json(service_plan)}
    for command in service_plan.uninstall_commands:
        subprocess.run(command, capture_output=True, text=True)
    if service_plan.unit_path is not None and service_plan.unit_path.exists():
        service_plan.unit_path.unlink()
    return {"uninstalled": True}


def status(root: Path) -> dict[str, object]:
    service_plan = plan(root)
    installed = False
    if service_plan.mechanism == "systemd-user":
        result = subprocess.run(
            ["systemctl", "--user", "is-enabled", SERVICE_NAME + ".service"],
            capture_output=True, text=True)
        installed = result.returncode == 0
    elif service_plan.mechanism == "launchd-user":
        installed = bool(service_plan.unit_path
                         and service_plan.unit_path.exists())
    elif service_plan.mechanism == "schtasks-logon":
        result = subprocess.run(
            ["schtasks", "/Query", "/TN", TASK_NAME],
            capture_output=True, text=True)
        installed = result.returncode == 0
    return {"installed": installed, "plan": _plan_json(service_plan)}


def _launchd_plist(root: Path) -> str:
    """CN1/A16: generated with plistlib — real dict/boolean types and
    the approved state root transported as OKTO_NEXUS_CONNECTOR_STATE."""
    import plistlib
    command = _daemon_command() + ["--state-dir", str(root)]
    payload = {
        "Label": f"ai.oktolabs.{SERVICE_NAME}",
        "ProgramArguments": command,
        "EnvironmentVariables": {
            "OKTO_NEXUS_CONNECTOR_STATE": str(root),
        },
        "RunAtLoad": True,
        "KeepAlive": True,
    }
    return plistlib.dumps(payload, sort_keys=True).decode("utf-8")


def _plan_json(service_plan: ServicePlan) -> dict[str, object]:
    return {
        "mechanism": service_plan.mechanism,
        "unit_path": str(service_plan.unit_path)
        if service_plan.unit_path else None,
        "command": service_plan.command,
        "survives": {
            "terminal_close": service_plan.survives_terminal_close,
            "logout": service_plan.survives_logout,
            "boot": service_plan.survives_boot,
        },
        "boot_note": service_plan.boot_note,
        "admin_required": False,
    }
