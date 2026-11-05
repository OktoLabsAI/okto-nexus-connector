"""OS autostart qualification with explicit consent (plan C09).

Linux: systemd **user** unit (no admin required); surviving boot requires
enable-linger, which we report honestly when unavailable. macOS: a
LaunchAgent plist loaded for the current user. Windows: a per-user
instance-scoped scheduled task at logon (Task Scheduler COM), no admin required. Every
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
    instance_id: str = ''
    name: str = ''
    principal: str = ''


def _daemon_command() -> list[str]:
    return [sys.executable, "-m", "okto_nexus_connector.daemon"]


def plan(root: Path) -> ServicePlan:
    system = platform.system()
    if system == "Linux":
        unit_dir = Path.home() / ".config" / "systemd" / "user"
        unit_path = unit_dir / f"{SERVICE_NAME}.service"
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
            survives_logout=_linger_enabled(),
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
            survives_logout=False,
            survives_boot=False,
            boot_note="LaunchAgents start at login; the daemon is a user "
                      "agent, not a system daemon")
    if system == "Windows":
        from .service_context import saved_context
        from .user_service import UserService
        selected = UserService(root)
        definition = selected.definition(saved_context(root), '0' * 32)
        return ServicePlan(
            mechanism="windows-task-scheduler",
            unit_path=None,
            command=[definition.executable, *definition.arguments],
            install_commands=[],
            uninstall_commands=[],
            survives_terminal_close=True,
            survives_logout=False,
            survives_boot=False,
            boot_note="the task starts at each logon of this user; it does "
                      "not run before login or while the user is logged out; native lifecycle qualification is tracked separately",
            instance_id=selected.instance_id, name=selected.name, principal=selected.principal)
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


def install(root: Path, *, dry_run: bool) -> dict[str, object]:
    service_plan = plan(root)
    if dry_run:
        return {"installed": False, "dry_run": True,
                "plan": _plan_json(service_plan)}
    if service_plan.mechanism == 'windows-task-scheduler':
        return _windows_install(root, service_plan)
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
    if service_plan.mechanism == 'windows-task-scheduler':
        return _windows_uninstall(root)
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
    elif service_plan.mechanism == 'windows-task-scheduler':
        from .user_service import UserService
        selected = UserService(root)
        receipt, task = selected.observe()
        return {'installed': receipt is not None, 'plan': _plan_json(service_plan),
            'native_state': task.state if task else None,
            'definition_digest': receipt['native_digest'] if receipt else None}
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
        "instance_id": service_plan.instance_id,
        "name": service_plan.name,
        "principal": service_plan.principal,
        "qualification": "see R3 platform campaign evidence",
    }


def _windows_install(root, service_plan):
    from ..daemon import manager
    from . import paths
    from .service_context import LAUNCH, launch_exclusion, read_document
    from .user_service import UserService
    root = root.resolve()
    service = UserService(root)
    service.prepare()  # no drain before configuration/ownership/native preflight
    receipt = service.receipt()
    if receipt and receipt['state'] == 'installed':
        return {'installed': True, 'already_installed': True, 'plan': _plan_json(service_plan)}
    before = manager._process_status(root)
    if before.state != 'stopped':
        stopped = manager.stop(root)
        if stopped.get('state') != 'stopped' or stopped.get('shutdown_outcome') == 'unconfirmed':
            return {'installed': False, 'shutdown': stopped, 'plan': _plan_json(service_plan)}
    paths.state_dir(root)
    with launch_exclusion(root):
        if manager._process_status(root).state != 'stopped':
            raise ConnectorError('DAEMON_UNAVAILABLE', 'service_install', 'A process still owns this instance.',
                action='Wait for its canonical drain before changing supervisor.')
        try:
            launch = read_document(root / 'run' / LAUNCH)
        except FileNotFoundError:
            launch = {}
        if launch.get('state') == 'pending':
            raise ConnectorError('STARTUP_PENDING', 'service_install', 'A detached launch remains pending.',
                action='Observe the existing launch; a second supervisor was not installed.')
        service.install()
        (root / 'run' / LAUNCH).unlink(missing_ok=True)
    return {'installed': True, 'plan': _plan_json(service_plan),
        'next_action': 'Use daemon start for immediate execution; the user logon trigger is enabled.'}


def _windows_uninstall(root):
    from ..daemon import manager
    from .service_context import launch_exclusion
    from .user_service import UserService
    root = root.resolve()
    service = UserService(root)
    try:
        service.observe()
    except ConnectorError as error:
        if error.code not in {'INSTALLATION_PENDING', 'SERVICE_DEFINITION_MISSING'}:
            raise
    before = manager._process_status(root)
    if before.state != 'stopped':
        stopped = manager.stop(root)
        if stopped.get('state') != 'stopped' or stopped.get('shutdown_outcome') == 'unconfirmed':
            return {'uninstalled': False, 'shutdown': stopped}
    if not root.exists():
        service.uninstall()
    else:
        with launch_exclusion(root):
            if manager._process_status(root).state != 'stopped':
                raise ConnectorError('DAEMON_UNAVAILABLE', 'service_uninstall', 'The current owner has not stopped.')
            service.uninstall()
    return {'uninstalled': True, 'data_preserved': True}
