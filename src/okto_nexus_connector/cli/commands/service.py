"""``service`` subcommands (plan C09)."""

from __future__ import annotations

from pathlib import Path

from ...errors import ConnectorError
from ...platform import service_install
from ..output import Output
from ..prompts import confirm


async def run_service(args, output: Output, root: Path):
    try:
        return await _run_service(args, output, root)
    except (OSError, ValueError) as error:
        raise ConnectorError('SERVICE_OPERATION_UNCONFIRMED', 'service',
            'The selected native operation or retained receipt was not confirmed.',
            possible_effect=args.subcommand != 'status' and not getattr(args, 'dry_run', False),
            action='Inspect this instance in Task Scheduler and its retained lifecycle receipts. No forced overwrite or detached fallback was used.') from error


async def _run_service(args, output: Output, root: Path):
    sub = args.subcommand
    if sub == "install":
        if not args.dry_run and not args.non_interactive:
            plan = service_install.plan(root)
            output.result({"plan": service_install._plan_json(plan)})
            if not confirm("Install the autostart service for this user?",
                           non_interactive=False, default=False):
                return {"installed": False, "aborted": True}
        result = service_install.install(root, dry_run=args.dry_run)
        output.line('service install planned' if args.dry_run else
                    'service installed' if result.get('installed') else 'service install pending — see report')
        return result
    if sub == "uninstall":
        result = service_install.uninstall(root, dry_run=args.dry_run)
        output.line('service uninstall planned' if args.dry_run else
                    'service uninstalled' if result.get('uninstalled') else 'service uninstall pending — see report')
        return result
    if sub == "status":
        return service_install.status(root)
    raise ConnectorError("CAPABILITY_UNSUPPORTED", "service",
                         f"unknown subcommand {sub!r}")
