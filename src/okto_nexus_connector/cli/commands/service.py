"""``service`` subcommands (plan C09)."""

from __future__ import annotations

from pathlib import Path

from ...errors import ConnectorError
from ...platform import service_install
from ..output import Output
from ..prompts import confirm


async def run_service(args, output: Output, root: Path):
    sub = args.subcommand
    if sub == "install":
        if not args.dry_run and not args.non_interactive:
            plan = service_install.plan(root)
            output.result({"plan": service_install._plan_json(plan)})
            if not confirm("Install the autostart service for this user?",
                           non_interactive=False, default=False):
                return {"installed": False, "aborted": True}
        result = service_install.install(root, dry_run=args.dry_run)
        output.line("service install planned" if args.dry_run else
                    "service installed")
        return result
    if sub == "uninstall":
        result = service_install.uninstall(root, dry_run=args.dry_run)
        output.line("service uninstall planned" if args.dry_run else
                    "service uninstalled")
        return result
    if sub == "status":
        return service_install.status(root)
    raise ConnectorError("CAPABILITY_UNSUPPORTED", "service",
                         f"unknown subcommand {sub!r}")
