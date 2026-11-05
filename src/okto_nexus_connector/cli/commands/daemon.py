"""``daemon`` subcommands (plan A.11)."""

from __future__ import annotations

from pathlib import Path

from ...daemon import manager
from ...errors import ConnectorError
from ..output import Output


async def run_daemon(args, output: Output, root: Path):
    sub = args.subcommand
    if sub == "start":
        result = manager.start(root, timeout=getattr(args, 'timeout', 45))
        output.line("daemon ready" if result.get("started") else
                    "daemon already running")
        return result
    if sub == "run":
        return await manager.run_foreground(root)
    if sub == "status":
        current = manager.status(root)
        output.line(f'daemon: {current.state}' + (f' ({current.reason})' if current.reason else ''))
        if current.launch_recovery:
            output.line(f"Launch receipt revision: {current.launch_recovery['receipt_revision']}")
        return current.to_json()
    if sub == 'recover-start':
        result = manager.recover_start(root, expected_revision=args.expected_revision)
        output.line('Pending launch fenced.' if result.get('recovered') else 'Launch recovery not confirmed; inspect the report.')
        return result
    if sub == "stop":
        result = manager.stop(root, timeout=getattr(args, 'timeout', 60))
        output.line("daemon stopped" if result.get("stopped") else
                    "daemon stop incomplete — see report")
        return result
    if sub == 'restart':
        result = manager.restart(root, timeout=getattr(args, 'timeout', 45))
        output.line('daemon ready' if result.get('state') == 'ready' else 'daemon restart incomplete — see report')
        return result
    raise ConnectorError("CAPABILITY_UNSUPPORTED", "daemon",
                         f"unknown subcommand {sub!r}")
