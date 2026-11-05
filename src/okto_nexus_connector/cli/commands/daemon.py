"""``daemon`` subcommands (plan A.11)."""

from __future__ import annotations

import asyncio
from pathlib import Path

from ...daemon import manager
from ...errors import ConnectorError
from ..output import Output


async def run_daemon(args, output: Output, root: Path):
    sub = args.subcommand
    if sub == "start":
        result = manager.start(root)
        output.line("daemon ready" if result.get("started") else
                    "daemon already running")
        return result
    if sub == "run":
        return await manager.run_foreground(root)
    if sub == "status":
        current = manager.status(root)
        output.line(f'daemon: {current.state}' + (f' ({current.reason})' if current.reason else ''))
        return current.to_json()
    if sub == "stop":
        result = manager.stop(root)
        output.line("daemon stopped" if result.get("stopped") else
                    "daemon stop incomplete — see report")
        return result
    raise ConnectorError("CAPABILITY_UNSUPPORTED", "daemon",
                         f"unknown subcommand {sub!r}")
