"""Command dispatch for the CLI."""

from __future__ import annotations

import asyncio
from pathlib import Path

from ...errors import ConnectorError
from ..output import Output


async def dispatch(args, output: Output):
    root = _state_dir(args)
    command = args.command
    if command == "connect":
        from .connect import run_connect
        return await run_connect(args, output, root)
    if command == "identity":
        from .identity import run_identity
        return await run_identity(args, output, root)
    if command == "discover":
        from .discover import run_discover
        return await run_discover(args, output, root)
    if command == "bind":
        from .bind import run_bind
        return await run_bind(args, output, root)
    if command == "runtime":
        from .runtime import run_runtime
        return await run_runtime(args, output, root)
    if command == "daemon":
        from .daemon import run_daemon
        return await run_daemon(args, output, root)
    if command == "service":
        from .service import run_service
        return await run_service(args, output, root)
    if command == "doctor":
        from .doctor import run_doctor
        return await run_doctor(args, output, root)
    if command == "mcp-config":
        from .mcp_config import run_mcp_config
        return await run_mcp_config(args, output, root)
    raise ConnectorError("CAPABILITY_UNSUPPORTED", "cli",
                         f"unknown command {command!r}")


def _state_dir(args) -> Path:
    from ...platform import paths
    explicit = getattr(args, "state_dir", None)
    return paths.state_dir(explicit)
