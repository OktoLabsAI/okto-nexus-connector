"""Command dispatch for the CLI."""

from __future__ import annotations

import asyncio
from pathlib import Path

from ...errors import ConnectorError
from ..output import Output


async def dispatch(args, output: Output):
    from ...transport.proxy import proxy_scope
    from ...platform.paths import default_state_dir
    root = Path(args.state_dir).expanduser().absolute() if getattr(args, 'state_dir', None) else default_state_dir()
    with proxy_scope(root):
        return await _dispatch(args, output)


async def _dispatch(args, output: Output):
    if args.command == 'reach':
        from .reach import run_reach
        return await run_reach(args)
    root = _state_dir(args)
    command = args.command
    if command == 'proxy':
        from .proxy import run_proxy
        return await run_proxy(args, output, root)
    if command == 'configure':
        from .configure import run_configure
        return await run_configure(args, output, root)
    if command == 'connection-config':
        from .connection_config import run_connection_config
        return await run_connection_config(args, output, root)
    if command == 'harness-config':
        from .harness_config import run_harness_config
        return await run_harness_config(args, output, root)
    if command == "connect":
        from .connect import run_connect
        return await run_connect(args, output, root)
    if command == "identity":
        from .identity import run_identity
        return await run_identity(args, output, root)
    if command == "executor":
        from .executor import run_executor
        return await run_executor(args, output, root)
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
    if command == "approvals":
        from .approvals import run_approvals
        return await run_approvals(args, output, root)
    raise ConnectorError("CAPABILITY_UNSUPPORTED", "cli",
                         f"unknown command {command!r}")


def _state_dir(args) -> Path:
    from ...platform import paths
    explicit = getattr(args, "state_dir", None)
    return paths.state_dir(explicit)
