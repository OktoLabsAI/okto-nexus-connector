"""``mcp-config`` — direct-HTTP MCP client configuration (plan C06).

Planning and applying the harness's *client* entry that talks straight to
the Nexus Server. The connector process is not in the resulting path; a
tools-only conversation configured this way works with the connector
stopped or even uninstalled (TC-24/TC-25).
"""

from __future__ import annotations

from pathlib import Path

from ...errors import ConnectorError
from ...platform import paths
from ...services.mcp_config_service import (
    apply_persistent_entry, plan_direct_entry, remove_persistent_entry,
    tools_only_status,
)
from ...storage.state_store import StateStore
from ..output import Output
from ..prompts import confirm


async def run_mcp_config(args, output: Output, root: Path):
    sub = args.subcommand
    if sub == "plan":
        from ...transport.https_client import is_loopback_origin, \
            origin_of
        plan = plan_direct_entry(
            adapter_id=args.harness, server_url=args.server,
            capability_ref=args.capability_ref,
            approved_origins={origin_of(args.server)},
            harness_is_local=is_loopback_origin(args.server),
            loopback_reachable=is_loopback_origin(args.server))
        payload = plan.to_json()
        payload.update(tools_only_status(plan))
        if args.harness == "pi_rpc":
            raise ConnectorError(
                "CAPABILITY_UNSUPPORTED", "mcp_client_configuration",
                "Pi has no MCP HTTP client in the qualified build",
                action="Use the native non-MCP bridge; stdio MCP is not a "
                       "fallback.")
        return payload
    if sub == "apply":
        from ...transport.https_client import is_loopback_origin, \
            origin_of
        plan = plan_direct_entry(
            adapter_id=args.harness, server_url=args.server,
            capability_ref=args.capability_ref,
            entry_name=args.entry_name,
            approved_origins={origin_of(args.server)},
            harness_is_local=is_loopback_origin(args.server),
            loopback_reachable=is_loopback_origin(args.server))
        output.result({"preview": plan.preview,
                       "file": str(args.file),
                       "note": "third-party entries are preserved; a "
                               "backup is written before any change"})
        if not args.non_interactive and not confirm(
                "Apply this entry to the harness config file?",
                non_interactive=False, default=False):
            return {"applied": False, "aborted": True}
        store = StateStore(paths.state_file(root))
        state = store.load()
        owned = state.preferences.get("mcp.owned_entry")
        result = apply_persistent_entry(plan, args.file,
                                        previously_owned=owned
                                        if isinstance(owned, dict) else None)
        store.update(lambda s: s.preferences.update(
            {"mcp.owned_entry": result.get("owned_entry")}))
        output.line("entry applied; the harness now reaches the Nexus "
                    "Server directly")
        return result
    if sub == "remove":
        store = StateStore(paths.state_file(root))
        state = store.load()
        owned = state.preferences.get("mcp.owned_entry")
        result = remove_persistent_entry(
            args.harness, args.file, entry_name=args.entry_name,
            previously_owned=owned if isinstance(owned, dict) else None)
        if result.get("changed"):
            store.update(lambda s: s.preferences.pop("mcp.owned_entry",
                                                     None))
        output.line("owned entry removed" if result.get("changed") else
                    "nothing to remove")
        return result
    raise ConnectorError("CAPABILITY_UNSUPPORTED", "mcp-config",
                         f"unknown subcommand {sub!r}")
