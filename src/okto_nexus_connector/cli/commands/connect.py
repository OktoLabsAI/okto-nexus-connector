"""``connect`` — the guided first-use flow (plan 2.1, C03.1).

Imports the canonical identity (protected entry), discovers local
harnesses, helps select one, shows one aggregated confirmation and
generates binding/profile/endpoint idempotently. ``--start`` adds the
explicit intent to open a runtime; connecting alone never spawns one
(TC-10). Ensures the daemon when needed.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from ...errors import ConnectorError
from ...identity.import_flow import (
    import_identity, read_secret_env, read_secret_from_mcp_entry,
    read_secret_masked, read_secret_stdin,
)
from ...identity.vault import open_vault, vault_backend_names
from ...platform import paths
from ...services.connect_service import create_binding
from ...services.discovery_service import (
    ADAPTERS, discover_inventory, pi_candidate, probe_version,
    select_explicit,
)
from ...storage.state_store import StateStore
from ...transport.https_client import NexusHTTPClient, origin_of
from ..output import Output
from ..prompts import confirm, select


def _read_key(args, output: Output) -> str:
    if args.credential_stdin:
        return read_secret_stdin()
    if args.credential_env:
        return read_secret_env(args.credential_env)
    if args.mcp_entry:
        if not args.mcp_entry_name:
            raise ConnectorError("VALIDATION_ERROR", "connect",
                                 "--mcp-entry requires --mcp-entry-name",
                                 action="Name the entry you explicitly "
                                        "choose to import.")
        return read_secret_from_mcp_entry(
            Path(args.mcp_entry).expanduser(),
            entry_name=args.mcp_entry_name,
            server_origin=origin_of(args.server))
    if args.non_interactive:
        raise ConnectorError("VALIDATION_ERROR", "credential",
                             "no credential source in non-interactive mode",
                             action="Use --credential-stdin, "
                                    "--credential-env or --mcp-entry.")
    return read_secret_masked(
        f"Canonical agent key for {args.server}: ")


async def run_connect(args, output: Output, root: Path) -> dict[str, object]:
    store = StateStore(paths.state_file(root))
    # 1. Identity import with /me validation and hint comparison.
    key = _read_key(args, output)
    import os as _os
    forced_file = _os.environ.get(
        "OKTO_NEXUS_CONNECTOR_VAULT", "").strip().lower() == "file"
    from ...identity.vault import vault_backend_names
    if (forced_file or not vault_backend_names()) and not \
            store.load().preferences.get("vault.fallback_file.approved"):
        if not args.non_interactive:
            approved = confirm(
                "No OS keyring is available. Store the canonical key in a "
                "restricted file inside your private state directory? "
                "This is not encryption; the OS account is the boundary.",
                non_interactive=False, default=False)
            if approved:
                store.update(lambda s: s.preferences.update(
                    {"vault.fallback_file.approved": True}))
        if not store.load().preferences.get("vault.fallback_file.approved"):
            raise ConnectorError("APPROVAL_REQUIRED", "vault",
                                 "restricted-file fallback not approved")
    vault = open_vault(paths.vault_dir(root),
                      approved_fallback=bool(store.load().preferences.get(
                          "vault.fallback_file.approved", False)))
    async with NexusHTTPClient(args.server) as http:
        identity_result = await import_identity(
            http, vault, store, key=key,
            alias=args.alias or _default_alias(args),
            agent_hint=args.agent)
        # Record/refresh the server profile from the authenticated /me.
        _remember_server(store, args.server, identity_result.server_id)
        output.line(f"identity ok: {identity_result.identity.alias} -> "
                    f"{identity_result.me_agent_id}")

        # 2. Local discovery; explicit selection only (TC-11).
        adapter_id, candidate, version = await _choose_harness(
            args, output)
        output.line(f"harness selected: {adapter_id} "
                    f"({candidate.executable}"
                    f"{', version ' + version if version else ''})")

        # 3. Resolve the project from cwd (C03.2).
        project = (args.project or Path.cwd()).resolve()
        if not project.is_dir():
            raise ConnectorError("WORKSPACE_UNAVAILABLE", "connect",
                                 f"project directory not found: {project}")

        # One aggregated confirmation (plan 2.1); human-readable preview
        # only — the structured form travels in the final result payload.
        from ...services.connect_service import aggregated_confirmation
        confirmation = aggregated_confirmation(
            server_url=args.server,
            me_agent_id=identity_result.me_agent_id,
            identity_alias=identity_result.identity.alias,
            adapter_id=adapter_id, executable=candidate.executable,
            version=version, workspace_root=str(project))
        for line in _render_confirmation(confirmation):
            output.line(line)
        if not confirm("Create this binding?", non_interactive=False,
                       default=True):
            return {"connected": False, "aborted": True,
                    "identity_alias": identity_result.identity.alias}

        # 5. Prepare/apply the binding and persist locally (idempotent).
        summary = await create_binding(
            http, store, identity=identity_result, key=key,
            alias=args.binding_alias or _binding_alias(adapter_id),
            adapter_id=adapter_id, candidate=candidate, version=version,
            workspace_root=project, server_url=args.server)
        if args.trusted_provider_home:
            store.update(lambda s: s.preferences.update(
                {f"binding.{summary.binding.binding_id}."
                 f"trusted_provider_home": True}))
    payload = summary.to_json()

    # 6. Ensure the daemon; start a runtime only when asked.
    from ...daemon import manager
    started = manager.start(root)
    payload["daemon"] = started
    try:
        client = manager.connect(root)
    except ConnectorError:
        client = None
    if client is not None:
        try:
            client.call("state.reload")
        finally:
            client.close()
    if args.start:
        from .runtime import start_runtime
        result = await start_runtime(
            output, root, alias=summary.binding.alias,
            project=None, harness=None, new_session=False, text=None)
        payload["runtime_start"] = result
    payload["connected"] = True
    output.line(f"connected: binding {summary.binding.alias} "
                f"({summary.binding.binding_id})")
    return payload


def _default_alias(args) -> str:
    return args.agent or "primary"


def _render_confirmation(confirmation: dict) -> list[str]:
    lines = ["Proposed binding:"]
    nexus = confirmation.get("nexus", {})
    harness = confirmation.get("harness", {})
    project = confirmation.get("project", {})
    lines.append(f"  agent:   {nexus.get('agent_id')} as "
                 f"{nexus.get('identity_alias')} @ {nexus.get('server_url')}")
    lines.append(f"  harness: {harness.get('adapter_id')} "
                 f"({harness.get('executable')}"
                 f"{', v' + harness.get('version') if harness.get('version') else ''})")
    lines.append(f"  project: {project.get('workspace_root')}")
    lines.append("  credentials stay in the local vault; provider login "
                 "stays on this host")
    return lines


def _binding_alias(adapter_id: str) -> str:
    return {"codex_app_server": "codex", "pi_rpc": "pi",
            "claude_stream": "claude"}.get(adapter_id, adapter_id)


async def _choose_harness(args, output: Output):
    """Discovery + selection; nothing is executed during discovery."""
    inventory = await discover_inventory()
    if args.harness:
        adapter_id = args.harness
        if adapter_id not in ADAPTERS:
            raise ConnectorError("VALIDATION_ERROR", "connect",
                                 f"unknown harness {adapter_id!r}")
    elif args.non_interactive:
        raise ConnectorError("AMBIGUOUS_BINDING", "connect",
                             "harness choice required",
                             action="Pass --harness explicitly in "
                                    "non-interactive mode.")
    else:
        options = []
        for entry in inventory:
            options.append((entry.adapter_id,
                            f"{entry.executable}"
                            f" [{entry.source}]"
                            f"{f' v{entry.version}' if entry.version else ''}"))
        if not options:
            raise ConnectorError("BINARY_NOT_FOUND", "connect",
                                 "no local harness installation found",
                                 action="Install Codex/Pi/Claude or pass "
                                        "--executable explicitly.")
        adapter_id = select("Which local harness should this agent use?",
                            options, non_interactive=False)
    if args.executable:
        if adapter_id == "pi_rpc":
            if not args.pi_node:
                raise ConnectorError("VALIDATION_ERROR", "connect",
                                     "Pi selection requires --pi-node "
                                     "(trusted Node executable)")
            candidate = pi_candidate(args.pi_node, args.executable)
        else:
            candidate = select_explicit(adapter_id, args.executable)
    else:
        matches = [e for e in inventory if e.adapter_id == adapter_id]
        if not matches:
            raise ConnectorError("BINARY_NOT_FOUND", "connect",
                                 f"no local candidate for {adapter_id}",
                                 action="Pass --executable explicitly.")
        exact = matches[0]
        candidate = select_explicit(adapter_id, exact.executable)
    version = await probe_version(candidate)
    return adapter_id, candidate, version


def _remember_server(store: StateStore, base_url: str, server_id: str) -> None:
    import time as _time
    from ...storage.state_store import ServerProfileRecord
    from ...transport.https_client import origin_of

    def _mutate(state):
        if server_id not in state.servers:
            state.servers[server_id] = ServerProfileRecord(
                server_id=server_id, base_url=base_url.rstrip("/"),
                origin=origin_of(base_url),
                added_at=_time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                        _time.gmtime()))
        else:
            existing = state.servers[server_id]
            existing.base_url = base_url.rstrip("/")
            existing.origin = origin_of(base_url)

    store.update(_mutate)
