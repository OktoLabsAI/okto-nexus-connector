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
from ...services.discovery_service import (
    discover_inventory, known_adapter, pi_candidate, probe_version,
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
    if args.non_interactive:
        raise ConnectorError('VALIDATION_ERROR', 'connect',
            'connect is an interactive R4 connection wizard.',
            action='Use connection-config apply with explicit parameters for automation.')
    store = StateStore(paths.state_file(root))
    # 1. Identity import with /me validation and hint comparison.
    key = _read_key(args, output)
    import os as _os
    forced_file = _os.environ.get(
        "OKTO_NEXUS_CONNECTOR_VAULT", "").strip().lower() == "file"
    from ...identity.vault import vault_backend_names
    # CN1/A14: --non-interactive NEVER prompts. An unapproved vault
    # fallback in headless mode is a prescriptive error, not an implicit
    # approval (test_19: no confirm call with non_interactive=False).
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
                                 "restricted-file fallback not approved",
                                 action="Pre-approve the fallback once in "
                                        "interactive mode, or install an OS "
                                        "keyring backend, before running "
                                        "non-interactive automation.")
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


    # The current Server accepts R4 registrations, realizations and binding
    # proposals. Share the durable wizard used by configure; never send the
    # obsolete executor/workspace_hint binding payload.
    from types import SimpleNamespace
    from nexus_connector_core.installation import effective_installation_ref
    from .configure import template
    from .configure_local import configure_local

    identity = identity_result.identity
    request_id = getattr(args, 'request_id', None)
    saved = store.load().preferences.get(
        f'configure.{identity.server_id}.{identity.agent_id}.{request_id}') if request_id else None
    candidate_ref = None
    portable = template()
    if not saved:
        adapter_id, candidate, version = await _choose_harness(args, output)
        output.line(f'harness selected: {adapter_id} ({candidate.executable}, version {version})')
        candidate_ref = effective_installation_ref(candidate)
        portable.update(adapter_id=adapter_id, alias=args.binding_alias or _binding_alias(adapter_id))
    wizard = SimpleNamespace(identity=identity.alias, agent=args.agent, request_id=request_id,
        candidate_ref=candidate_ref, project=str(args.project.resolve()) if args.project else None,
        workspace_label=None, provider_home=getattr(args, 'provider_home', None),
        workspace_id=None, binding_id=None,
        operator_identity=getattr(args, 'operator_identity', None),
        operator_proof_ref=getattr(args, 'operator_proof_ref', None))
    result = await configure_local(wizard, output, root, store, identity, args.server, portable)
    if result.get('saved') and args.start:
        from .runtime import start_runtime
        result['runtime_start'] = await start_runtime(output, root,
            alias=result['connection_name'], project=None, harness=None, new_session=False, text=None)
        result['runtime_started'] = True
    return result


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
    executable_choice = None
    if args.harness:
        adapter_id = args.harness
        if not known_adapter(adapter_id):
            raise ConnectorError("VALIDATION_ERROR", "connect",
                                 f"unknown harness {adapter_id!r}")
    elif args.non_interactive:
        raise ConnectorError("AMBIGUOUS_BINDING", "connect",
                             "harness choice required",
                             action="Pass --harness explicitly in "
                                    "non-interactive mode.")
    else:
        # CN1/A14+CN-01.04 (test_24): the selection value is the EXACT
        # installation — the entry's executable/ref — never a bare
        # adapter id that falls back to matches[0].
        options = []
        for entry in inventory:
            name = {"codex_app_server": "Codex", "pi_rpc": "Pi",
                    "claude_stream": "Claude Code"}.get(entry.adapter_id, entry.adapter_id)
            options.append((entry.executable,
                            f"{name} - {entry.executable}"
                            f" [{entry.source}]"
                            f"{f' v{entry.version}' if entry.version else ''}"))
        if not options:
            raise ConnectorError("BINARY_NOT_FOUND", "connect",
                                 "no local harness installation found",
                                 action="Install Codex/Pi/Claude or pass "
                                        "--executable explicitly.")
        adapter_id = None
        executable_choice = select(
            "Which local harness should this agent use?",
            options, non_interactive=args.non_interactive)
        for entry in inventory:
            if entry.executable == executable_choice:
                adapter_id = entry.adapter_id
                break
        if adapter_id is None:
            raise ConnectorError("VALIDATION_ERROR", "connect",
                                 "selected installation vanished from the "
                                 "inventory")
    executable_path = (args.executable if args.executable
                       else executable_choice)
    if executable_path is None:
        raise ConnectorError("VALIDATION_ERROR", "connect",
                             "no installation selected")
    if adapter_id == "pi_rpc":
        if not args.pi_node:
            raise ConnectorError("VALIDATION_ERROR", "connect",
                                 "Pi selection requires --pi-node "
                                 "(trusted Node executable)")
        candidate = pi_candidate(args.pi_node, executable_path)
    else:
        candidate = select_explicit(adapter_id, executable_path)
    probed = await probe_version(candidate)
    return adapter_id, probed, probed.version


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
