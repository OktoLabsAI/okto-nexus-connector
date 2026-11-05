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
    display_name, known_adapter, resolve_selection,
)
from ...services.discovery_configuration import configured_candidates
from ...storage.state_store import StateStore
from ...transport.https_client import NexusHTTPClient, origin_of
from ..output import Output
from ..prompts import confirm, select
from . import connect_reuse


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
    if args.non_interactive or getattr(args, 'json', False):
        raise ConnectorError('VALIDATION_ERROR', 'connect',
            'connect is an interactive R4 connection wizard.',
            action='Use connection-config apply with explicit parameters for automation.')
    store = StateStore(paths.state_file(root))
    state = store.load()
    retained = connect_reuse.saved_request(args, state, select)
    identity = connect_reuse.selected_identity(args, state, retained)
    if retained:
        if getattr(args, 'customize', False):
            raise ConnectorError('OPERATION_CONFLICT', 'connect',
                'This request already has reviewed settings. Resume it unchanged; use configure for an explicit configuration change.')
        args.request_id = retained[1]
        connect_reuse.validate_choices(args, retained[3], state)
    if not args.server:
        raise ConnectorError('SERVER_REQUIRED', 'connect',
            'Select --server for a new connection, or --identity for an imported identity.')
    reuse = identity is not None and not connect_reuse.credential_supplied(args)
    import os as _os
    forced_file = _os.environ.get(
        "OKTO_NEXUS_CONNECTOR_VAULT", "").strip().lower() == "file"
    # CN1/A14: --non-interactive NEVER prompts. An unapproved vault
    # fallback in headless mode is a prescriptive error, not an implicit
    # approval (test_19: no confirm call with non_interactive=False).
    if not reuse and (forced_file or not vault_backend_names()) and not \
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
    before = connect_reuse.identity_snapshot(state, identity) if reuse else None
    key = await asyncio.to_thread(vault.resolve, identity.secret_handle) if reuse else _read_key(args, output)
    current_binding = None
    async with NexusHTTPClient(args.server) as http:
        if reuse:
            me = await http.me(key)
            if (me.server_id, me.agent_id, me.credential_epoch) != (identity.server_id, identity.agent_id, identity.credential_epoch):
                raise ConnectorError('AGENT_ID_MISMATCH', 'connect',
                    'The stored credential does not match the selected identity and credential revision.')
            if connect_reuse.identity_snapshot(store.load(), identity) != before:
                raise ConnectorError('STALE_GENERATION', 'connect', 'The selected identity changed during authentication.')
        else:
            identity_result = await import_identity(
                http, vault, store, key=key,
                alias=identity.alias if identity else args.alias or _default_alias(args),
                agent_hint=args.agent)
            _remember_server(store, args.server, identity_result.server_id)
            identity = identity_result.identity
        if retained:
            if store.load().preferences.get(retained[0]) != retained[3]:
                raise ConnectorError('STALE_GENERATION', 'connect', 'The saved request changed during authentication.')
            current_binding = await connect_reuse.verify_binding(http, key, store, identity, retained)
        output.line(f"identity ok: {identity.alias} -> {identity.agent_id}")


    # The current Server accepts R4 registrations, realizations and binding
    # proposals. Share the durable wizard used by configure; never send the
    # obsolete executor/workspace_hint binding payload.
    from types import SimpleNamespace
    from nexus_connector_core.installation import effective_installation_ref
    from .configure import template
    from .configure_local import configure_local

    request_id = getattr(args, 'request_id', None)
    saved = store.load().preferences.get(
        f'configure.{identity.server_id}.{identity.agent_id}.{request_id}') if request_id else None
    candidate_ref = None
    portable = template()
    if not saved:
        records = [r for r in store.load().execution_executors if r.server_id == identity.server_id]
        if len(records) > 1:
            raise ConnectorError('PROFILE_DRIFT', 'connect', 'The local executor registration is ambiguous.')
        args.discovery_configuration = records[0].discovery_configuration if records else {}
        args.installation_observations = records[0].installation_observations if records else ()
        adapter_id, candidate, version = await _choose_harness(args, output)
        output.line(f'Harness recommendation: {display_name(adapter_id)} ({candidate.executable}'
                    + (f', {candidate.launch_script}' if getattr(candidate, 'launch_script', None) else '') + ').')
        output.line('Discovery does not confirm provider login or session readiness. Version and protocol checks run after the review is accepted.')
        candidate_ref = effective_installation_ref(candidate)
        portable.update(adapter_id=adapter_id, alias=args.binding_alias or _binding_alias(adapter_id))
    wizard = SimpleNamespace(identity=identity.alias, agent=args.agent, request_id=request_id,
        project_preset_flow=True, customize=getattr(args, 'customize', False),
        discovery_inventory_revision=getattr(args, 'discovery_inventory_revision', None),
        start_at_login=getattr(args, 'start_at_login', None),
        candidate_ref=candidate_ref, project=str((args.project or Path.cwd()).resolve()) if not saved or args.project else None,
        workspace_label=None, provider_home=getattr(args, 'provider_home', None),
        workspace_id=None, binding_id=None,
        operator_identity=getattr(args, 'operator_identity', None),
        operator_proof_ref=getattr(args, 'operator_proof_ref', None))
    result = await configure_local(wizard, output, root, store, identity, args.server, portable)
    if retained and result.get('saved'):
        from ...daemon import manager
        from . import configure_startup
        from .configure_local import ipc
        await asyncio.to_thread(manager.start, root)
        await ipc(root, 'state.reload')
        result = dict(result, reused=True, current_binding=current_binding)
        result = await configure_startup.result(root, store.load().preferences[retained[0]], result)
        output.line('Reused the saved project connection. Current execution grants and provider readiness remain subject to Nexus admission.')
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
    """Recommend one exact host candidate; executable checks belong after review."""
    from nexus_connector_core import calculate_inventory_revision
    from nexus_connector_core.installation import effective_installation_ref
    if args.harness and not known_adapter(args.harness):
        raise ConnectorError('VALIDATION_ERROR', 'connect', f'unknown harness {args.harness!r}')
    inventory = await configured_candidates(getattr(args, 'discovery_configuration', {}),
        observations=getattr(args, 'installation_observations', ()))
    candidates = [c for c in inventory if known_adapter(c.adapter_id)
                  and (not args.harness or c.adapter_id == args.harness)]
    if args.executable:
        target = Path(args.executable).expanduser().resolve()
        candidates = [c for c in candidates if Path(c.launch_script or c.executable).resolve() == target]
    if args.pi_node:
        node = Path(args.pi_node).expanduser().resolve()
        candidates = [c for c in candidates if c.adapter_id == 'pi_rpc' and Path(c.executable).resolve() == node]
    if not candidates:
        raise ConnectorError('BINARY_NOT_FOUND', 'connect', 'No matching installation is available in this host inventory.',
            action='Install a supported harness or review this executor discovery configuration. Explicit paths must select an inventoried installation.')
    choices = {}
    for candidate in candidates:
        ref = effective_installation_ref(candidate)
        # The Core resolves aliases/duplicate rows and rejects conflicting refs.
        choices[(candidate.adapter_id, ref)] = resolve_selection(candidates, candidate.adapter_id, ref)
    options = [(key, f'{display_name(c.adapter_id)} — {c.executable}'
                + (f' + {c.launch_script}' if c.launch_script else '') + f' [{c.source}]')
               for key, c in choices.items()]
    selected = options[0][0] if len(options) == 1 else select(
        'Which local harness should this project use?', options, non_interactive=args.non_interactive)
    if selected not in choices:
        raise ConnectorError('VALIDATION_ERROR', 'connect', 'Select an installation from this host inventory.')
    candidate = choices[selected]
    args.discovery_inventory_revision = calculate_inventory_revision(inventory)
    return candidate.adapter_id, candidate, candidate.version


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
