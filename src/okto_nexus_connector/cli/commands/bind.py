"""``bind`` — advanced binding management without touching identity."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from ...errors import ConnectorError
from ...identity.import_flow import read_secret_env, read_secret_masked, read_secret_stdin
from ...platform import paths
from ...services.connect_service import create_binding
from ...services.discovery_service import pi_candidate, probe_version, select_explicit
from ...storage.state_store import StateStore
from ...transport.https_client import NexusHTTPClient
from ..output import Output


async def run_bind(args, output: Output, root: Path):
    store = StateStore(paths.state_file(root))
    sub = args.subcommand
    if sub in ("prepare", "apply"):
        from .identity import _vault
        from ...services.binding_onboarding import BindingOnboarding
        service = BindingOnboarding(store, _vault(root, store))
        if sub == "prepare":
            result = await service.prepare(identity_alias=args.identity,
                realization_ref=args.realization_ref, alias=args.alias, client_intent_id=args.client_intent_id)
            output.line("Binding proposal prepared. Review the diff and obtain any required operator approval.")
            return result
        result = await service.apply(identity_alias=args.identity, prepare_intent_id=args.prepare_intent_id,
            client_intent_id=args.client_intent_id, approved_diff_hash=args.approved_diff_hash,
            operator_proof_ref=args.operator_proof_ref)
        output.line("Binding acknowledged. Runtime execution still requires Server authorization and an applied lease.")
        return result
    if sub == "create":
        state = store.load()
        identity = state.identity_by_alias(args.identity)
        if identity is None:
            raise ConnectorError("VALIDATION_ERROR", "bind",
                                 f"unknown identity {args.identity!r}",
                                 action="Import it first with 'identity add'.")
        profile = state.servers.get(identity.server_id)
        if profile is None:
            raise ConnectorError("SERVER_ID_CHANGED", "bind",
                                 "server profile missing")
        if args.pi_node:
            candidate = pi_candidate(args.pi_node, args.executable)
        else:
            candidate = select_explicit(args.harness, args.executable)
        candidate = await probe_version(candidate)
        version = candidate.version
        project = (args.project or Path.cwd()).resolve()
        if args.non_interactive:
            key = (read_secret_stdin() if sys_stdin_piped()
                   else read_secret_env("OKTO_NEXUS_CONNECTOR_KEY"))
        else:
            key = read_secret_masked(
                f"Canonical key for {identity.alias}: ")
        from ...identity.vault import open_vault
        vault = open_vault(paths.vault_dir(root),
                           approved_fallback=bool(state.preferences.get(
                               "vault.fallback_file.approved", False)))
        # CN1/A14 (test_25): use the REAL ImportResult DTO — the old
        # local stub class body referenced `identity` before assignment
        # (NameError) and never reached the service.
        from ...identity.import_flow import ImportResult
        identity_result = ImportResult(
            identity=identity, created=False,
            me_agent_id=identity.agent_id, server_id=identity.server_id)
        async with NexusHTTPClient(profile.base_url) as http:
            summary = await create_binding(
                http, store, identity=identity_result, key=key,
                alias=args.alias, adapter_id=args.harness,
                candidate=candidate, version=version,
                workspace_root=project, server_url=profile.base_url)
        output.line(f"binding {summary.binding.alias} created")
        return summary.to_json()
    if sub == "list":
        state = store.load()
        from ...services.binding_onboarding import BindingOnboarding
        service = BindingOnboarding(store, None)
        return {"bindings": [asdict(item) for item in state.bindings],
                "execution_bindings": [service._view(state, item) for item in state.binding_intents]}
    if sub == "show":
        state = store.load()
        from ...services.binding_onboarding import BindingOnboarding
        records = [r for r in state.binding_intents if r.alias == args.alias]
        if records:
            if len(records) != 1 or state.binding_by_alias(args.alias) is not None:
                raise ConnectorError("OPERATION_CONFLICT", "bind", "The binding alias is ambiguous.")
            return BindingOnboarding(store, None)._view(state, records[0])
        binding = state.binding_by_alias(args.alias)
        if binding is None:
            raise ConnectorError("VALIDATION_ERROR", "bind",
                                 f"unknown binding {args.alias!r}")
        return {"binding": asdict(binding)}
    if sub == "remove":
        result = _remove_binding(store, args.alias, keep_config=args.keep_config)
        output.line(f"binding {args.alias} removed "
                    f"(local; canonical identity untouched)")
        return result
    raise ConnectorError("CAPABILITY_UNSUPPORTED", "bind",
                         f"unknown subcommand {sub!r}")


def _remove_binding(store: StateStore, alias: str, *, keep_config: bool):
    state = store.load()
    binding = state.binding_by_alias(alias)
    if binding is None:
        raise ConnectorError("VALIDATION_ERROR", "bind",
                             f"unknown binding {alias!r}")
    removed_config = None
    if not keep_config:
        removed_config = _try_remove_owned_config(state, binding)

    def _mutate(state_):
        state_.bindings = [b for b in state_.bindings
                           if b.binding_id != binding.binding_id]
        state_.preferences.pop(
            f"binding.{binding.binding_id}.trusted_provider_home", None)

    store.update(_mutate)
    return {
        "alias": alias,
        "binding_id": binding.binding_id,
        "config_entry_removed": removed_config,
        "note": "local binding removed; the canonical agent, its key and "
                "other bindings are untouched",
    }


def _try_remove_owned_config(state, binding):
    """Remove the product-owned direct-HTTP MCP entry if present."""
    from ...services.mcp_config_service import remove_persistent_entry
    from pathlib import Path
    import sys
    if sys.platform == "win32":
        targets = {
            "codex_app_server": Path.home() / ".codex" / "config.toml",
            "claude_stream": Path.home() / ".claude" / "settings.json",
        }
    else:
        targets = {
            "codex_app_server": Path.home() / ".codex" / "config.toml",
            "claude_stream": Path.home() / ".claude" / "settings.json",
        }
    target = targets.get(binding.adapter_id)
    if target is None or not target.exists():
        return None
    owned = state.preferences.get(
        f"binding.{binding.binding_id}.mcp_owned_entry")
    try:
        result = remove_persistent_entry(
            binding.adapter_id, target,
            entry_name=binding.mcp_entry_name,
            previously_owned=owned if isinstance(owned, dict) else None)
        return result
    except Exception:
        return {"changed": False,
                "note": "owned entry not removed; remove it manually after "
                        "verifying ownership"}


def sys_stdin_piped() -> bool:
    import sys
    return not sys.stdin.isatty()
