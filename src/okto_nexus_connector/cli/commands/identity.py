"""``identity add/list/show/remove/replace-credential`` (plan 6, C01)."""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from pathlib import Path

from ...errors import ConnectorError
from ...identity.import_flow import (
    import_identity, read_secret_env, read_secret_masked, read_secret_stdin,
    remove_identity, replace_credential,
)
from ...identity.vault import open_vault
from ...platform import paths
from ...storage.state_store import StateStore
from ...transport.https_client import NexusHTTPClient
from ..output import Output


def _vault(root: Path, store: StateStore):
    state = store.load()
    return open_vault(paths.vault_dir(root),
                      approved_fallback=bool(state.preferences.get(
                          "vault.fallback_file.approved", False)))


def _read_key(args, output: Output, *, prompt: str) -> str:
    if args.credential_stdin:
        return read_secret_stdin()
    if args.credential_env:
        return read_secret_env(args.credential_env)
    if args.non_interactive:
        raise ConnectorError("VALIDATION_ERROR", "credential",
                             "no credential source in non-interactive mode",
                             action="Use --credential-stdin or "
                                    "--credential-env.")
    return read_secret_masked(prompt)


def _approve_fallback(args, output: Output, store: StateStore) -> bool:
    state = store.load()
    if state.preferences.get("vault.fallback_file.approved"):
        return True
    if not args.non_interactive:
        from ..prompts import confirm
        approved = confirm(
            "No OS keyring is available. Store the canonical key in a "
            "restricted file inside your private state directory? "
            "This is not encryption; the OS account is the boundary.",
            non_interactive=False, default=False)
        if approved:
            store.update(lambda s: s.preferences.update(
                {"vault.fallback_file.approved": True}))
        return approved
    return False


async def run_identity(args, output: Output, root: Path):
    store = StateStore(paths.state_file(root))
    sub = args.subcommand
    if sub == "add":
        import os as _os
        from ...identity.vault import vault_backend_names
        forced_file = _os.environ.get(
            "OKTO_NEXUS_CONNECTOR_VAULT", "").strip().lower() == "file"
        if forced_file or not vault_backend_names():
            approved = _approve_fallback(args, output, store)
            if not approved:
                raise ConnectorError(
                    "APPROVAL_REQUIRED", "vault",
                    "restricted-file fallback was not approved",
                    action="Re-run interactively and approve, or install "
                           "an OS keyring backend.")
        vault = open_vault(paths.vault_dir(root),
                           approved_fallback=bool(store.load().preferences
                                                  .get("vault.fallback_"
                                                       "file.approved")))
        key = _read_key(args, output, prompt="Canonical agent key: ")
        async with NexusHTTPClient(args.server) as http:
            result = await import_identity(
                http, vault, store, key=key, alias=args.alias,
                agent_hint=args.agent)
        # Preserve the explicitly selected and authenticated Server for
        # subsequent executor registration and credential replacement.
        from .connect import _remember_server
        await asyncio.to_thread(_remember_server, store, args.server, result.server_id)
        output.line(f"identity {result.identity.alias}: agent "
                    f"{result.identity.agent_id} on {result.server_id} "
                    f"({'imported' if result.created else 're-validated'})")
        return {"alias": result.identity.alias,
                "agent_id": result.identity.agent_id,
                "server_id": result.server_id,
                "created": result.created}
    if sub == "list":
        state = store.load()
        return {"identities": [asdict(item) for item in state.identities]}
    if sub == "show":
        state = store.load()
        record = state.identity_by_alias(args.alias)
        if record is None:
            raise ConnectorError("VALIDATION_ERROR", "identity",
                                 f"unknown alias {args.alias!r}")
        return {"identity": asdict(record)}
    if sub == "remove":
        vault = _vault(root, store)
        if not args.non_interactive:
            from ..prompts import confirm
            if not confirm(f"Remove local identity {args.alias!r}? "
                           "The canonical agent stays on the Server.",
                           non_interactive=False, default=False):
                return {"removed": False, "aborted": True}
        return remove_identity(vault, store, alias=args.alias,
                               revoke_globally_hint=False)
    if sub == "replace-credential":
        vault = _vault(root, store)
        key = _read_key(args, output,
                        prompt="New canonical key (rotated): ")
        async with NexusHTTPClient(_server_url(store, args.alias)) as http:
            result = await replace_credential(
                http, vault, store, alias=args.alias, key=key)
        output.line(f"credential replaced for {result.identity.alias} "
                    f"(epoch {result.identity.credential_epoch})")
        return {"alias": result.identity.alias,
                "credential_epoch": result.identity.credential_epoch}
    raise ConnectorError("CAPABILITY_UNSUPPORTED", "identity",
                         f"unknown subcommand {sub!r}")


def _server_url(store: StateStore, alias: str) -> str:
    state = store.load()
    record = state.identity_by_alias(alias)
    if record is None:
        raise ConnectorError("VALIDATION_ERROR", "identity",
                             f"unknown alias {alias!r}")
    profile = state.servers.get(record.server_id)
    if profile is None:
        raise ConnectorError("SERVER_ID_CHANGED", "identity",
                             "server profile missing locally")
    return profile.base_url
