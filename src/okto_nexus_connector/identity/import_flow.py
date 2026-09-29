"""Guided canonical-identity import (plan C01.2/C01.3).

The canonical agent key already used by MCP is imported through a protected
entry: masked interactive input, ``--credential-stdin`` for automation, an
environment variable name, or an MCP entry the operator explicitly selected.
We never scan files or keychains for secrets. The key is validated against
the Server's ``/me`` under TLS/origin checks and the returned identity is
compared with the requested hint; divergence aborts without touching the
canonical registration (TC-03/TC-04).
"""

from __future__ import annotations

import getpass
import json
import os
import stat
import sys
from dataclasses import dataclass
from pathlib import Path

from ..errors import ConnectorError, agent_mismatch, auth_required
from ..storage.state_store import IdentityRecord, StateStore
from .vault import RestrictedFileVault, SecretVault, handle_for

MAX_KEY_LENGTH = 4096


@dataclass(slots=True)
class ImportResult:
    identity: IdentityRecord
    created: bool
    me_agent_id: str
    server_id: str


def read_secret_stdin() -> str:
    data = sys.stdin.readline()
    if not data.endswith("\n"):
        raise ConnectorError("VALIDATION_ERROR", "credential_stdin",
                             "credential line must end with newline")
    key = data.rstrip("\r\n")
    _check_key_shape(key)
    return key


def read_secret_masked(prompt: str = "Canonical agent key: ") -> str:
    key = getpass.getpass(prompt)
    _check_key_shape(key)
    return key


def read_secret_env(name: str) -> str:
    if not name or any(c in name for c in "\r\n\x00"):
        raise ConnectorError("VALIDATION_ERROR", "credential_env")
    value = os.environ.get(name)
    if value is None:
        raise ConnectorError("VALIDATION_ERROR", "credential_env",
                             f"environment variable {name!r} is not set",
                             action="Set it or use --credential-stdin.")
    _check_key_shape(value)
    return value


def read_secret_from_mcp_entry(path: Path, *, entry_name: str,
                               server_origin: str) -> str:
    """Read one explicitly selected MCP entry's bearer credential.

    The operator chose the file and entry name; we do not enumerate or
    guess. We verify the entry targets the expected Server origin and
    refuse to copy anything else. The original file is never modified.
    """
    try:
        info = path.lstat()
    except OSError as exc:
        raise ConnectorError("WORKSPACE_UNAVAILABLE", "mcp_entry",
                             f"cannot read {path}: {exc}") from exc
    if not stat.S_ISREG(info.st_mode):
        raise ConnectorError("PROFILE_DRIFT", "mcp_entry",
                             "selected MCP config is not a regular file")
    if info.st_size > 1024 * 1024:
        raise ConnectorError("CAPACITY_EXCEEDED", "mcp_entry")
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ConnectorError("PROFILE_DRIFT", "mcp_entry",
                             f"invalid JSON: {exc}") from exc
    servers = document.get("mcpServers", document) if isinstance(
        document, dict) else {}
    if not isinstance(servers, dict) or entry_name not in servers:
        raise ConnectorError("VALIDATION_ERROR", "mcp_entry",
                             f"entry {entry_name!r} not found",
                             action="Choose an existing entry explicitly.")
    entry = servers[entry_name]
    url = entry.get("url") if isinstance(entry, dict) else None
    # CN1/A06 (test_12): the origin comparison is the EXACT tuple
    # (scheme, host, port) — a textual prefix would accept
    # https://nexus.example.attacker.invalid as https://nexus.example.
    entry_origin = _origin_tuple(url) if isinstance(url, str) else None
    expected_origin = _origin_tuple(server_origin)
    if entry_origin is None or entry_origin != expected_origin:
        raise ConnectorError(
            "PROFILE_DRIFT", "mcp_entry",
            "the selected entry does not target the expected Server origin",
            action="Pick the entry that points at the approved Nexus "
                   "Server; other entries are not imported.")
    token = (entry.get("headers", {}).get("Authorization", "")
             if isinstance(entry.get("headers"), dict) else "")
    if isinstance(token, str) and token.startswith("Bearer "):
        token = token[len("Bearer "):].strip()
    if token.startswith("${") and token.endswith("}"):
        env_name = token[2:-1]
        return read_secret_env(env_name)
    env_name = entry.get("bearer_token_env_var") if isinstance(
        entry, dict) else None
    if isinstance(env_name, str) and env_name:
        return read_secret_env(env_name)
    if not isinstance(token, str) or not token:
        raise ConnectorError("PROVIDER_AUTH_REQUIRED", "mcp_entry",
                             "selected entry has no bearer credential",
                             action="Provide the key via --credential-stdin.")
    _check_key_shape(token)
    return token


def _origin_tuple(url: str) -> tuple[str, str, int | None] | None:
    """Exact (scheme, host, default-port-stripped) tuple or None."""
    from urllib.parse import urlsplit
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return None
    if parts.username or parts.password or "@" in parts.netloc:
        return None
    port = parts.port
    if (parts.scheme == "https" and port == 443) or \
            (parts.scheme == "http" and port == 80):
        port = None
    return (parts.scheme, parts.hostname.lower(), port)


def _check_key_shape(key: str) -> None:
    if (not key or len(key) > MAX_KEY_LENGTH or "\x00" in key
            or any(c in key for c in "\r\n")
            or key.startswith(("mcp-cap:", "tkt_", "nstkt_"))):
        raise ConnectorError("VALIDATION_ERROR", "credential",
                             "credential has an invalid shape",
                             action="Paste the canonical agent key exactly "
                                    "as issued by the Server.")


async def import_identity(http, vault: SecretVault, store: StateStore,
                          *, key: str, alias: str,
                          agent_hint: str | None) -> ImportResult:
    """Validate a key against ``/me`` and store it namespaced.

    ``http`` is an open ``NexusHTTPClient``. The identity recorded locally
    is the one the Server derives from the credential; a hint mismatch
    aborts (plan A.4.1). Re-importing the same key reuses the record and
    never creates remote state.
    """
    try:
        me = await http.me(key)
    except ConnectorError as exc:
        if exc.code == "AGENT_AUTH_REQUIRED":
            raise auth_required("me", "the Server rejected this key") from exc
        raise
    if agent_hint and me.agent_id != agent_hint:
        raise agent_mismatch(
            "me",
            f"hint {agent_hint} but key authenticates as {me.agent_id}")
    existing = None

    def _mutate(state):
        nonlocal existing
        _ensure_connector_id(state)
        if state.identity_by_alias(alias) is not None:
            other = state.identity_by_alias(alias)
            if other.server_id != me.server_id or \
                    other.agent_id != me.agent_id:
                raise ConnectorError(
                    "AMBIGUOUS_BINDING", "identity",
                    f"alias {alias!r} is already used by "
                    f"{other.server_id}/{other.agent_id}",
                    action="Choose another local alias with --alias.")
        existing = state.identity_for(me.server_id, me.agent_id)

    store.update(_mutate)
    namespace = f"{me.server_id}/{me.agent_id}"
    if existing is None:
        handle = vault.store(namespace, key)
        identity = IdentityRecord(
            alias=alias, server_id=me.server_id, agent_id=me.agent_id,
            secret_handle=handle, credential_epoch=1,
            added_at=_now(), display_name=me.display_name)

        def _append(state_):
            if state_.identity_for(me.server_id, me.agent_id) is None:
                state_.identities.append(identity)

        store.update(_append)
        return ImportResult(identity, True, me.agent_id, me.server_id)
    if existing.alias != alias:
        raise ConnectorError(
            "AMBIGUOUS_BINDING", "identity",
            f"this identity is already imported as {existing.alias!r}",
            action=f"Use 'identity show {existing.alias}' or remove it first.")
    vault.replace(namespace, key)
    identity = existing
    if identity.revoked:
        def _clear(state):
            record = state.identity_for(me.server_id, me.agent_id)
            if record is not None:
                record.revoked = False
        store.update(_clear)
        identity.revoked = False
    return ImportResult(identity, False, me.agent_id, me.server_id)


async def replace_credential(http, vault: SecretVault, store: StateStore,
                            *, alias: str, key: str) -> ImportResult:
    """Substitute a rotated key in the correct identity (plan C01.4)."""
    state = store.load()
    record = state.identity_by_alias(alias)
    if record is None:
        raise ConnectorError("VALIDATION_ERROR", "identity",
                             f"unknown identity alias {alias!r}")
    me = await http.me(key)
    if me.agent_id != record.agent_id or me.server_id != record.server_id:
        raise agent_mismatch(
            "replace-credential",
            f"new key authenticates as {me.server_id}/{me.agent_id}, "
            f"but alias {alias!r} is {record.server_id}/{record.agent_id}")

    def _mutate(state_):
        target = state_.identity_for(record.server_id, record.agent_id)
        if target is not None:
            target.credential_epoch += 1
            target.revoked = False

    vault.replace(f"{record.server_id}/{record.agent_id}", key)
    store.update(_mutate)
    record.credential_epoch += 1
    record.revoked = False
    return ImportResult(record, False, me.agent_id, me.server_id)


def remove_identity(vault: SecretVault, store: StateStore, *, alias: str,
                    revoke_globally_hint: bool) -> dict[str, object]:
    """Local removal is distinct from central revocation (plan 5.1)."""
    state = store.load()
    record = state.identity_by_alias(alias)
    if record is None:
        raise ConnectorError("VALIDATION_ERROR", "identity",
                             f"unknown identity alias {alias!r}")

    def _mutate(state_):
        state_.identities = [
            item for item in state_.identities
            if not (item.server_id == record.server_id
                    and item.agent_id == record.agent_id)]
        state_.bindings = [
            item for item in state_.bindings
            if not (item.server_id == record.server_id
                    and item.agent_id == record.agent_id)]

    store.update(_mutate)
    vault.remove(f"{record.server_id}/{record.agent_id}")
    return {
        "alias": alias,
        "server_id": record.server_id,
        "agent_id": record.agent_id,
        "bindings_removed": len(state.bindings_for_agent(
            record.server_id, record.agent_id)),
        "revoked_globally": False,
        "note": ("Local references and the stored key were removed. The "
                 "canonical agent still exists on the Server; revoking it "
                 "centrally is a separate authorized Server operation."
                 if not revoke_globally_hint else
                 "Use the Server's authorized key-management flow to "
                 "revoke the canonical credential."),
    }


def _ensure_connector_id(state) -> None:
    if not state.connector_id:
        import uuid
        state.connector_id = f"conn_{uuid.uuid4().hex[:24]}"


def _now() -> str:
    import time
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
