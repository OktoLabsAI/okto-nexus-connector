"""``doctor`` — prescriptive layered diagnostics (plan C07.5, TC-30).

Checks per layer: environment, Core/contract, state/vault, identities and
Server profiles (optional live probe), binaries, journal, IPC/daemon,
transports. Diagnostics never suggest disabling TLS or the sandbox, and
inspecting state has no side effects.
"""

from __future__ import annotations

import platform
import sys
import time
from pathlib import Path

from ... import __version__
from ...daemon import manager
from ...errors import ConnectorError
from ...identity.vault import open_vault, vault_backend_names
from ...platform import paths
from ...services.discovery_service import discover_inventory
from ...storage.state_store import StateStore
from ...transport.https_client import NexusHTTPClient
from ..output import Output


async def run_doctor(args, output: Output, root: Path):
    checks: list[dict[str, object]] = []

    def check(layer: str, name: str, status: str, detail: str = "",
              action: str = "") -> None:
        checks.append({"layer": layer, "name": name, "status": status,
                       "detail": detail, "action": action})

    # environment
    ok = sys.version_info >= (3, 11)
    check("environment", "python",
          "ok" if ok else "fail",
          f"{sys.version.split()[0]} on {platform.system()} "
          f"{platform.release()}" if ok else
          f"Python {sys.version.split()[0]} is too old",
          "" if ok else "install Python 3.11 or newer")
    state_dir_ok = root.is_dir()
    check("environment", "state-dir",
          "ok" if state_dir_ok else "fail", str(root),
          "" if state_dir_ok else
          f"create {root} or set OKTO_NEXUS_CONNECTOR_STATE")

    # core
    try:
        import nexus_connector_core as core
        check("core", "version", "ok", core.__version__)
        check("core", "contract", "ok", core.CONTRACT_REVISION)
    except Exception as exc:
        check("core", "import", "fail", str(exc),
              "reinstall the okto-nexus-connector distribution")

    # state & vault
    store = StateStore(paths.state_file(root))
    try:
        state = store.load()
        check("state", "schema", "ok", f"v{state.schema_version}")
        check("state", "connector_id", "ok" if state.connector_id else
              "unknown", state.connector_id or "not initialized yet")
    except ConnectorError as error:
        check("state", "load", "fail", error.message, error.action or "")
        state = None
    names = vault_backend_names()
    if names:
        check("vault", "backend", "ok", names[0])
    else:
        approved = bool(state and state.preferences.get(
            "vault.fallback_file.approved"))
        check("vault", "backend", "ok" if approved else "warn",
              "restricted-file (approved)" if approved else
              "no OS keyring; restricted-file fallback not approved",
              "" if approved else
              "approve the restricted-file fallback during 'identity add' "
              "or install an OS keyring backend")
    try:
        vault = open_vault(paths.vault_dir(root),
                           approved_fallback=bool(state and
                           state.preferences.get("vault.fallback_file."
                                                 "approved", False)))
        check("vault", "namespaces", "ok",
              str(len(vault.list_namespaces())))
    except ConnectorError as error:
        check("vault", "status", "fail", error.message, error.action or "")

    # identities & servers
    if state is not None:
        check("identity", "imported", "ok" if state.identities else "warn",
              f"{len(state.identities)} identitie(s)",
              "" if state.identities else
              "run 'connect' or 'identity add' to import a canonical key")
        for record in state.servers.values():
            detail = f"{record.base_url} ({record.server_id})"
            if args.probe:
                status, message, action = await _probe_server(record, state,
                                                              store)
                check("server", record.server_id, status,
                      f"{detail} — {message}", action)
            else:
                check("server", record.server_id, "unknown", detail,
                      "run with --probe for a live check")
        check("binding", "count", "ok" if state.bindings else "warn",
              str(len(state.bindings)),
              "" if state.bindings else "run 'connect' to create one")
        for binding in state.bindings:
            executable = Path(binding.candidate_executable)
            if not executable.exists():
                check("binary", binding.alias, "fail",
                      f"missing {executable}",
                      "re-run 'discover' and rebind")
            elif args.probe:
                from ...services.discovery_service import select_explicit
                try:
                    candidate = select_explicit(binding.adapter_id,
                                                executable)
                    check("binary", binding.alias, "ok",
                          f"{candidate.executable} "
                          f"{candidate.fingerprint[:19]}… "
                          f"build={candidate.build_identity[:19]}…"
                          if candidate.build_identity else
                          f"{candidate.executable} "
                          f"{candidate.fingerprint[:19]}…")
                except ConnectorError as error:
                    check("binary", binding.alias, "fail", error.message,
                          error.action or "")
            else:
                check("binary", binding.alias, "unknown",
                      str(executable), "probe with --probe")

    # inventory
    inventory = await discover_inventory()
    check("discovery", "candidates", "ok" if inventory else "warn",
          f"{len(inventory)} found",
          "" if inventory else "install Codex/Pi/Claude locally or pass "
          "--executable explicitly")

    # containment preflight (Core 0.2.0 / PC11): productive launches
    # refuse closed when the backend cannot honor its contract
    try:
        from ...services.discovery_service import containment_status
        preflight = containment_status()
        missing = [f"{name}: {detail}" for name, detail in
                   preflight.items() if detail != "ok"]
        check("containment", "preflight",
              "ok" if not missing else "fail",
              "; ".join(missing) if missing else
              ", ".join(f"{k}=ok" for k in preflight),
              "" if not missing else
              "managed launches will be refused "
              "(PROCESS_CONTAINMENT_UNAVAILABLE); run the daemon on a "
              "platform with a qualified containment backend")
    except Exception as exc:
        check("containment", "preflight", "unknown", str(exc))

    # journal
    try:
        journal_path = paths.journal_path(root)
        if journal_path.exists():
            size = journal_path.stat().st_size
            level = "ok" if size < 200 * 1024 * 1024 else "warn"
            check("journal", "size", level, f"{size} bytes",
                  "" if level == "ok" else
                  "run 'daemon stop' then compact or archive the journal")
        else:
            check("journal", "size", "unknown", "not created yet")
    except OSError as exc:
        check("journal", "size", "fail", str(exc))

    # daemon & transports
    current = manager.status(root)
    check("daemon", "process", "ok" if current.running else "warn",
          f"pid {current.pid}" if current.pid else "not running",
          "" if current.running else "run 'daemon start'")
    if current.running:
        try:
            client = manager.connect(root)
            try:
                response = client.call("status")
                result = response.get("result", {})
                if isinstance(result, dict):
                    transports = result.get("transports", {})
                    for server_id, stats in (transports.items() if
                                             isinstance(transports, dict)
                                             else ()):
                        state_name = stats.get("state", "unknown") if \
                            isinstance(stats, dict) else "unknown"
                        check("transport", server_id,
                              "ok" if state_name == "online" else "warn",
                              str(state_name),
                              "" if state_name == "online" else
                              "check Server availability; reconnect is "
                              "automatic with backoff")
            finally:
                client.close()
        except (ConnectorError, OSError) as exc:
            check("daemon", "ipc", "fail", str(exc))
    else:
        check("transport", "status", "unknown", "daemon not running")

    passed = sum(1 for c in checks if c["status"] == "ok")
    failed = sum(1 for c in checks if c["status"] == "fail")
    warned = sum(1 for c in checks if c["status"] == "warn")
    summary = {"checks": checks, "summary": {
        "ok": passed, "warn": warned, "fail": failed,
        "version": __version__}}
    output.line(f"doctor: {passed} ok, {warned} warn, {failed} fail")
    return summary


async def _probe_server(record, state, store):
    identity = next((i for i in state.identities
                     if i.server_id == record.server_id), None)
    if identity is None:
        return "warn", "no imported identity", "run 'identity add'"
    try:
        vault = open_vault(paths.vault_dir(root),
                           approved_fallback=True)
        key = vault.resolve(identity.secret_handle)
    except ConnectorError as error:
        return "warn", f"vault: {error.message}", error.action or ""
    try:
        async with NexusHTTPClient(record.base_url) as http:
            me = await http.me(key)
        if me.agent_id != identity.agent_id:
            return "fail", f"key now authenticates {me.agent_id}", \
                "run 'identity replace-credential' after confirming the " \
                "rotation"
        return "ok", f"/me {me.agent_id}", ""
    except ConnectorError as error:
        return "fail", f"{error.code}: {error.message}", error.action or ""
