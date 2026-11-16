"""``doctor`` — prescriptive layered diagnostics (plan C07.5, TC-30).

Checks per layer: environment, Core/contract, state/vault, identities and
Server profiles (optional live probe), binaries, journal, IPC/daemon,
transports. Diagnostics never suggest disabling TLS or the sandbox, and
inspecting state has no side effects.
"""

from __future__ import annotations

import platform
import sys
from pathlib import Path

from ... import __version__
from ...daemon import manager
from ...errors import ConnectorError
from ...identity.vault import open_vault, vault_backend_names
from ...platform import paths
from ...redaction import redact_mapping
from ...services.discovery_service import discover_inventory
from ...storage.state_store import StateStore
from ...transport.https_client import NexusHTTPClient
from ..output import Output
from ..diagnostic_report import DiagnosticReport, subject


async def run_doctor(args, output: Output, root: Path):
    report = DiagnosticReport(probe=args.probe) if getattr(args, 'diagnostic_format', 'legacy') == 'v1' else None
    try:
        return await _run_doctor(args, output, root, report)
    except Exception:
        if report is None:
            raise
        # An unexpected observation failure must not export exception text or
        # fabricate a failed prerequisite. Preserve already observed facts.
        report.set('CHECK_UNCONFIRMED')
        return report.result()


async def _run_doctor(args, output: Output, root: Path, report):
    checks: list[dict[str, object]] = []

    def check(layer: str, name: str, status: str, detail: str = "",
              action: str = "", *, code=None, scope=None, evidence=None) -> None:
        checks.append({"layer": layer, "name": name, "status": status,
                       "detail": detail, "action": action})
        if report is not None and code is not None:
            report.set(code, scope=scope, evidence=evidence)

    if report is not None:
        for code in ('RUNTIME_UNCONFIRMED', 'PROVIDER_LOGIN_NOT_CHECKED',
                     'MCP_NEGOTIATION_NOT_OBSERVED', 'PARTICIPATION_NOT_OBSERVED',
                     'HANDOFF_TEST_NOT_OBSERVED', 'WORK_NOT_CHECKED'):
            report.set(code)

    # environment
    ok = sys.version_info >= (3, 11)
    check("environment", "python",
          "ok" if ok else "fail",
          f"{sys.version.split()[0]} on {platform.system()} "
          f"{platform.release()}" if ok else
          f"Python {sys.version.split()[0]} is too old",
          "" if ok else "install Python 3.11 or newer",
          code='PYTHON_SUPPORTED' if ok else 'PYTHON_UNSUPPORTED')
    state_dir_ok = root.is_dir()
    check("environment", "state-dir",
          "ok" if state_dir_ok else "fail", str(root),
          "" if state_dir_ok else
          "not initialized; run 'connect' when ready to configure this instance",
          code='STATE_DIRECTORY_PRESENT' if state_dir_ok else 'STATE_DIRECTORY_ABSENT')

    # core
    try:
        import nexus_connector_core as core
        check("core", "version", "ok", core.__version__, code='CORE_IMPORTED')
        check("core", "legacy_contract", "ok", core.CONTRACT_REVISION, code='LEGACY_CONTRACT_REPORTED')
        revision = getattr(core, 'R4_CONTRACT_REVISION', None)
        executable = getattr(core, 'R4_BUNDLE_EXECUTABLE', False) is True
        verify = getattr(core, 'verify_r4_bundle', None)
        try:
            if not revision or not executable or not callable(verify):
                raise ValueError('No executable R4 contract')
            verify()
        except Exception:
            check('core', 'contract', 'fail', 'R4 bundle unavailable or invalid',
                  'Install the approved Connector and its exact pinned Core artifact.', code='CORE_CONTRACT_INVALID')
        else:
            check('core', 'contract', 'ok', revision, code='CORE_CONTRACT_VERIFIED')
            check('core', 'execution_scope', 'ok',
                  'R4 bundle verified; provider qualification, binding, authorization and applied lease are separate.')
    except Exception as exc:
        check("core", "import", "fail", str(exc),
              "reinstall the okto-nexus-connector distribution", code='CORE_IMPORT_FAILED')

    # state & vault
    store = StateStore(paths.state_file(root))
    try:
        state = store.read_snapshot()
        check("state", "schema", "ok", f"v{state.schema_version}",
              code='STATE_SCHEMA_COMPATIBLE', evidence={'schema_version': state.schema_version})
        check("state", "connector_id", "ok" if state.connector_id else
              "unknown", state.connector_id or "not initialized yet",
              code='CONNECTOR_ID_PRESENT' if state.connector_id else 'CONNECTOR_ID_ABSENT')
    except ConnectorError as error:
        check("state", "load", "fail", error.message, error.action or "", code={
            'STATE_INVALID': 'STATE_INVALID', 'VERSION_INCOMPATIBLE': 'STATE_VERSION_INCOMPATIBLE',
            'CAPACITY_EXCEEDED': 'DIAGNOSTIC_LIMIT'}.get(error.code, 'STATE_UNAVAILABLE'))
        state = None
    names = vault_backend_names()
    if names:
        check("vault", "backend", "ok", names[0], code='VAULT_BACKEND_DISCOVERED')
    else:
        approved = bool(state and state.preferences.get(
            "vault.fallback_file.approved"))
        check("vault", "backend", "ok" if approved else "warn",
              "restricted-file (approved)" if approved else
              "no OS keyring; restricted-file fallback not approved",
              "" if approved else
              "approve the restricted-file fallback during 'identity add' "
              "or install an OS keyring backend",
              code='VAULT_FALLBACK_APPROVED' if approved else 'VAULT_FALLBACK_NOT_APPROVED')
    try:
        vault = open_vault(root / 'vault',
                           approved_fallback=bool(state and
                           state.preferences.get("vault.fallback_file."
                                                 "approved", False)))
        # Keyrings cannot enumerate namespaces; their empty list proves nothing
        # about credential availability. File enumeration does not unlock keys.
        if vault.backend_name() == 'restricted-file':
            exists = (root / 'vault' / 'secrets.json').is_file()
            count = len(vault.list_namespaces())
            check("vault", "namespaces", "ok" if exists else "unknown", str(count),
                  code='VAULT_NAMESPACE_SNAPSHOT_READ' if exists else 'VAULT_ENUMERATION_UNCONFIRMED',
                  evidence={'count': count})
        else:
            check("vault", "namespaces", "unknown", "OS keyring enumeration unavailable", code='VAULT_ENUMERATION_UNCONFIRMED')
    except ConnectorError as error:
        check("vault", "status", "fail", error.message, error.action or "", code='VAULT_ACCESS_UNCONFIRMED')
    except (OSError, ValueError, TypeError) as error:
        check("vault", "status", "fail", type(error).__name__,
              "inspect access and format of the existing vault; no changes were made",
              code='VAULT_ACCESS_UNCONFIRMED' if isinstance(error, OSError) else 'VAULT_SNAPSHOT_INVALID')

    # identities & servers
    if state is not None:
        check("identity", "imported", "ok" if state.identities else "warn",
              f"{len(state.identities)} identitie(s)",
              "" if state.identities else
              "run 'connect' or 'identity add' to import a canonical key",
              code='IDENTITY_METADATA_PRESENT' if state.identities else 'IDENTITY_NOT_SELECTED',
              evidence={'count': len(state.identities)})
        for index, record in enumerate(state.servers.values(), 1):
            scope = subject('server', record.server_id, fallback=f'server-{index}')
            detail = f"{record.base_url} ({record.server_id})"
            if args.probe:
                status, message, action = await _probe_server(record, state,
                                                  root, report=report, scope=scope)
                check("server", record.server_id, status,
                      f"{detail} — {message}", action)
            else:
                if report is not None:
                    report.server_pending(scope)
                check("server", record.server_id, "unknown", detail,
                      "run with --probe for a live check")
        check("binding", "count", "ok" if state.bindings else "warn",
              str(len(state.bindings)),
              "" if state.bindings else "run 'connect' to create one", code='BINDING_NOT_CHECKED',
              evidence={'legacy_binding_count': len(state.bindings)})
        for index, binding in enumerate(state.bindings, 1):
            scope = subject('binding', binding.alias, fallback=f'binding-{index}')
            executable = Path(binding.candidate_executable)
            if not executable.exists():
                check("binary", binding.alias, "fail",
                      f"missing {executable}",
                      "re-run 'discover' and rebind", code='EXECUTABLE_MISSING', scope=scope)
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
                          f"{candidate.fingerprint[:19]}…", code='BINARY_CANDIDATE_VALIDATED', scope=scope)
                except ConnectorError as error:
                    check("binary", binding.alias, "fail", error.message,
                          error.action or "", code='BINARY_CANDIDATE_INVALID', scope=scope)
            else:
                check("binary", binding.alias, "unknown",
                      str(executable), "probe with --probe", code='BINARY_CHECK_NOT_REQUESTED', scope=scope)

    # inventory
    inventory = await discover_inventory()
    check("discovery", "candidates", "ok" if inventory else "warn",
          f"{len(inventory)} found",
          "" if inventory else "install Codex/Pi/Claude locally or pass "
          "--executable explicitly", code='INSTALLATION_CANDIDATES_PRESENT' if inventory else 'INSTALLATION_UNCONFIRMED',
          evidence={'count': len(inventory)})

    # containment preflight (Core 0.2.0 / PC11): productive launches
    # refuse closed when the backend cannot honor its contract
    try:
        from ...services.discovery_service import containment_status
        preflight = containment_status()
        missing = [f"{name}: {detail}" for name, detail in
                   preflight.items() if detail != "ok"]
        unsupported = "platform" in preflight and preflight["platform"] != "ok"
        check("containment", "preflight",
              "unsupported" if unsupported else "ok" if not missing else "fail",
              "UNSUPPORTED_PLATFORM: " + "; ".join(missing) if unsupported else
              "; ".join(missing) if missing else
              ", ".join(f"{k}=ok" for k in preflight),
              "" if not missing else
              "managed launches will be refused "
              "(PROCESS_CONTAINMENT_UNAVAILABLE); run the daemon on a "
              "Windows/Linux executor with successful containment preflight. "
              "See https://github.com/OktoLabsAI/okto-nexus-connector/blob/feature/v0.2.0/docs/runbook.md#unsupported-executor-platform",
              code='CONTAINMENT_UNCONFIRMED' if not preflight else 'UNSUPPORTED_PLATFORM' if unsupported else 'PROCESS_CONTAINMENT_UNAVAILABLE' if missing else 'CONTAINMENT_PREFLIGHT_CONFIRMED',
              evidence={'support': 'unsupported' if unsupported else 'supported'})
    except Exception as exc:
        check("containment", "preflight", "unknown", str(exc), code='CONTAINMENT_UNCONFIRMED')

    # journal
    try:
        journal_path = paths.journal_path(root)
        if journal_path.exists():
            size = journal_path.stat().st_size
            level = "ok" if size < 200 * 1024 * 1024 else "warn"
            check("journal", "size", level, f"{size} bytes",
                  "" if level == "ok" else
                  "run 'daemon stop' then compact or archive the journal",
                  code='JOURNAL_SIZE_OBSERVED' if level == 'ok' else 'JOURNAL_SIZE_HIGH', evidence={'bytes': size})
        else:
            check("journal", "size", "unknown", "not created yet", code='JOURNAL_NOT_OBSERVED')
    except OSError as exc:
        check("journal", "size", "fail", str(exc), code='JOURNAL_UNAVAILABLE')

    # daemon & transports
    current = manager.status(root)
    check("daemon", "process", "ok" if current.ipc_ok else "warn",
          current.state + (f"; pid {current.pid}" if current.pid else '') +
          (f"; {current.reason}" if current.reason else ''),
          current.next_action or ("run 'daemon start'" if current.state == 'stopped' else
          "" if current.ipc_ok else "inspect daemon status and logs; ownership or readiness is unconfirmed"),
          code='SERVICE_READY' if current.ipc_ok and current.state == 'ready' else 'SERVICE_NOT_READY' if current.state in {'stopped', 'failed'} else 'SERVICE_UNCONFIRMED',
          evidence={'state': current.state})
    if current.ipc_ok:
        try:
            client = manager.connect(root)
            try:
                response = client.call("status")
                result = response.get("result", {})
                if isinstance(result, dict):
                    transports = result.get("transports", {})
                    for index, (server_id, stats) in enumerate((transports.items() if
                                             isinstance(transports, dict)
                                             else ()), 1):
                        state_name = stats.get("state", "unknown") if \
                            isinstance(stats, dict) else "unknown"
                        check("transport", server_id,
                              "ok" if state_name == "online" else "warn",
                              str(state_name),
                              "" if state_name == "online" else
                              "check Server availability; reconnect is "
                              "automatic with backoff", code='TRANSPORT_ONLINE' if state_name == 'online' else 'TRANSPORT_UNCONFIRMED',
                              scope=subject('server', server_id, fallback=f'transport-{index}'), evidence={'state': state_name})
            finally:
                client.close()
        except (ConnectorError, OSError) as exc:
            check("daemon", "ipc", "fail", str(exc), code='SERVICE_IPC_UNCONFIRMED')
    else:
        check("transport", "status", "unknown", f"daemon {current.state}; transport not observed", code='TRANSPORT_NOT_OBSERVED')

    if report is not None:
        return report.result()

    passed = sum(1 for c in checks if c["status"] == "ok")
    failed = sum(1 for c in checks if c["status"] == "fail")
    warned = sum(1 for c in checks if c["status"] == "warn")
    unsupported = sum(1 for c in checks if c["status"] == "unsupported")
    summary = {"checks": checks, "summary": {
        "ok": passed, "warn": warned, "fail": failed, "unsupported": unsupported,
        "version": __version__}}
    output.line(f"doctor: {passed} ok, {warned} warn, {failed} fail, {unsupported} unsupported")
    return redact_mapping(summary)


async def _probe_server(record, state, root, *, report=None, scope=None):
    scope = scope or subject('server', record.server_id)
    if report is not None:
        report.server_pending(scope)
    identity = next((i for i in state.identities
                     if i.server_id == record.server_id), None)
    if identity is None:
        return "warn", "no imported identity", "run 'identity add'"
    try:
        vault = open_vault(root / 'vault', approved_fallback=bool(
            state.preferences.get('vault.fallback_file.approved', False)))
        key = vault.resolve(identity.secret_handle)
    except ConnectorError as error:
        if report is not None:
            report.set('VAULT_ACCESS_UNCONFIRMED', scope=scope)
        return "warn", f"vault: {error.message}", error.action or ""
    except (OSError, ValueError, TypeError):
        if report is not None:
            report.set('VAULT_ACCESS_UNCONFIRMED', scope=scope)
        return 'warn', 'vault: existing credential is inaccessible or invalid', 'inspect the selected vault'
    try:
        async with NexusHTTPClient(record.base_url) as http:
            me = await http.me(key)
        if report is not None:
            report.set('SERVER_RESPONSE_CONFIRMED', scope=scope)
        if me.server_id != record.server_id:
            if report is not None:
                report.set('SERVER_ID_MISMATCH', scope=scope)
            return 'fail', 'SERVER_ID_MISMATCH: Server installation differs from the saved profile', \
                'Confirm the selected Server installation before explicitly updating its profile.'
        if report is not None:
            report.set('SERVER_ID_CONFIRMED', scope=scope)
        if me.agent_id != identity.agent_id:
            if report is not None:
                report.set('SCOPE_MISMATCH', scope=scope)
            return "fail", f"key now authenticates {me.agent_id}", \
                "run 'identity replace-credential' after confirming the " \
                "rotation"
        if report is not None:
            report.set('PARTICIPANT_AUTHENTICATED', scope=scope)
        return "ok", f"/me {me.agent_id}", ""
    except ConnectorError as error:
        if report is not None:
            if error.code == 'EXECUTOR_OFFLINE':
                report.set('SERVER_UNREACHABLE', scope=scope)
            elif error.code in {'AUTH_FAILED', 'PERMISSION_DENIED'}:
                report.set('SERVER_RESPONSE_CONFIRMED', scope=scope)
                report.set('AUTH_FAILED', scope=scope)
            elif error.code == 'VERSION_INCOMPATIBLE':
                report.set('SERVER_RESPONSE_CONFIRMED', scope=scope)
                report.set('SERVER_UPGRADE_REQUIRED', scope=scope)
            else:
                # AGENT_AUTH_REQUIRED is also a generic HTTP 4xx fallback;
                # never parse its message to invent an authentication result.
                report.set('AUTHENTICATION_UNCONFIRMED', scope=scope)
        return "fail", f"{error.code}: {error.message}", error.action or ""
