# Test matrix — okto-nexus-connector

Statuses: NOT_RUN / PASS / FAIL. "contract" rows ran against the bundled
fake peers (HTTP A.5 routes + NXL r3 WSS server side); they never close
the homonymous real-environment cases. Environment for all PASS rows:
Windows 11, Python 3.13.1, Core wheel 0.2.1.dev0 (SHA-256 d0c35f…a538).
Evidence: [core021-alignment-2026-09-26](evidence/core021-alignment-2026-09-26.md).

| ID | Case | Status | Where / note |
|---|---|---|---|
| TC-01 | Isolated install | PASS | tests/e2e/test_packaging.py (clean venv, dependency audit) |
| TC-02 | No Nexus user | PASS | tests/unit/test_boundaries.py + models (no user store anywhere) |
| TC-03 | Canonical key reuse | PASS | tests/contract/test_import_flow.py (re-import ⇒ same identity, no remote creation) |
| TC-04 | False agent hint | PASS | contract + e2e (AGENT_ID_MISMATCH; secret never echoed) |
| TC-05 | Origin/vault failures | PASS | contract (cross-origin redirect refusal, revoked, keyring fallback approval) |
| TC-06 | Multi-identity isolation | PASS | tests/unit/test_vault.py (two servers, same alias, distinct namespaces) |
| TC-07 | Singleton under race | PASS | tests/integration/test_process_lifecycle.py (real subprocesses) |
| TC-08 | IPC unauthorized peer | PASS | tests/integration/test_daemon_app.py (denied before dispatch) |
| TC-09 | Independent terminal | PASS | process test (CLI exits, daemon alive) + logs-stream follower close |
| TC-10 | Daemon ≠ runtime | PASS | daemon boots with bindings registered; no harness spawns (suite-wide) |
| TC-11 | Safe discovery | PASS | discovery never executes candidates; explicit selection only; npm shims resolved passively or refused (Core 0.2.0 shapes) |
| TC-12 | Complete connect | PASS | tests/e2e/test_cli_flows.py (guided, idempotent, no manual IDs) |
| TC-13 | Setup failure/CAS | PASS | unit mcp-config tests (backup, retry-idempotent, third parties kept) |
| TC-14 | Remote project paths | PASS | integration root-guard (other cwd refused; drift via fingerprint re-check) |
| TC-15 | Outbound WSS | PASS | contract suite (client dials out; no connector listener) |
| TC-16 | Lane tickets | PASS | valid/invalid/forged ticket lanes; A-ticket≠B-binding |
| TC-17 | Reconnect | PASS | contract reconnect + generation fence; reconcile before admissions |
| TC-18 | Generation/clone | PASS | stale-generation welcome refusal; journal CAS on restart |
| TC-19 | Priorities under flood | PASS | urgent budget survives 600-frame flood with honest drop counter |
| TC-20 | Single Core | PASS | structural test: no Popen/parser outside the platform layer |
| TC-21 | Reuse/new-session | PASS | integration lifecycle (reuse default, --new-session explicit) |
| TC-22 | Interrupt vs stop | PASS | integration (interrupt keeps runtime; stop closes owned only) |
| TC-23 | Shutdown lifecycle | PASS | honest per-session outcomes; subprocess stop drains and exits |
| TC-24 | Direct MCP HTTP | PASS | mcp-config plan/apply points at the Server; requires_daemon=false |
| TC-25 | No MCP surface | PASS | structural + wheel entrypoint/metadata audit; Pi diagnostic |
| TC-26 | Native Pi bridge | PASS | contract (scope/lease gates; free text never an action) |
| TC-27 | Two channels/uncertain | PASS (contract) | transport/lease separation; no retry-on-uncertainty paths; real partition needs C11 |
| TC-28 | Daily CLI | PASS | e2e (human + --json paths, stable codes) |
| TC-29 | Approval authority | PASS | tests/integration/test_approvals.py (routing, no auto-approval, CAS refusal) |
| TC-30 | Doctor | PASS | e2e + unit (layered, prescriptive; never suggests disabling TLS) |
| TC-31 | Crash boundaries | PASS (shape) | journal/receipt survival + unknown ownership after restart; OS-level cuts inherit Core K10 evidence |
| TC-32 | Lease/clock | PASS (shape) | rollback-fenced clock refuses new work (Core fence exercised) |
| TC-33 | Saturation | PASS (shape) | JOURNAL_FULL typed failure + urgent reserve; no phantom facts |
| TC-34 | Hostile config | PASS | ownership-CAS refusals; no traversal (paths validated); IPC auth |
| TC-35 | Redaction | PASS | unit suite (bearer/ticket/capability/key shapes; recursive mapping) |
| TC-36 | Autostart | PASS (plan+dry-run) | per-OS plans, consent, no admin; real boot survival staged NOT_RUN (no authorized hosts) |
| TC-37 | Foreground shutdown | PASS | daemon run/stop bounded; subprocess exits with report |
| TC-38 | Upgrade/rollback | PASS | schema-version guard refuses newer state; runbook procedure |
| TC-39 | Uninstall scope | PASS | owned-entry removal preserves third parties (unit + e2e) |
| TC-40 | Packages | PASS | clean-venv wheel install + audit |
| TC-41 | Two real Servers | PASS (contract) | two fake peers, one daemon, isolated namespaces/lanes/vault |
| TC-42 | Adapter qualification | NOT_RUN | real provider runs are credential/cost-gated; binaries present (0.157.0/0.87.1/2.1.282) |
| TC-43 | Topology A/B/C | NOT_RUN | blocked: Server not adapted; no authorized hosts |
| TC-44 | MCP identity uniqueness | NOT_RUN | requires adapted Server (same key for MCP HTTP and runtime) |
| TC-45 | Joint release | NOT_RUN | requires N13/C11 campaign artifacts |

## J-matrix (Anexo B)

All J01–J34 remain **NOT_RUN**. They require the joint campaign with
the adapted Nexus Server (C11 blockers in
[IMPLEMENTATION_STATUS](IMPLEMENTATION_STATUS.md)). The contract fakes
 exercised in this repository are recorded as fixture-level evidence
 only and never close a J-case.
