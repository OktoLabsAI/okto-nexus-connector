# Evidence — CN2 correction campaign (2026-09-29)

**Test ID:** EV-CN2-CORRECTION-2026-09-29
**Status:** PASS — all eight executed failure groups (N01–N08) corrected
with causal seeds; N09 wired; artifacts bumped.
**Baseline:** Connector 0.1.0.dev0 @ 28d1476 (audit snapshot); corrected
tree bumps to **0.2.0.dev0** (CN2-08.02) with Core pinned at 0.2.10.dev0
(unchanged; wheel sha256 `4cde3b9a…50da0`).

## CN2 regression suite (CN2-00.01)

The package again delivered only Markdown + `RESULTADOS_RESUMO.json` (no
`regressoes/` executables — confirmed by inventory). Per CN1's identical
precedent, the 20 CN2 cases were RECONSTRUCTED from the exact
reproductions in `01_RELATORIO_REAVALIACAO.md` §4–11 (same node ids and
causal conditions) in `tests/regressoes/test_connector_audit_cn2.py`:

```
tests/regressoes/test_connector_audit_cn2.py
20 passed (16 former FAIL + 3 controls + 1 load observation turned
normative-with-configured-limit)
```

The 36 CN1 seeds remain green (one test_27 peer-skip unchanged, recorded).
Environmental fixtures were fixed as fixtures (POSIX exec bits, Pi
discovery expectation), never by disabling containment.

## Full suite

```
python -m pytest tests/ -q --tb=no
177 passed, 2 skipped
unit 62+1s · contract 35 · integration 20 · e2e 7 ·
regressoes CN1 35+1s · regressoes CN2 20
```

## Findings → corrections (N01–N09)

| Finding | Correction | Seed |
|---|---|---|
| N01 | Immutable `ValidatedOperation` DTO carries the FULL authenticated envelope (namespace, SessionKey, generations, revisions, grant ref) from receiver to manager; remote verbs resolve sessions ONLY by `session_by_key` in the wire's namespace and match binding/agent/workspace; the dispatcher passes the DTO whole; lane reservations are REVALIDATED after every admission wait (epoch/state/generation) — a detached lane during the wait yields zero effects | b02, b03[foreign], control[same] |
| N02 | Separate work classes: productive effects share a bounded pool while `turn.interrupt`/`runtime.close`/deny run on a reserved control path (never behind the submits they must stop); admission is bounded (items+bytes) BEFORE `create_task`, with an explicit `CAPACITY_EXCEEDED` refusal that keeps the intent consultable; refused receipt enqueue keeps a durable obligation (journal) | b01 + observation (now a configured-limit normative test) |
| N03 | `EventBridge._journal_page` reads FINITE pages from the PUBLIC `Journal.events()` port — one event returns immediately, zero means end-of-snapshot; replay resumes after the VALIDATED durable ACK (unacknowledged batches are replayed, never silently skipped); `event.ack` is validated for namespace (foreign frames never associate), known stream of the CURRENT connection serial, and a contiguous watermark that can never exceed what was actually written; reconnect wakes pending streams without new native events | b04, b05, b05b, b05c |
| N04 | Reconciliation has explicit `pending/failed/complete` state — a failed initial projection keeps the transport OUT of ready and blocks productive admissions (no empty-report success); durable receipts answer via the PUBLIC `Journal.get_receipt`/`claimed_sessions` ports with NO executable required (missing handle = unknown ownership, not nonexistence) | b06, b10 |
| N05 | Typed stop classification: `FAILED` receipt without effect releases only the stop reservation (session stays managed, retryable); typed `CoreError` (retry_safe, pre-effect) same; `OUTCOME_UNKNOWN` keeps session+producer; the same rules apply to remote close | b07[failed_receipt], b07[core_exception] |
| N06 | `decide_native_approval` derives the action from the validated decision, builds `NativeApprovalOperation` with NAMED fields (the old positional call inverted request/decision and crashed with TypeError) and reaches the public Core port; the managed start now installs an EPHEMERAL per-session provider home containing the direct-HTTP MCP client config (Server URL + env-bound bearer reference via the Core's public renderers) — the URL reaches the harness process, not only the token | b08, b09 |
| N07 | The bootstrap link ticket picks ONE eligible binding and uses exactly ITS agent's credential (identity A first in the list never keys binding B); a lane added on a READY transport gets its own owned attach producer and completes a REAL attach without reconnecting the socket | b11, b11b |
| N08 | `validate_link_url` centralizes WSS origin validation (non-loopback MUST be `wss://`, userinfo refused, loopback `ws://` the documented lab exception) applied at transport CONSTRUCTION — before any ticket fetch or credential reaches the websocket library (b12 proves zero library touches and zero ticket fetches) | b12 |
| N09 | `evaluate_runtime_availability` + `AvailabilityReport.to_dict()` consumed; `availability_snapshot()` publishes the versioned executor projection (format v2, evidence-derived `executor_revision` that changes with installs — not the state schema version); `resolve_installation` exposed for exact one-candidate selection; catalog remains the single source | inspection + snapshot test in discovery flows |

## Built artifacts (this evidence run)

| Artifact | SHA-256 |
|---|---|
| `okto_nexus_connector-0.2.0.dev0-py3-none-any.whl` | `fd9c08a0cb515104f9c5daa52fa4ed9ba98f66825d902d9ac37c9fab0c8efcb3` |
| `okto_nexus_connector-0.2.0.dev0.tar.gz` | `1474baf16ffd3410ad33cba3f998366687043653c6cdf59507d2cbafae425b63` |

Clean-venv install + `-I` imports + catalog/availability + CLI smoke green
(tests/e2e/test_packaging.py).

## Honest limits / declared pendings (CN2-06.04)

- **Remote `runtime.open`** remains explicitly UNSUPPORTED with a typed
diagnostic until the authorized vertical gate (Server side of the r3
contract) — a declared delivery boundary, not synthetic success.
- **Pi bridge / codex resume / native approvals opt-in** in the daemon
composition: pi_native_action and codex_resume callbacks are not yet
wired into `CoreRuntimeHost.build` (CN2-06.03 stays PENDING — the audit
explicitly allows declaring the pendência).
- G1 (lab) is claimed for the corrected scope; G2/G3 (real vertical,
multi-host) remain BLOCKED on the adapted Server and authorized hosts —
unchanged external gates.
- No push/publication performed (this plan explicitly withholds that
authorization).
