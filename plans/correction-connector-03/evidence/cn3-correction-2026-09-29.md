# Evidence — CN3 correction campaign (2026-09-29)

**Test ID:** EV-CN3-CORRECTION-2026-09-29
**Status:** PASS — all six executed defect groups corrected with causal
seeds; G01/G02 wiring advanced with the app-flow routes now real.
**Baseline:** Connector 0.2.0.dev0 @ 2354392 (audit snapshot; HEAD was
identical); corrected tree bumps to **0.3.0.dev0**; Core pinned
0.2.10.dev0 (unchanged).

## CN3 regression suite (CN3-00.02)

No `regressoes/` executables shipped (package inventory: 8 files,
Markdown/JSON only). The 12 CN3 cases were reconstructed from the exact
reproductions in `01_RELATORIO_REAVALIACAO.md` §4–10 (same node ids and
causal conditions; D01's five variants per the matrix) in
`tests/regressoes/test_connector_audit_cn3.py` — plus two author-added
variants (D03b double-release, D05 target-2 control) that the plan's
"não resolver assim" clauses demand:

```
tests/regressoes/test_connector_audit_cn3.py
14 passed (9 former FAIL + 3 controls + 2 author variants)
```

CN2 (20 + b-fixtures) and CN1 (35+1s) preserved green — two CN2 seeds
adapted with causal conditions intact (documented below).

## Full suite

```
python -m pytest tests/ -q --tb=no
191 passed, 2 skipped
unit 62+1s · contract 35 · integration 20 · e2e 7 ·
regressoes CN1 35+1s · CN2 20 · CN3 14
```

## Findings → corrections (CN3-01…06 + G01/G02)

| Finding | Correction | Seed |
|---|---|---|
| CN3-01 | `ValidatedOperation` completed with `session_owner_generation`/`workspace_binding_id`; `_resolve_remote_session` compares the FULL envelope (connection/owner generations, authorization/configuration revisions) against the session's authorized snapshot — divergence is a typed STALE_GENERATION refusal with ZERO effects, never silently "corrected"; the lane reservation captured at admission now includes the AUTHORIZATION REVISION and `lane_reservation_valid` rechecks it after every wait (rotation invalidates queued work); `add_lane` on an existing lane rotates epochs atomically and demotes it to pending | D01[configuration/owner/generation], D06, controls[valid/hash] |
| CN3-02 | `reconcile.request` (and approval.request) validated against the AUTHENTICATED channel namespace BEFORE any handler/storage read; the callback receives the channel's trusted namespace — a foreign frame is refused with an in-scope error frame and never selects another Server's journal | D02 + own-server control |
| CN3-03 | Admission reservations are objects carrying the EXACT cost computed once (`try_reserve → _Reservation`); `release` spends that object, double-release raises; sequenced work of varying frame lengths ends at items=0/bytes=0 | D03, D03b |
| CN3-04 | Cold-host recovery: `ensure_history_journal()` opens the technical journal explicitly (single-flight) before reconcile/replay answers — a closed store is state to RECOVER, never a false empty; `_journal_page` and `_core_ack` recover it on demand | D04 |
| CN3-05 | `wait_event_ack` is a watermark-target PREDICATE (checked before/after each wait): a duplicate valid ACK satisfies the wait its watermark covers; an old watermark never satisfies a newer target; `send_events` replay no longer erases received proofs; duplicate ACKs wake waiters idempotently | D05 (+ author target-2 control) |
| CN3-06 | Ephemeral MCP home scoped by the FULL owner (server/executor/binding/session) with an ownership marker and atomic writes — two namespaces with the same textual session_id never share or overwrite configs | D07 |
| G01 | Availability wired into the app flow: `discover` CLI and a new `availability.snapshot` IPC op publish the versioned snapshot through the real surfaces; `inventory_revision` in BindingRecord now derives from the snapshot's evidence revision (schema-version arithmetic removed; field widened additively) | discover/IPC wiring + D-flow |
| G02 | `_decide_approval` is two-phase (reserve → validate → network → confirm): invalid decisions and transport failures do NOT consume the request; approvals keyed by (server_id, request_id); the authorized decision also reaches `decide_native_approval` when the session is live-managed (Server-CAS otherwise); lane tickets store expiry and rotation updates epochs | approvals tests + D06 rotation |

CN2 fixture adaptations (causal conditions preserved, per CN3-00.02):
b09 looks for the config under the namespaced home (same URL assertion);
the approvals integration request now flows the native route when the
session is managed.

## Built artifacts (this evidence run)

| Artifact | SHA-256 |
|---|---|
| `okto_nexus_connector-0.3.0.dev0-py3-none-any.whl` | `2f91a2f00a1b96a0bde709b9ede8a4b4db7cd8b2b4f1434c84dbba4ce44bf292` |
| `okto_nexus_connector-0.3.0.dev0.tar.gz` | `7cacc1a79f2a8901063ea34b2f20bd560ee0c2800b01fc39f40d982681a9640c` |

Clean-venv install + smoke green (tests/e2e/test_packaging.py).

## Declared pendings (CN3-06/07 scope honesty)

- **Remote `runtime.open`**: still typed-UNSUPPORTED — the Server-side
  contract of the open grant is not agreed; CN3-06.01 remains BLOCKED on
  that external contract (declared, not half-wired).
- **CN3-03.05 end-to-end intent publication** (stable id before the
  first mutating POST + receipt-publication endpoint): the connector now
  classifies uncertainty correctly, but the receipt endpoint is a Server
  contract — recorded in the handoff as the open item.
- **CN3-06.03 optional compositions** (pi_native_action / codex_resume /
  native-approvals opt-in in `create_runtime`): still not wired into the
  daemon composition; capabilities remain declared-unavailable.
- **CN3-06.04 daemon exit with unknown ownership**: exit code 1 +
  honest reports remain; SO-transfer qualification is its own campaign.
- G1 claimed for the corrected lab scope; G2/G3 unchanged (external).
- No push/publication (explicitly withheld by this plan).
