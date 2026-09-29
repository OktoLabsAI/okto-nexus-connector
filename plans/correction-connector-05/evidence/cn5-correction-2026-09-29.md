# Evidence — CN5 correction campaign (2026-09-29)

**Test ID:** EV-CN5-CORRECTION-2026-09-29
**Status:** PASS — all four confirmed groups (Q01–Q04, six failures)
corrected with causal seeds through the real application paths;
complementary matrix cases implemented; CN4/CN3/CN2/CN1 preserved.
**Baseline:** Connector 0.4.0.dev0 @ `87b8fd2` (HEAD identical to the
audited snapshot — no reset); corrected tree bumps to **0.5.0.dev0**;
Core pinned `0.2.10.dev0` (unchanged).

## CN5 regression suite (CN5-00.02)

For the FOURTH consecutive time the package arrived WITHOUT its
executables — `00_ENTREGAR_AO_AGENTE.md` states "nesta rodada eles estão
realmente incluídos" and asks to verify `regressoes/test_cn4_review.py`,
`test_cn5_review.py`, `test_cn3_review.py`, `cn2_helpers.py` and
`executar_verificacao.py`, but only 6 Markdown/JSON files were delivered
(no code, no failure messages). Per the CN1–CN4 precedent, the seeds
were RECONSTRUCTED from the fixed specifications of
`02_PLANO_CORRECAO_CN5.md` (sections 2–3: DTOs, signatures, states,
effect order) and the node ids of `03_MATRIZ_ACEITE.md` ACN5-01..32:

```
tests/regressoes/test_connector_audit_cn5.py
15 passed — Q01a[approve/deny] through the REAL receiving path,
Q01b foreign namespace (zero dispatch proven by spy), Q02 cancel-waiter
(+CoreError terminal, +replay-in-flight, +malformed applied, +target
changed during POST), Q03 rotation during in-flight attach (+READY
control, +triple rotation), Q04 transient recovery (+send failure,
+drain timers, +Core-ACK re-apply) and the healthy control.
```

## Full suite

```
python -m pytest tests/ -q --tb=no
222 passed, 2 skipped (+ e2e packaging 2 passed in a clean venv)
unit 62+1s · contract 35 · integration 20 · e2e 7 · regressoes
CN1 35+1s · CN2 20 · CN3 14 · CN4 18 · CN5 15
```

Seed adaptations with causal conditions preserved (documented):
- CN4 P01-family seeds now DELIVER through `_on_remote_approval` (the
  auditor's own adaptation) and the control uses the CN5 target-scoped
  manager signature; p01e asserts the typed different-decision conflict.
- CN2 b08 uses the target-scoped signature built from the session's own
  namespace; the integration approvals frame is explicitly
  administrative (no session_id) — a session-bearing frame pointing at
  an unmanaged session is now a typed refusal by contract.

## Findings → corrections (Q01–Q04)

| Finding | Correction | Seed |
|---|---|---|
| **Q01a** receiving destroys the correlation proof | New `services/approval_state.py`: `PendingApproval` keeps an IMMUTABLE deep copy of the operational proposal (`request_hash`/`request_id`/`method`/`params` exactly as received, never re-calculated) while ONLY `display_proposal` is redacted; `to_public_dict()` is the sole outward projection (IPC listing, logs, export) and never serializes the DTO; the global redactor is untouched | Q01a[approve/deny] — the REAL Core accepts the reply against the request its pump observed; ACN5-21 token redacted in display, intact in operation |
| **Q01b** HTTP uses B but the Core selected is A | `ApprovalKey(server, executor, request)` + `ApprovalTarget` carry the full scope; the manager's `decide_native_approval` is target-scoped (`session_by_key` ONLY — the bare-`session_id` path is prohibited in this flow) and compares namespace/agent/workspace plus the captured connection/owner generations and authorization/configuration revisions against the session's authorized state BEFORE the Core call (divergence = typed `BINDING_NOT_AUTHORIZED`/`STALE_GENERATION`, zero effects, never repaired from a foreign session); a native request whose session is not managed in ITS namespace is a typed refusal — never silently administrative; the daemon's global `session_id in session_ids()` test is gone | Q01b (spy proves ZERO dispatch to A's Core; refusal precedes the canonical POST per CN5-01.04 — neither origin consumed it) |
| **Q02** waiter cancel kills the accepted decision; CoreError leaves a ghost phase | `_run_decision(attempt)` is a producer task registered on the attempt BEFORE the first await; the waiter `asyncio.shield`s it — cancelling the waiter never cancels the POST; the producer OWNS its HTTP client; repeats share/consult the same attempt (identical decision) or get a typed conflict (different decision); every exit is explicit per the phase table (SERVER_REFUSED/SERVER_UNKNOWN/SERVER_CONFIRMED/NATIVE_PENDING/NATIVE_REFUSED/NATIVE_UNKNOWN/APPLIED) — `CoreError` is captured separately as a typed terminal NATIVE_REFUSED keeping the canonical confirmation; `applied` must be a strictly boolean `True` upstream (string/object = typed VERSION_INCOMPATIBLE, zero native); new `approvals.status` IPC reports phase + REAL producer presence + allowed action; `approvals.pending` distinguishes awaiting-operator from decision-in-processing; shutdown does not cancel producers just for expiring | Q02 (cancel during retained POST → producer alive → shared repeat → exactly ONE POST), Q02b (JOURNAL_FULL → NATIVE_REFUSED consultable), Q01c (malformed applied), Q01d (replay keeps attempt), Q01e (target changed during POST → STALE_GENERATION, confirmation kept, no re-POST) |
| **Q03** rotation during in-flight attach loses the successor | Per-lane `attach_generation` + ONE coordinator task; `LaneAttachAttempt` snapshots provider/identity/revisions/epoch/connection serial BEFORE the first await (a late provider result is never relabelled with the lane's current values); rotation cancels the old attempt, bumps the generation and marks `reattach_requested` — the done-callback observes the OLD task's completion and `_ensure_lane_attach`s the CURRENT generation (handshake attaches also route through the coordinator); the attach frame rides an INTERNAL envelope carrying the generation and the sender discards obsolete frames BEFORE any byte leaves; transient provider failure keeps PENDING with ONE owned backoff retry (TimerHandle cancelled on removal/stop); genuine no-op bumps nothing | Q03 (old provider retained → successor runs → late old ticket never relabels), control (READY rotation), Q03b (1→2→3 → only generation 3) |
| **Q04** transient error kills the publisher; reconnect doesn't restore it | `_ensure_stream_task` is called by EVERY entry point (publish, ack_arrived, transport_online_again) — a dead publisher is re-created on reconnect without any new harness event, retrieving the old task's exception once; page-read/send/Core-ACK failures park with cursors intact under ONE owned retry timer per stream (progressive 0.25/0.5/1/2/5s, reset on progress, anticipated by reconnect); `_closed` (drain) is the bridge's own lifecycle, separate from transport offline; drain cancels timers and forbids resurrection without touching runtime operations or the journal; batch target and replay cursors preserved (CN4-02 intact) | Q04 transient (reconnect-only recovery, only seq1 ever sent, ACK1 applied once), Q04b (send failure converges), Q04c (drain: no resurrection, no orphan timers), Q04d (failed Core-ACK re-applies the SAME watermark) |

## Built artifacts (this evidence run)

| Artifact | SHA-256 |
|---|---|
| `okto_nexus_connector-0.5.0.dev0-py3-none-any.whl` | `ca732f6f4ae5f039239c6b402d32963fab7fecf905c32ca1fcee60edb3c47bef` |
| `okto_nexus_connector-0.5.0.dev0.tar.gz` | `90d51b25e9f66206d1964e7398cb5adb44572b04e183b2e071b5833da0ed4fa0` |

Clean-venv install + import-isolated smoke green (tests/e2e/test_packaging.py).

## Declared pendings (CN5-05.02 scope honesty)

- **Post-crash recovery of a canonically-unknown decision** remains
  BLOCKED on the Server's stable intent/receipt consultation contract
  (declared external since CN2); same-process recovery IS proven (Q02).
- **ACN5-26 double cancel during native application**: the producer
  survives waiter cancels (Q02 proves the mechanism for the whole
  flow); the matrix case itself was not seeded separately.
- **Remote snapshot publication / UI / two hosts / providers / SO
  qualification**: NOT_RUN (external campaigns, per the handoff).
- `runtime.open` remote: still typed-UNSUPPORTED (external gate).
- Exit-with-unknown supervision/transfer: exit code 1 + honest reports;
  qualification is its own campaign (unchanged declaration).
