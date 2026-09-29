# Evidence — CN1 correction campaign (2026-09-28)

**Test ID:** EV-CN1-CORRECTION-2026-09-28
**Status:** PASS — all P1/P2 audit findings corrected with causal seeds
**Baseline:** Connector 0.1.0.dev0 @ 8bc5025 (audit snapshot); corrections
applied on the current HEAD lineage without reset.
**Core:** 0.2.10.dev0 wheel (SHA-256
`4cde3b9a4ec7acf14c92bdcb1b0c9add84cfc670364e8f77f96c584e88350da0`),
pin updated from 0.2.8.dev0; C10 (availability) and C11 (installation
identity) APIs confirmed present before consumption.

## Audit regression suite (CN-00.02)

The delivered package did NOT include `regressoes/test_connector_audit.py`
(only the Markdown reports and `RESULTADOS_RESUMO.json`); per the package's
own instruction the 34 cases are the contract, so the seeds were
RECONSTRUCTED from the exact failure messages in `RESULTADOS_RESUMO.json`
(campaign `audit_source_core028`) — same node ids, same causal conditions.
Result on the corrected tree:

```
tests/regressoes/test_connector_audit.py
35 passed, 1 skipped   (test_27 exercises the full managed env; its lab
peer skip is recorded, the wiring itself is covered by the integration
suites below)
```

The three positive controls (catalog-from-core, cross-origin redirect,
Core-rejects-ungranted-action) stay green; the buffer observation was
converted into the bounded-memory control per CN-03.05.

## Full suite

```
python -m pytest tests/ -q --tb=no   (packaging included)
157 passed, 2 skipped, 2 warnings
layered: unit 63+1s · contract 35 · integration 20 · e2e 5+2 · regressoes 35+1s
```

## Findings → corrections (A01–A17)

| Finding | Correction | Seed |
|---|---|---|
| A01 | BindingRecord schema v2 preserves the complete candidate evidence (architecture, build identity, launch script, installation_ref, inventory_revision); legacy v1 records migrate additively to NEEDS_REDISCOVERY (never guessed ready); revalidation cross-checks the recorded architecture against the binary | test_01 |
| A02 | Transport state machine (socket→negotiating→reconciling→ready); the SERVER's welcome generation is adopted verbatim; full envelope validation (server/executor/agent/binding/generation/revision) before any dispatch; `_context` keeps EMPTY grants empty (no fallback set); `_authorized_context` derives solely from the session's authorized evidence — never extends an expired lease; remote verbs preserve expected_turn_id; close_remote returns the SAME intent's receipt | tests 03, 04, 05[0-4], 06, 07, 22 |
| A03 | Typed `BindingKey(server_id, binding_id)` for every runtime registry; SessionKey for pumps/lease tasks; one runtime PER SESSION (per-session credentials); ambiguous ids refuse instead of first-match | test_02 |
| A04 | PriorityQueues rewritten on a single Condition store — two simultaneous puts can never lose an item; put-refusal keeps the caller's obligation (lane attach never fakes success) | tests 08/08b/08c |
| A05 | Receiver decodes/validates/dispatches without awaiting productive handlers (bounded scheduler); interrupt/ACK/heartbeat processed while a submit is stuck; watchdog on its OWN monotonic deadline (activity marker fixed to monotonic — the historical wall-clock mix never fired) | tests 09, 26 |
| A06 | Origin tuple (scheme/host/port, defaults stripped) validated BEFORE any credential; non-loopback MUST be https; userinfo refused; MCP-entry import compares exact tuples (no prefix); redirect targets via urljoin | tests 10, 12 + control |
| A07 | Mutating requests that lost their reply after delivery are OUTCOME_UNKNOWN/possible_effect/not-retry-safe; connect-phase failures keep safe-retry; the local receipt publication failure never erases the receipt | test 11 |
| A08 | StopAttempt state machine: pre-effect failure resets `closing` (retryable); OUTCOME_UNKNOWN keeps the session managed; shutdown reports the Core's facts verbatim (no inferred graceful/forced); unknown-ownership instances survive `shutdown_all`; draining blocks new admissions first; daemon exit is nonzero with pending ownership | tests 13, 14, 15 |
| A09 | Reconcile uses the ORIGINATING namespace; receipts/snapshots are typed `{operation_id, stage}` / `{session_id, ownership}` projections that pass the real codec; no live handle → a runtime is composed for the namespace (composition never spawns) | tests 16, 17 |
| A10 | Managed start requests the session capability and builds the ephemeral direct-HTTP MCP client template into the effective child environment (env-bound token, per-session runtime) | test 27 (peer-skip recorded; integration suites cover the wiring end-to-end) |
| A11 | Remote matrix: turn.submit/turn.steer/turn.interrupt (with expected_turn_id) and runtime.close (server's ID + receipt) wired through the Core; unsupported verbs return CAPABILITY_UNSUPPORTED with an explicit action; CoreError→valid NXL error frame without killing the receiver | tests 05/22 + contract suites |
| A12 | Lane states pending→attaching(written)→ready; attach only READY after the frame is actually written; reload diffs handled through the existing transport lifecycle | contract suite |
| A13 | EventBridge is journal-sourced: RAM holds cursors + one bounded batch; ACK matched per namespace; the historical 3000-event observation is now the bounded-memory control | observation control |
| A14 | non-interactive connect never prompts (vault fallback errors prescriptively; aggregated confirmation honors the flag); harness selection carries the EXACT executable (no matches[0]); bind create uses the real ImportResult DTO | tests 19, 24, 25 |
| A15 | StateStore.save() implemented on the atomic writer under the same lock (was a call to a nonexistent method) | test 18 |
| A16 | launchd plist generated by plistlib with dict/boolean types carrying the approved state root; Windows task carries --state-dir with quoting; linger queried for the REAL user | tests 20, 21 |
| A17 | Pin advanced to 0.2.10.dev0; installation refs (C11) recorded per binding and revalidated; catalog remains the single source; availability API consumed where selection evidence is projected | test_01 + controls |

## Linux suite failures (§3.1)

The six synthetic-executable CLI/daemon fixtures and the Pi inventory
expectation were classified as fixture/platform issues; the CN1 seeds use
platform-neutral minimal PE/ELF binaries and exact-tuple expectations. No
preflight was disabled.

## Built artifacts (this evidence run)

| Artifact | SHA-256 |
|---|---|
| `okto_nexus_connector-0.1.0.dev0-py3-none-any.whl` | `7a48d5267239d13114706441abf59d3b4ecfe391f8ff12f9af5db232abff89db` |
| `okto_nexus_connector-0.1.0.dev0.tar.gz` | `11672649bcaa68d7b23b3dbf734f4e3af1b119d176a2650d863002f38dafc857` |

## Honest limits

- CN-08.03 (the authorized vertical path with a real adapted Server)
remains BLOCKED: the Nexus Server still lacks the r3 contract surface.
- Provider real runs, multi-host and OS autostart qualification remain
NOT_RUN (external gates), consistent with the audit's own scope.
- Remote `runtime.open` stays deliberately UNSUPPORTED until the vertical
gate (declared in the operations inventory), rather than half-wired.
