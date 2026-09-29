# Evidence — CN4 correction campaign (2026-09-29)

**Test ID:** EV-CN4-CORRECTION-2026-09-29
**Status:** PASS — all five reproduced groups (P01–P05) corrected with
causal seeds through the real application paths; G-wiring items closed
where the contract allows and declared where it does not.
**Baseline:** Connector 0.3.0.dev0 @ `e930797` (HEAD identical to the
audited snapshot — no reset); corrected tree bumps to **0.4.0.dev0**;
Core pinned `0.2.10.dev0` (unchanged).

## CN4 regression suite (CN4-00.02)

For the THIRD consecutive time the package arrived WITHOUT its
executables — `00_ENTREGAR_AO_AGENTE.md` claims the ZIP contains
`regressoes/` and `executar_verificacao.py`, but the material delivered
is only 6 files (Markdown + a name/status JSON with no code and no
failure messages). Per the CN1/CN2/CN3 precedent, the seeds were
RECONSTRUCTED from the exact reproductions (`01_RELATORIO_REAVALIACAO.md`
P01–P05) and node ids (`03_MATRIZ_ACEITE.md` ACN4-01..14), plus four
complementary matrix cases (ACN4-15/19/24/33) and the CN3 suite —
which our tree already runs against the real Core (the auditor's own
fixture adaptation, `ensure_history_journal`, is a non-issue here):

```
tests/regressoes/test_connector_audit_cn4.py
18 passed (11 former FAIL + 3 controls + 4 complementary)
tests/regressoes/test_connector_audit_cn3.py  14 passed (preserved)
```

## Full suite

```
python -m pytest tests/ -q --tb=no
207 passed, 2 skipped (+ e2e packaging 2 passed in a clean venv)
unit 62+1s · contract 35 · integration 20 · e2e 7 · regressoes
CN1 35+1s · CN2 20 · CN3 14 · CN4 18
```

Three seeds causally adapted (diff in the package's spirit, each
preserving the observed causal condition):
- CN2 `_AckTransport` double now accepts the batch's `target` predicate
  (same returns; None while unacknowledged — replay condition intact).
- integration `test_approval_routing_and_cas`: a repeat now CONSULTS the
  recorded terminal outcome (ACN4-16/20) — the Server CAS stays answered
  exactly once with the ORIGINAL decision.
- CN2 b09 MCP-home lookup follows the CN4-03 versioned layout (same URL
  assertion).

## Findings → corrections (P01–P05)

| Finding | Correction | Seed |
|---|---|---|
| **P01** approvals: untranslated vocabulary, native before Server, wrong origin/key reuse | `_decide_approval` rewritten: ApprovalKey `(server, request)` resolves EXACTLY one pending (ambiguity = typed error listing servers, ZERO HTTP); binding resolved by server+binding_id with the frame's agent compared — never `split(binding_id)`, never first-match; `agent_key` (the secret) never substitutes the dictionary key; FIRST external operation is the canonical `http.approval_decision`; vocabulary translated by the Core's public contract only after confirmation (approve→accept, deny→decline; action from the observed request KIND, never `decision.startswith`); native application only for a LIVE MANAGED session (checked via `session_ids()`, not by swallowing VALIDATION_ERROR); state machine pending→server_pending→server_confirmed→native_pending→applied with result_unknown/native_refused terminal variants; definitive refusals restore pending, possible-write NEVER does; concurrent second decision gets an explicit conflict; terminal records retire BY THEIR OWN KEY into a bounded (128) tombstone history so repeats consult the same outcome | P01[approve/deny], P01b, P01c (+repeat), control, P01d, P01e |
| **P02** bridge ACK caller | `wait_event_ack` now called with `target=batch[-1].sequence` — ACK1 can never conclude batch2's wait; on timeout the obligation PARKS (exactly one attempt while the ACK is absent) and wakes on the transport's NEW validated watermark (`ack_observer` → `EventBridge.ack_arrived`), a reconnect, or a new event; a failed Core-ACK application keeps a `pending_core_ack` obligation re-applied verbatim on a bounded 5s retry (never a re-send of the agent's work) | P02 (+ CN3 D05 preserved, CN2 b05 replay on reconnect preserved) |
| **P03** MCP home long-ID collision | `_mcp_home_dir`: versioned digest (`v2-<readable>-<sha256(canonical tuple)[:24]>`) of the full ownership tuple — 85-char ids differing at the end are DISTINCT trees; the `.owner.json` marker (layout+full tuple) is VALIDATED BEFORE any write — a divergent/foreign marker refuses (typed error) and is never overwritten; marker-less non-empty trees are refused; the marker itself is written atomically; active sessions keep their exact tree (no migration of live homes) | P03[long/control], P03b (foreign marker) |
| **P04** inventory loses the candidate; revision blind to bytes | `inventory_candidates()` returns the FULL Core `InstallationCandidate` objects (PATH + shims + operator extras) — CLI `discover` and the daemon IPC `availability.snapshot` op use the SAME service function over the SAME effective inventory (no more `candidate(executable, explicit=True)` re-creation that dropped the Pi pair's CLI identity/version/build); `availability_snapshot` revision `inv2:` hashes a canonical, order-independent serialization of ALL evidence per candidate (identity, fingerprint/build, version/arch, trust/source, Core state/reasons, core+format versions) — CLI-bytes changes rotate it, repeats/reorders do not; the binding persists the revision of the FULL effective inventory (never a subset re-hash) | P04[cli/ipc], P04b, control |
| **P05** lane rotation dead on reload; expiry discarded | `add_lane` rotation: true no-op returns WITHOUT epoch bump; a real rotation cancels the in-flight old attach, installs the CURRENT provider/identity/epoch, fences to pending ALWAYS and re-attaches through the existing coordinator on a live channel; `_sender` only marks ready when the attach frame's credential_epoch/authorization_revision match the lane's CURRENT values (a late old attach never opens the new epoch); `reload_state` DIFFS persisted credential_epoch/authorization_revision against `lane_info()` and rotates only on change; lane ticket providers return `(ticket, expires_in)` from the real HTTPS contract and the transport records a local monotonic expiry (5s margin) — an expired ticket demotes the lane from ready on the next inbound frame and renews single-flight through attach (ticket renewal never extends the runtime lease) | P05, P05b (+ no-op control), P05c |

## Built artifacts (this evidence run)

| Artifact | SHA-256 |
|---|---|
| `okto_nexus_connector-0.4.0.dev0-py3-none-any.whl` | `c60c9a8e9a0e3f181d600689fdcd9e84edded8904e8b630531409777dfc9202f` |
| `okto_nexus_connector-0.4.0.dev0.tar.gz` | `d89ff690c2de26d95ce980beb02a14449118ce193c0dc32fb89df59a88d7730e` |

Clean-venv install + import-isolated smoke green (tests/e2e/test_packaging.py).

## Declared pendings (CN4-06.03 scope honesty)

- **P01 input-provide end-to-end with a live input request** (ACN4-18):
  the action/kind routing is implemented; the matrix case stays NOT_RUN
  because it needs a native input request observed in the pump of a
  live harness — lab-owned, next campaign.
- **Remote receipt/intent publication contract** (P01/05 stable-intent
  POST evidence) and **snapshot publication to the real Server**
  (ACN4-30): BLOCKED on the Server's r3 contract — the connector
  publishes versioned projections only through its real surfaces.
- **`resolve_selection` productive caller** (ACN4-27/28): the public
  resolver is wired for bind-time resolution of the SELECTED candidate;
  the two-identical-installs selector flow needs the Server's
  authorization response to carry the selected ref — BLOCKED there.
- **SO-transfer/supervision on unknown exit** (ACN4-34): exit code 1 +
  honest reports remain; qualification is its own campaign.
- **runtime.open remote**: still typed-UNSUPPORTED (external gate).
- Providers/real Nexus Server/two-host topology: NOT_RUN (external).
- CN4-05.03 automatic renewal is single-flight-through-attach with a
  local monotonic deadline; a full renewal POLICY (backoff quotas,
  revoke correlation) is declared pending, not silently claimed.
