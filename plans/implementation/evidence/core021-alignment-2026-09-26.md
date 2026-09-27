# Evidence — Core 0.2.1.dev0 alignment suite run (2026-09-26)

**Test ID:** EV-CORE021-ALIGNMENT-2026-09-26
**Status:** PASS
**Scope:** full automated suite after adopting `nexus-connector-core`
0.2.1.dev0 (C2 reaudit, findings R01–R10).

## Environment

| Item | Value |
|---|---|
| OS | Windows 11 (win32), x86_64 |
| Python | 3.13.1 |
| Connector | okto-nexus-connector 0.1.0.dev0 (working tree, pin updated to core==0.2.1.dev0) |
| Core wheel | nexus_connector_core-0.2.1.dev0-py3-none-any.whl, SHA-256 `d0c35f4cd386ae35ac2bc8c143c9c6b18a11ab0d61dd39bcc8bf60290ebfa538` (source HEAD `7a7a248`) |

## Connector changes for the C2 update

1. **R06 (fix):** `_current_build_identity` recomputes Pi identities
   against the **pi-coding-agent package root** (`parents[2]`, C2
   definition) — the previous `parents[3]` scope-dir root would produce
   false `PROFILE_DRIFT` for Pi bindings. A dedicated regression seed
   (`test_pi_identity_root_consistent_between_selection_and_revalidation`)
   binds selection-time identity to revalidation.
2. **C2/R-§6 (async lifecycle):** the daemon opens the shared journal
   through the Core's async `open_journal` entry (blocking schema setup
   off the event loop); `shutdown_all` closes journal and ledger via
   `aclose()`.
3. Pin `nexus-connector-core==0.2.1.dev0`; clean-venv packaging audit
   re-run against the new wheel.
4. Adopted transparently (no connector change needed): R01 dedup/lock
   decoupling, R02 control-pool, R03 clock-aware effect fence, R05
   coalesced wakes, R07 containment-gated probes (typed refusals carry
   the missing-requirement map into our `doctor`), R09
   `CodexResumeGrant` publication and the corrected `environment`
   annotation, R10 cleanup budget/eviction semantics, R08
   `pi_install_root`/`pi_node` composed discovery (connector keeps the
   public standalone helpers for the daemon-less CLI path).

## Command

```
python -m pytest tests/ -q --tb=no
```

## Result

```
115 passed, 1 skipped in ~3m30s (layered: unit 55+1s, contract 35,
integration 20, e2e 5)
```

## Built artifacts (this evidence run)

| Artifact | SHA-256 |
|---|---|
| `okto_nexus_connector-0.1.0.dev0-py3-none-any.whl` | `bebe2211a3faa14ec8035b599af79283eac4182dba36f542be320b43b830a085` |
| `okto_nexus_connector-0.1.0.dev0.tar.gz` | `940f6a08ce6777c527a086ddaf40d283dbb6d1a0c5dc6239a772dfb6d62be4de` |

## Honest limits

- External blockers unchanged: real-Server campaign (C11/J-matrix) and
  real-provider qualification remain NOT_RUN; no publication performed.
- Pi bindings recorded under the pre-C2 identity formula (0.2.0.dev0
  era) revalidate honestly to `PROFILE_DRIFT` with the re-qualify
  action — no silent acceptance of a stale identity algorithm.
- POSIX qualification still staged (Windows-only evidence run).
