# Evidence — Core 0.2.3.dev0 alignment suite run (2026-09-26)

**Test ID:** EV-CORE023-ALIGNMENT-2026-09-26
**Status:** PASS
**Scope:** full automated suite after adopting `nexus-connector-core`
0.2.3.dev0 (C3 reaudit S01–S08 and C4 reaudit T01–T07, spanning versions
0.2.2.dev0 → 0.2.3.dev0).

## Environment

| Item | Value |
|---|---|
| OS | Windows 11 (win32), x86_64 |
| Python | 3.13.1 |
| Connector | okto-nexus-connector 0.1.0.dev0 (working tree, pin updated to core==0.2.3.dev0) |
| Core wheel | nexus_connector_core-0.2.3.dev0-py3-none-any.whl, SHA-256 `128911353f4bd0d673e0390092e569605eaedf0a9e15a834e9c634a49f01dfe1` (source HEAD `3d304f9`) |

## Connector impact assessment (C3+C4)

The campaigns are overwhelmingly Core-internal (single effective clock,
spawn-unit lease revalidation, fence-shared approval replies, lock-free
lease CAS, dedicated force/observation pools, byte-frontier dispatch
guards, artifact stat re-verification, force-before-audit containment,
bounded manifest enumeration). Connector surface:

1. **`create_runtime` gained only the additive `cleanup_budget_seconds`
   (default 5.0)** and now resolves one effective clock internally
   (S01) — the connector passes no clock and is unaffected; behavior
   verified by the integration suites.
2. **`CoreError.message` (S07, additive)**: `str(CoreError)` now carries
   the redacted human diagnostics instead of the code. The connector's
   `CoreError → ConnectorError` wrappers (`selection`, `probe`,
   `discovery`, `config` stages) surface those diagnostics with the
   stable `code` intact — an improvement, no change required.
3. **Build-identity algorithm advanced to `core.build_identity.v3`
   (S06/T06)** with the Pi 0.87.1 grant re-recorded. The connector
   recomputes identities through the Core's own functions, so
   revalidation uses v3 automatically; bindings recorded under v1/v2
   digests revalidate honestly to `PROFILE_DRIFT` with the re-qualify
   action (same declared policy as the previous alignments; no silent
   cross-version acceptance).
4. T04's DispatchGuards seam is additive with legacy factories untouched
   — the contract-test RecordingFactory keeps working (integration
   suites confirm).
5. Containment preflight shape unchanged; the structured
   missing-requirement map additionally travels in `CoreError.message`.

## Command

```
python -m pytest tests/ -q --tb=no
```

## Result

```
115 passed, 1 skipped (layered: unit 55+1s, contract 35,
integration 20, e2e 5)
```

## Built artifacts (this evidence run)

| Artifact | SHA-256 |
|---|---|
| `okto_nexus_connector-0.1.0.dev0-py3-none-any.whl` | `60d2b0156bf43b4bbb002d614460719d82d8a3b3beef28e05d6d9faa80eafd86` |
| `okto_nexus_connector-0.1.0.dev0.tar.gz` | `6ed106b088cc2e04ea6de05b3259cc18e5845bd35db7a11d662a25d6d244d7af` |

## Honest limits

- External blockers unchanged: real-Server campaign (C11/J-matrix) and
  real-provider qualification remain NOT_RUN; no publication performed.
- POSIX qualification still staged (Windows-only evidence run).
