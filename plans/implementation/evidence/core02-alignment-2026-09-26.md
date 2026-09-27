# Evidence — Core 0.2.0 alignment suite run (2026-09-26)

**Test ID:** EV-CORE02-ALIGNMENT-2026-09-26
**Status:** PASS
**Scope:** full automated suite after adopting `nexus-connector-core`
0.2.0.dev0 (C1/PC00–PC14) — composition via `create_runtime`, shared
journal/ledger, portable build identities, passive shim/release
discovery and containment preflight.

## Environment

| Item | Value |
|---|---|
| OS | Windows 11 (win32), x86_64 |
| Python | 3.13.1 |
| Connector | okto-nexus-connector 0.1.0.dev0 (working tree, pin updated to core==0.2.0.dev0) |
| Core wheel | nexus_connector_core-0.2.0.dev0-py3-none-any.whl, SHA-256 `633483707aaf7eb7cfebdfee53f57e99ace45ae657017bf4a3dfa91783d8e253` (source HEAD `15772d3`) |
| Harnesses present | Pi 0.87.1, Codex CLI 0.157.0, Claude Code 2.1.282 (provider runs remain gated) |

## Connector changes for the Core update

1. `services/core_host.py` composes runtimes through the public
   `create_runtime` (PC06); the private `CopiedAdapterFactory` import is
   gone. One shared `SQLiteJournal` + one `SQLiteOwnedSlotLedger` per
   daemon (PC01 off-loop workers).
2. `BindingRecord` gained `candidate_build_identity` and
   `candidate_launch_script` (PC09/PC12); `candidate_for` revalidates
   fingerprint **and** build identity; Pi composite fingerprints work
   end-to-end.
3. Discovery integrates `resolve_windows_npm_shim` and
   `discover_pi_releases` passively (PC10); refused shapes yield nothing
   (verified: this host's npm shims are non-standard and honestly
   produce zero candidates).
4. `doctor` reports the containment preflight layer (PC11); `win32`
   `job_objects=ok` observed.
5. IPC dispatch logs internal errors with tracebacks (was silent
   `UNKNOWN`).

## Command

```
python -m pytest tests/ -q --tb=no
```

## Result

```
114 passed, 1 skipped, 2 warnings in 213.27s (0:03:28)
```

(1 skip: the POSIX-only shim path on Windows; 2 warnings are the
pytest-asyncio mark notice and an unraisable-thread cleanup notice from
the Core's off-loop executor teardown in one recovery test.)

## Layer breakdown

| Layer | Count | New coverage for Core 0.2.0 |
|---|---|---|
| unit | 55 | `tests/unit/test_core02_integration.py`: passive shims (valid/dynamic shapes), build-identity round-trip + drift detection, pre-release bindings still path-bound, containment preflight, Pi release layout with trusted roots, explicit selection attaches PC09 identity |
| contract | 35 | unchanged (frame/HTTP/bridge surfaces stable across the Core update) |
| integration | 20 | recovery suite now composes via `create_runtime`; daemon/two-server/logs/approvals flows unchanged and green |
| e2e | 5 | clean-venv install now pulls the 0.2.0 core wheel; packaging metadata pin verified |

## Built artifacts (this evidence run)

| Artifact | SHA-256 |
|---|---|
| `okto_nexus_connector-0.1.0.dev0-py3-none-any.whl` | `4a9928a7e9f6c3902e8124e8d787dac1652a1c627d979af2b91b8569ec6e3903` |
| `okto_nexus_connector-0.1.0.dev0.tar.gz` | `c810a2a58ee079212589fc43ae8cb391936fb83a2ccf0d452a6e60e749a00fb2` |

## Honest limits

- Same external blockers as before: the real Server campaign (C11/J)
  and real-provider qualification remain NOT_RUN.
- The `create_runtime` default `max_owned_sessions=32` is adopted from
  the Core's public composition (plan A.16's "8" was an engineering
  hypothesis; the Core owns runtime budgets now — see DECISIONS D13).
- POSIX qualification still staged (Windows-only evidence run).
