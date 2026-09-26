# Evidence — local suite run (2026-09-26)

**Test ID:** EV-LOCAL-SUITE-2026-09-26
**Status:** PASS (automated contract/unit/integration/e2e layers)
**Scope:** everything executable without a real Nexus Server build,
real provider credentials or additional hosts.

## Environment

| Item | Value |
|---|---|
| OS | Windows 11 (win32), x86_64 |
| Python | 3.13.1 |
| Connector | okto-nexus-connector 0.1.0.dev0 (this working tree; first commit pending) |
| Core wheel | nexus_connector_core-0.1.0.dev0-py3-none-any.whl, SHA-256 `ac376605217bc8236a6306ca303499e1923fd68b56a7b5586799ec95aeb1553f` (source repo HEAD `706b16a`) |
| Harnesses present | Pi 0.87.1, Codex CLI 0.157.0, Claude Code 2.1.282 (not exercised by these tests — provider runs are separately gated) |
| Peers | `tests/fakes/http_peer.py` (A.5 contract routes) and `tests/fakes/wss_peer.py` (NXL r3 server side) — contract fakes, not Servers |

## Command

```
python -m pytest tests/ -q --tb=no
```

## Result

```
105 passed, 1 warning in 208.12s (0:03:28)
```

(The warning is pytest-asyncio's unknown-mark notice for `process`/
`packaging` on the older plugin layout; marks are registered in
pyproject.)

## Layer breakdown

| Layer | Files | What it proves |
|---|---|---|
| unit | 49 tests | state/vault semantics, redaction, IPC framing, lock identity, direct-HTTP MCP config with CAS/backup, OS service plans, structural no-MCP/no-Popen/no-server-deps boundaries |
| contract | 35 tests | HTTPS client vs fake peer (me/hint/redirect/tickets/capability), identity import flows, NXL WSS client vs fake peer (welcome, lanes, ack watermarks, priorities, reconnect, receipts), Pi native bridge scope/lease gates |
| integration | 19 tests | daemon composition (IPC auth, runtime lifecycle, two servers isolation, restart/reconcile/lease-clock/journal-full shapes, log-follow independence), real subprocess singleton/stop, honest failure of unqualified binaries |
| e2e | 8 tests | real CLI processes against fake peers: identity add/list/show, guided connect with piped protected entries, daemon ensure/stop, doctor, mcp-config plan/apply/remove preserving third parties, clean-venv wheel install with dependency audit |

## Honest limits

- The fake peers are contract peers; passing them does **not** close any
  J-case (multi-host/provider/OS) — those remain NOT_RUN pending the
  joint campaign (C11 blockers recorded in the status file).
- `runtime start` against the real qualified Codex/Pi/Claude builds was
  not executed here (provider credentials/cost gating); the e2e covers
  the honest failure path for unqualified binaries.
- Windows-only execution in this run; the POSIX matrix (WSL2/Linux) is
  prepared in CI but hosted runners remain unexecuted (billing decision,
  same policy as the Core repo).

## Built artifacts (this evidence run)

| Artifact | SHA-256 |
|---|---|
| `okto_nexus_connector-0.1.0.dev0-py3-none-any.whl` | `fb209b09948e8da7cedb387c3a7e17adc9414b4b43fa0c616de4c279e1805090` |
| `okto_nexus_connector-0.1.0.dev0.tar.gz` | `3f6da1d966f4a59a3613891830081f7f9f5f85c2d1456993f25aaed22efc6736` |

Built with `python tools/build_artifacts.py` (normalized local build);
not published anywhere.
