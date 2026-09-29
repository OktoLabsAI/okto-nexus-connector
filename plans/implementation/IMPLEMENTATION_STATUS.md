# Implementation status — 2026-09-28 (CN1 corrected, Core 0.2.10.dev0)

Connector `0.1.0.dev0`. Baseline: this repository, consuming the Core
development wheel `nexus_connector_core-0.2.8.dev0` (SHA-256
`6f4823f348732801cbe93318efbb2efd339e066f268dfd617d378adc9de0e23a`,
source HEAD da campanha C9). **CN1 (2026-09-28):** a auditoria independente
(A01–A17) foi corrigida na íntegra — ver
[plans/correction-connector-01](../correction-connector-01/evidence/cn1-correction-2026-09-28.md);
o pin avançou para 0.2.10.dev0 (C10+C11 adotados).

The full automated suite passes locally on Windows 11 / Python 3.13.1:
**122 passed, 1 skipped**
([alignment evidence](evidence/core028-alignment-2026-09-26.md)). The
joint multi-host/real-Server campaign (C11 / J-matrix) remains
**blocked** below — mocks never close those gates.

| Phase | Status | Evidence / next gate |
|---|---|---|
| C00 | DONE | Scaffolding, threat model, fakes, staged CI, plans artifacts; isolated install proven (TC-01/TC-02 by test_boundaries + packaging e2e) |
| C01 | DONE | Vault (keyring + approved restricted-file), protected import with /me + hint comparison, rotation with epoch, local-vs-central removal (TC-03..TC-06) |
| C02 | DONE | Daemon singleton under OS lock + birth token, authenticated IPC (unix/loopback+token), readiness = ping, bounded drain with honest reports (TC-07..TC-10) |
| C03 | DONE | Guided connect with aggregated consent, idempotent re-runs, cwd root guard, provider login stays local (TC-11..TC-14) |
| C04 | DONE | Contract HTTPS client + outbound NXL r3 WSS with lanes/tickets/watermark ACK/priorities/reconnect/generation fencing against the fake peers (TC-15..TC-19) |
| C05 | DONE | Runtime lifecycle strictly via the Core public API with Server-authorized contexts; receipts stable; interrupt/stop distinct; two-Server isolation (TC-20..TC-23, TC-41 contract level) |
| C06 | DONE | Direct-HTTP MCP client config with CAS/backup/ownership; tools-only without daemon; Pi non-MCP bridge; no MCP surface in the package (TC-24..TC-27, J16/J33 shapes) |
| C07 | DONE | Full CLI table with --json/--non-interactive, stable exit codes, layered doctor (TC-28..TC-30) |
| C08 | DONE | Restart/reconcile/no-replay, lease clock fence, journal-full honesty, unauthorized-IPC denial, redaction boundary (TC-31..TC-35 shapes; OS-level crash cuts inherit Core K10 evidence) |
| C09 | DONE | Per-OS service plans with consent + honest survival matrix; state schema guard; owned-fields-only removal (TC-36..TC-39; POSIX boot qualification staged NOT_RUN) |
| C10 | DONE | Wheel+sdist built and dependency-audited in a clean venv (against core 0.2.0); artifacts hashed; no publication performed (TC-40/TC-42 partial: provider qualification remains blocked) |
| C11 | BLOCKED | Requires the adapted Nexus Server (r3 contract: `/v1/connections/*`, `/v1/runtime/*`, WSS link) and authorized hosts A/B/C. All J-cases NOT_RUN. |

## Explicit blockers (external)

1. **Nexus Server not adapted** — `okto-nexus` does not yet implement the
   A.5 contract routes, MCP-HTTP-only topology or the NXL link endpoint.
   Every real-Server scenario (J01–J34, TC-43/44/45) stays NOT_RUN; the
   fake peers are contract fixtures, not servers.
2. **No authorized multi-host campaign** — topology A/B/C (Linux/Windows
   heterogeneity) not provisioned; TC-42 real-provider qualification
   (Codex 0.157.0 / Pi 0.87.1 / Claude 2.1.282 are installed locally but
   runs are credential/cost-gated per plan rules).
3. **Hosted CI unexecuted** — same billing decision as the Core repo;
   the workflow is staged and read-only.

## Known honest limits in this build

- Windows is the only OS exercised in this evidence run; POSIX paths are
  implemented (unix sockets, systemd-user, POSIX permissions) but their
  qualification is staged, not claimed.
- `attach` mode is inherited from the Core's unqualified POSIX substrate
  and intentionally not exposed in the CLI beyond diagnosis.
- The TUI dashboard is a plan-optional surface; only the CLI exists.
- In-process daemon teardown in tests uses a bounded stop guard; the
  production path (subprocess daemon stop) is covered by real-process
  tests.
