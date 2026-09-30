# Implementation status — 2026-09-29 (CN5 corrected, 0.5.0.dev0)

## R4 session capability consumer — 2026-09-30

[The capability client](R4_SESSION_CAPABILITIES.md) validates exact opening
scope, audience, actions, origin and deadline, keeps secrets out of its DTO
representation, and retains safe replay recovery metadata. Final source
regression: 343 passes and two existing skips. The daemon's durable launch
configuration and MCP/native handler integration remain pending; no gate closed.

## R4 automatic control startup — 2026-09-30

[Daemon startup](R4_DAEMON_STARTUP.md) connects explicit registrations to the
real daemon lifecycle, fresh bootstrap, durable inventory sequence and control
negotiation. Empty recovery reads the Core journal/ledgers; nonempty history
stays pending. The Server now fences inventory producer handoff by the
current reconciled channel. Results: 325 regression passes (two existing
skips), 65 directed, 107 Nexus R4 and 109 installed, with overlap. Evidence:
[evidence/r4-startup-report.json](evidence/r4-startup-report.json). Automatic
lanes/runtime composition and nonempty recovery remain incomplete; no product
gate is closed.


## R4 durable executor registration — 2026-09-30

[Executor registration](R4_EXECUTOR_REGISTRATION.md) adds schema 5 persisted
registration intent/result, CAS against the selected identity/profile and a
process-local bootstrap ticket API. The CLI registers, lists and queries the
same executor; response loss reuses the original intent. `identity add` now
persists its authenticated Server profile. Evidence is recorded in
`evidence/r4-registration-report.json`. Automatic R4 daemon startup and live
credential/lease renewal remain pending; no gate is closed.

## R4 daemon operation consumers — 2026-09-30

[Execution ownership](R4_EXECUTION_OWNER.md) moves canonical operation
translation into daemon-owned producers with independent productive/control
consumers. It installs the initial Core lease before opening, translates seven
actions through public Core APIs and publishes the same operation's receipt.
Cancelled observers retain producers; failure fences the channel without a
second native attempt. Tests and limitations are recorded in
`evidence/r4-execution-report.json`. Automatic startup, credentials/renewal,
production environment composition and reconciliation remain incomplete.

## R4 physical selection — 2026-09-30

[Approved physical selection](R4_PHYSICAL_SELECTION.md) adds schema 4 binding
mappings, replay without downgrade, strict opaque-reference resolution and
Core host composition from revalidated local evidence. The public loopback
journey now uses the persisted resolver through the five-action cycle.
Results are in `evidence/r4-selection-report.json`; daemon composition,
approved-revision refresh and final product acceptance remain open.


## R4 connection ownership — 2026-09-30

The [R4 connection owner](R4_CONNECTION_OWNER.md) now correlates lease/attach
replies through one reader, bounds operation reservations and preserves lease
installation producers under cancellation. An ordered replay of the same lease
request confirms the Server ACK commit before execution can publish receipts
on another channel. Contracts and the real loopback WSS journey are recorded
in `evidence/r4-connection-report.json` and the Nexus coordinated report.
The daemon composition, physical resolver and nonempty reconciliation remain
pending. No R4 product gate is closed. Sections below are historical baselines.


Connector `0.1.0.dev0`. Baseline: this repository, consuming the Core
development wheel `nexus_connector_core-0.2.8.dev0` (SHA-256
`6f4823f348732801cbe93318efbb2efd339e066f268dfd617d378adc9de0e23a`,
source HEAD da campanha C9). **CN1 (2026-09-28):** a auditoria independente
(A01–A17) foi corrigida na íntegra — ver
[plans/correction-connector-01](../correction-connector-01/evidence/cn1-correction-2026-09-28.md);
o pin avançou para 0.2.10.dev0 (C10+C11 adotados). **CN2 (2026-09-29):** a reavaliação independente (N01–N09) foi corrigida e o Connector bumpou para 0.2.0.dev0 — ver [plans/correction-connector-02](../correction-connector-02/evidence/cn2-correction-2026-09-29.md); remote runtime.open permanece UNSUPPORTED declarado até o gate vertical. **CN3:** os seis grupos da reavaliação (gerações/revisões, reconcile estrangeiro, quota de admissão, histórico a frio, ACK duplicado, colisão de config MCP) foram corrigidos — ver [plans/correction-connector-03](../correction-connector-03/evidence/cn3-correction-2026-09-29.md). **CN4:** aprovação autorizada pelo Server antes do Core (tradução approve→accept/deny→decline, chave completa até binding/sessão, fases com tombstone idempotente), ACK do lote certo no EventBridge (alvo = watermark do lote; obrigação distinta para confirmação pendente no Core), home MCP por digest versionado com marker validado antes de escrever, inventário fiel (candidato Core completo + revisão inv2 de toda a evidência, mesma função de serviço na CLI e no IPC) e rotação real de lanes no reload com expiração de ticket observada — ver [plans/correction-connector-04](../correction-connector-04/evidence/cn4-correction-2026-09-29.md). **CN5:** recebimento de aprovação preserva a proposta operacional (DTOs com projeção redigida separada), aplicação nativa obrigatoriamente escopada por SessionKey/gerações, decisão com produtor próprio sob shield do waiter (fases terminais tipadas, `approvals.status`), rotação de lane com gerações de attach e envelope interno, e publicador de eventos retomado por reconexão após falha transitória — ver [plans/correction-connector-05](../correction-connector-05/evidence/cn5-correction-2026-09-29.md).

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
