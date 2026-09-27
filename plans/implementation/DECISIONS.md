# Decisions (ADR log) — okto-nexus-connector

Normative constraints come from `02_PLANO_NEXUS_CONNECTOR.md` r3
(`nxl-1-agent-centric-http-only-2026-09-25-r3`) and its Anexo A. This
log records the engineering decisions taken while implementing within
those constraints.

## D1 — Dependencies

`nexus-connector-core==0.1.0.dev0` (pinned dev wheel), `websockets`
(NXL client) and `httpx` (contract HTTPS). No MCP SDK at any layer; the
clean-venv audit enforces it (TC-01). No dependency on `okto_nexus`.

## D2 — IPC transport selection

AF_UNIX stream socket with 0o600 where supported (Linux; kept as the
primary on POSIX), otherwise loopback-only TCP with an ephemeral port
published in the per-user readiness file plus a 256-bit token required
in the first frame (`hello`). On Windows this loopback variant is the
*primary* and is qualified as private+authenticated; it is never bound
to external interfaces. PID alone is never identity: readiness carries
a process birth token and liveness requires an authenticated ping.

## D3 — Secret vault backends

OS keyring preferred when importable and viable; otherwise a
restricted-file store inside the per-user state directory, plaintext
under OS-account protection, **only** after explicit recorded approval.
`OKTO_NEXUS_CONNECTOR_VAULT=file` forces the file backend (containers,
tests) — the approval gate still applies. We never claim encryption we
do not have.

## D4 — One journal, one slot ledger, per-binding runtimes

The daemon shares one SQLite technical journal and one installation-wide
owned-slot ledger file; each binding gets its own `LocalRuntimeCore`
instance with its selected candidate and workspace root. ManagedSession
bookkeeping is namespaced by `(server_id, session_id)`; colliding raw
session ids across independent Servers surface as an explicit
`AMBIGUOUS_BINDING` instead of addressing the wrong session.

## D5 — Event ACK ordering

The Core journal is acknowledged **only after** the Server's
`event.ack` (trusted-host durable-ingress rule from the Core docs).
Without an ACK, events stay un-compacted and replay on reconnect;
dedup is Server-side by event identity.

## D6 — Remote operations authority

Server-initiated `operation.submit` frames rebuild the authorization
context from durable binding state plus the transport's connection
generation, and re-verify the semantic intent hash before the Core
admission. The frame codec's own enforcement is treated as the first
line; the handler check stays as defense in depth.

## D7 — Native factory seam

Production always launches through the Core's copied-adapter factory
(qualification allowlist included). `CoreRuntimeHost.build(factory=...)`
is an explicit trusted-host injection seam used by contract tests only;
a fake factory passing tests grants no provider qualification (TC-42
stays provider-gated).

## D8 — Error frames vocabulary

NXL `error` frames require the operation-stage enum; connector
component stages are preserved inside `corrective_action` as
`[stage: …]`, with `OUTCOME_UNKNOWN` for possible effects and `FAILED`
otherwise. No invented stages.

## D9 — Service managers

systemd **user** units (Linux; lingering reported honestly), LaunchAgent
(macOS) and a per-user ONLOGON scheduled task (Windows). Admin is never
required for common use; survival of terminal-close/logout/boot is
stated per mechanism rather than generalized. Unsupported platforms get
a prescriptive refusal with the foreground path.

## D10 — Lane attach race fix

`binding.attach` is only sent for lanes still registered after the
ticket fetch completes; removing a binding while attach is in flight
cannot resurrect the lane (found by the two-Server isolation test).

## D11 — Loopback proxy exemption

The HTTPS client disables environment-proxy trust for loopback targets
(development and contract peers) while keeping normal proxy behavior
for remote origins; `NO_PROXY` exclusions are honored.

## D13 — Core 0.2.0 adoption (C1/PC00–PC14)

The connector consumes `nexus-connector-core==0.2.0.dev0` and follows
the Core's updated public surface:

- **`create_runtime` is the only composition path (PC06).** The private
  `CopiedAdapterFactory` import was removed; the daemon builds every
  runtime through the public factory, keeping the documented
  `native_factory` seam exclusively for contract tests.
- **One shared journal + one shared owned-slot ledger per daemon (PC01).**
  Both now own an off-loop worker thread inside the Core; sharing a
  single instance per installation avoids per-binding executors and is
  the shape the Core's own consumer smoke demonstrates.
- **Portable build identities (PC09) travel with bindings.**
  `BindingRecord` records `candidate_build_identity` and
  `candidate_launch_script`; `candidate_for` revalidates both the
  path-bound fingerprint and the content identity, so moving an
  installation still breaks the local binding while content drift is
  caught even at the same path. Bindings created before 0.2.0 keep
  working (path-bound only).
- **Passive discovery helpers (PC10/RC-10-03).** `discover` integrates
  the Core's npm-shim resolver and Pi release-layout enumeration — both
  purely passive, never executing wrappers; refused shapes yield
  nothing and explicit `--executable` remains the guaranteed path.
- **Containment preflight (PC11) surfaced in `doctor`.** The binary layer
  reports the backend requirements; managed launches refuse closed via
  the Core's `PROCESS_CONTAINMENT_UNAVAILABLE` when they cannot be
  honored.
- Identifier policy (PC05), effect-frontier revalidation (PC03), EOF
  fencing (PC04), tombstone release (PC07) and lease-containment
  independence (PC02) are Core-internal and apply automatically.

## D15 — Core 0.2.1.dev0 adoption (C2 reaudit R01–R10)

- **R06 fix applied:** Pi build identities revalidate against the
  pi-coding-agent **package** root (`parents[2]`), exactly matching the
  Core's selection-time computation; a regression seed binds the two so
  any future root drift fails loudly. Pi now qualifies **only** via the
  portable identity (fingerprint stays the local binding proof) — the
  connector already threads `build_identity` end-to-end since D13.
- **Async journal lifecycle:** the daemon opens the shared journal via
  `open_journal` (off-loop schema setup) and closes journal/ledger via
  `aclose()` per the Core's PC01 transition notes.
- **R08 composed discovery:** `create_runtime(pi_install_root=, pi_node=)`
  is available for runtime-side discovery; the connector's daemon-less
  CLI keeps the Core's public standalone helpers
  (`discover_pi_releases`, `resolve_windows_npm_shim`) — same supported
  resolver, no wrapper execution either way.
- R01–R05, R07, R09, R10 are Core-internal or publish surfaces the
  connector adopts transparently (e.g. `CodexResumeGrant` is now public
  for the future resume seam; containment-gated probes surface their
  missing-requirement map through our typed errors).

## D16 — No publication

Wheel/sdist are built and hashed locally; no PyPI publication, no
remote repository creation — both require explicit authorization per
plan rules (A.1, A.17). (Also recorded as D12 before the Core-0.2.0
log insertion above.)
