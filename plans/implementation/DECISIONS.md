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

## D12 — No publication

Wheel/sdist are built and hashed locally; no PyPI publication, no
remote repository creation — both require explicit authorization per
plan rules (A.1, A.17).
