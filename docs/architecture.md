# Architecture — okto-nexus-connector

The connector is the application that lives on the harness machines. It
imports canonical agent credentials, binds identity/harness/project and
administers managed runtimes **exclusively** through
[`nexus-connector-core`](../okto-nexus-connector-core). Remote control is
an outbound WSS channel (NXL r3); tools travel as **direct HTTP MCP**
between the harness's own client and the Nexus Server — the connector is
never in that path and contains no MCP implementation of any transport.

```text
  ┌────────────────────────── harness host ──────────────────────────┐
  │  CLI / TUI (observer + limited control)                          │
  │      │ authenticated IPC (unix socket / loopback+token)          │
  │  ┌───▼────────────────────────── daemon ─────────────────────┐   │
  │  │ identity vault │ state store │ runtime manager │ event    │   │
  │  │                │              │ (Core API)      │ bridge  │   │
  │  │ NXL transport (outbound WSS, lanes per binding) ─┘         │   │
  │  │ HTTPS client (contract routes, canonical key auth)         │   │
  │  └────────────────────────────────────────────────────────────┘   │
  │  ┌─── nexus_connector_core ───────────────────────────────────┐   │
  │  │ journal · kernel · adapters (Codex/Pi/Claude) · process    │   │
  │  └────────────────────────────────────────────────────────────┘   │
  │        ▲ native protocol (stdio of the runtime, NOT MCP)          │
  │  Codex app-server │ Pi RPC │ Claude stream-json                  │
  └───────────────────────────────────────────────────────────────────┘
        │ outbound WSS (control)            │ direct HTTP (MCP tools)
        ▼                                   ▼
              Nexus Server (canonical domain, MCP HTTP endpoint)
```

## Modules

| Module | Responsibility |
|---|---|
| `cli/` | Intent, protected input, selection, confirmation, human/JSON output |
| `daemon/` | Composition root, singleton lock with process identity, lifecycle |
| `ipc/` | Authenticated local control (hello-token before any effect) |
| `identity/` | Canonical key import, vault backends, /me validation |
| `transport/` | Contract HTTPS client + outbound NXL WSS client with lanes |
| `services/` | connect/bind flows, runtime manager, MCP client config, doctor |
| `storage/` | Atomic non-secret state; secrets only as vault references |
| `harness_config/` | Declarative direct-HTTP MCP client entry planning |
| `platform/` | State layout, process identity, OS service plans |

## Key invariants

1. **No MCP anywhere.** Structural tests (`tests/unit/test_boundaries.py`)
   reject MCP imports, symbols and subprocess spawning outside the
   platform layer; the wheel has no MCP dependency or entrypoint.
2. **No native duplication.** Every managed process is launched by the
   Core through the public `RuntimeCore` API (TC-20).
3. **Secrets stay in the vault.** The state file, journal, IPC frames,
   logs and child argv contain only `vault:`/`mcp-cap:` references; a
   redaction boundary scrubs connector-owned surfaces (TC-35).
4. **Authority comes from the Server.** Every mutable runtime intent is
   authorized through `intents:resolve`; `ExecutionContext` is built from
   that resolution, never from peer payloads (A.5, A.7).
5. **Honest uncertainty.** `OUTCOME_UNKNOWN` never becomes a retry;
   duplicate operation IDs return the known receipt; conflicting intents
   raise `OPERATION_CONFLICT` (A.9, verified against the Core journal).
6. **Outbound only.** The connector dials the Server; the harness host
   opens no listener beyond the per-user IPC endpoint (TC-15).

## Process model

One daemon per OS account + state directory (the local trust domain).
Multiple agents and at least two Server profiles share the daemon with
independent namespaces (`server_id → agent/binding → session`). The
technical journal and the installation-wide owned-slot ledger are shared
files owned by the daemon. CLI processes are observers: closing them
never stops the daemon or unrelated sessions (TC-09).

## R4 development lease exchange

The R4 control path is under development and is separate from the historical
daemon flow above. Core `0.2.30.dev0` supplies a runtime-owned request nonce,
monotonic t0, immutable full authority scope and an application ACK.

After `negotiate_r4_control`, `apply_r4_lease` exchanges a correlated grant
over the same authenticated socket. The caller must exclusively own its
reader during this phase. The function calls `RuntimeCore.install_r4_lease`
before sending `lease.applied`; receiving `lease.granted` does not authorize
the Connector to manufacture a context or extend a deadline.

Operation dispatch must obtain its context with
`runtime.r4_operation_context(frame, connection_id=..., connection_generation=...)`.
The Core checks the complete scope, current grant, action and expiry. Live
renewal uses the durable Core CAS. Revocation through `revoke_r4_lease` fences
native effects immediately and acknowledges only a confirmed application.

The transport contract test uses the negotiated state and installed Core.
The coordinated Nexus vertical test exercises lease installation followed
by five Core operations and receipt publication. The subsequent canonical
control tests use actual Server issuance, admission and dispatch services,
with synthetic qualification and native peers. Durable grant recovery,
socket multiplexing and daemon integration remain open. A lost application
ACK requires reconciliation; it does not permit another initial grant or
another operation ID. The executable R4 bundle and host readiness gates
remain disabled.

## R4 close receipt policy

Core 0.2.30.dev0 verifies `reason`, `drain_seconds` and `interrupt_seconds`
against the native journal hash before `publish_core_close_receipt` sends
the R4 receipt. Hosts use `r4_close_operation(frame)` to retain all three
fields. An observation timeout or canceled caller does not terminate the
owned producer; query the same operation ID for its eventual result.

The coordinated installed test exercises canonical close after productive
lease expiry, cancel/replay and transactional receipt/session projection.
Full Connector source regression with the same Core: 241 passed, 2 skipped.
See `plans/implementation/evidence/r4-close-policy.json`. This increment
does not wire close into the R4 daemon or qualify a native provider.

With Core 0.2.28, an unchanged-scope renewal may remain pending while an
already authorized interrupt, close or strictly negative decision proceeds.
The same source connection, scope and grant and an action retained by the
pending grant are required. Reconnect, changed scope and revocation remain
fenced. The Core reserves productive admission before releasing session
locks for storage, and late renewal cannot revive a closing session.
See `plans/implementation/evidence/r4-pending-containment.json` for this
increment's separate consumer campaign and limits.

## R4 native domain backend

The public transport.native_actions.native_action_bridge factory binds a
Server-issued native capability to the current Core runtime incarnation.
The HTTP backend preserves the original action ID and claim idempotency key,
refuses redirects, bounds request/response JSON, and never automatically
retries an uncertain mutation. The capability secret stays in the trusted
backend; the Core receives the scoped reference and current authority.

The installed integration campaign exercises this backend and the Nexus
embedded backend against the same canonical handoff services. Automatic
daemon launch, Pi socket ownership and capability renewal remain separate
host integration work; remote R4 readiness remains false.

## Pi native action ownership

R4LaunchSetup may carry a trusted native_action_factory composed from the
approved session capability. CoreRuntimeHost passes the resulting owned
launch callback through the public Core create_runtime API. The factory is
session scoped, Pi only, and cannot change on runtime reuse.

The execution owner fences native ingress when closing a session. Shutdown
closes ingress before draining Core, and retains the runtime and shared
stores when a canonical domain producer is still pending. Socket timeout or
caller cancellation does not cancel a producer or permit a new action ID.
Automatic configuration, capability renewal and nonempty reconciliation
remain required before remote execution readiness can be enabled.


## R4 session credential issuance ownership

State schema 7 adds non-secret session capability reservation records. A
reservation is keyed by Server, executor, session and audience and binds the
complete opening content, revisions and requested actions. Its request ID and
deterministic vault handle are committed before HTTP.

SessionCapabilityOwner keeps one bounded producer per reservation. Canceling
an observer or close waiter does not cancel HTTP, result recording or vault
persistence. A returned secret is stored before the launch configuration is
released. CapabilityLaunchProvider composes this service through the existing
R4ExecutionOwner launch port and rechecks the host's current-lane/configuration
guard around asynchronous work.

Response loss can replay only the original request identity to obtain the
Server's recovery metadata. The owner never substitutes a capability or
converts missing material into a new issuance. Another process recording
MATERIAL_UNAVAILABLE cannot erase a STORED result. State files contain no
credential material; the vault remains subject to the existing OS-keyring or
explicitly approved file-backend policy.

The in-process cached receipt retains its original monotonic deadline. A
restart preserves the vault secret and reservation but requires authority
reconciliation before reuse; it does not reanchor a persisted TTL. Automatic
approved-profile composition, safe rehydration under a reconciled lease,
capability renewal and terminal reservation retention/cleanup remain pending.
The durable capacity is 128 reservations by default and fails before HTTP.
This is an incremental issuance component, not complete daemon onboarding.
