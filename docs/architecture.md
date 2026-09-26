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
