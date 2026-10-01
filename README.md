# okto-nexus-connector

Remote harness connector application for [Okto Nexus](https://github.com/OktoLabsAI/okto-nexus):
the lightweight CLI + daemon that lives on the machines where Codex,
Pi and Claude Code run, imports canonical agent credentials, binds
identity/harness/project and administers managed runtimes through
[`nexus-connector-core`](../okto-nexus-connector-core).

- Distribution `okto-nexus-connector`, import `okto_nexus_connector`,
  Python ≥ 3.11, entry point `okto-nexus-connector`, consuming
  `nexus-connector-core==0.2.8.dev0` (public `create_runtime`
  composition).
- **No MCP implementation of any transport.** Harness MCP clients talk
  directly to the Nexus Server over HTTP; the connector only *configures*
  those clients. Stdio of the native runtime protocols is not MCP.
- **No Nexus user login.** The canonical agent key already used by MCP
  authenticates onboarding; the OS account is only a local boundary.
- Status: `0.1.0.dev0` — development build; see
  [plans/implementation/status](plans/implementation/IMPLEMENTATION_STATUS.md)
  for gates, evidence and honestly-blocked items.

## What it does

| Area | Delivered by |
|---|---|
| Identity | Protected import (masked/stdin/MCP entry), /me validation with hint comparison, OS keyring with explicit restricted-file fallback, rotation with epoch bump |
| Daemon | One instance per OS account + state dir under an OS lock with process birth identity; readiness = authenticated IPC ping; bounded drain with per-session reports |
| IPC | Unix socket (POSIX) / loopback+token (Windows), hello-authentication before any effect, streamed log follow |
| Transport | Contract HTTPS client (plan A.5 routes) + outbound NXL r3 WSS: lanes per binding with scoped tickets, heartbeats, bounded priority queues, generation fencing, reconnect+reconcile |
| Runtimes | Start/reuse/new-session, submit, interrupt (keeps runtime) vs stop (closes owned resources), logs, inspect — all via the Core's public API with the Server-authorizing `intents:resolve` flow |
| MCP config | Declarative direct-HTTP entries (Codex TOML / Claude JSON) with plan/apply/backup/CAS/ownership; tools-only needs no daemon; Pi gets the non-MCP native bridge |
| Ops | `doctor` layered diagnostics, `service install` per-OS plans with honest survival matrix, redacted exports |

## Quickstart

```bash
# on the harness machine, from the Server screen's proposed command:
okto-nexus-connector connect --server https://nexus.example --agent ag_123
# → paste the canonical key, pick the local harness, confirm the binding

okto-nexus-connector runtime start codex      # in the project directory
okto-nexus-connector runtime status
okto-nexus-connector runtime logs <session> --follow
```

## Development

```bash
python -m pip install -e .[test]
python -m pip install <core wheel>            # pinned nexus-connector-core
python -m pytest -q                           # unit + contract + integration + e2e
python -m build                               # wheel + sdist
```

Guides: [architecture](docs/architecture.md) ·
[runbook](docs/runbook.md) · [threat model](docs/threat-model.md).
Implementation status, backlog, decisions, the acceptance matrix and the
evidence log live under [plans/implementation/](plans/implementation/).

## License

Elastic License 2.0 with the *SaaS and Competing Service Definition*
addendum — Copyright 2026 Okto Labs (same licensor decision as
`okto-pulse-core` / `nexus-connector-core`). See [LICENSE](LICENSE).

## Persisted R4 executor discovery

After importing an identity and registering the executor, configure discovery
for that Server. This replaces the previous discovery configuration for that
executor; it does not approve a workspace binding or authorize a runtime.

~~~powershell
okto-nexus-connector executor configure-discovery --server-id SERVER_ID --harness-root "C:/Harnesses"
okto-nexus-connector discover --server-id SERVER_ID
okto-nexus-connector executor show SERVER_ID
~~~

The --harness-root option is repeatable (up to 32 directories). It approves
passive discovery of binaries found on PATH inside those directories; it does
not add directories to PATH. Paths must be existing absolute paths. Directory
identity is checked again before discovery and daemon control operations.

For Pi release layouts, also supply --pi-install-root and --pi-node together.
The named Node file may be outside the release directory; only that exact file
is approved in addition to the release root. Discovery never executes it.
Full Node/CLI candidate identity is preserved through the public Core facade.

The daemon reads this configuration from its registered Server executor. A
configuration change invalidates its current control snapshot and requires
reconnection/reconciliation. Runtime approval and lease checks still apply.
Physical path replacement is refused until the local operator configures it
again. To clear discovery roots and the Pi pair:

~~~powershell
okto-nexus-connector executor configure-discovery --server-id SERVER_ID
~~~

The --json and --non-interactive flags are available as global options.
Configuration contains local paths and file identity only; remote inventory
remains path-free. State schema 9 adds discovery configuration. Schema 8
records migrate with no implicit discovery choices; older clients refuse
schema 9 state. Runtime CLI admission and final release qualification remain under integration.

### Launch consent and realization

Stage explicit local launch consent using an imported identity on the registered
Server. The configuration digest binds the identity, executor, adapter, consent
ID, profile revision, protected secret references and optional provider login
directory. This command does not read credentials or start a provider.

~~~powershell
okto-nexus-connector executor configure-launch --identity SUBJECT --harness codex_app_server --local-consent-id CONSENT_ID --profile-revision 1 --provider-home "C:/Users/USER"
~~~

Use --secret-ref NAME=vault:HANDLE (repeatable) when a protected provider secret
reference is needed. Plaintext credentials are refused. The provider home is
optional and must be explicitly chosen if an existing login is to be used.

Start the daemon so it publishes the configured inventory. Select the opaque
candidate_ref and executor_revision returned by the discovery preview:

~~~powershell
okto-nexus-connector daemon start
okto-nexus-connector discover --server-id SERVER_ID
okto-nexus-connector executor realize --identity SUBJECT --client-intent-id REALIZATION_INTENT --harness codex_app_server --candidate-ref CANDIDATE_REF --inventory-revision INVENTORY_REVISION --configuration-digest CONFIGURATION_DIGEST --project "D:/Projects/Workspace" --label "Workspace"
~~~

The realization command sends an authenticated local IPC request to the daemon.
The daemon owns the scoped bootstrap ticket and serializes realization publication
with ticket rotation. It remains the inventory publisher, checks its authenticated
inventory and publishes only opaque realization evidence. Disconnecting the CLI
does not cancel the retained publication; reuse the same intent to recover its result.
Use --workspace-id to select an existing logical workspace. Reuse the same
client intent and identical arguments after a lost response. Local mappings are
persisted before HTTP and acknowledged after scope checks. No path or credential
material is included in the realization response.

The acknowledged realization remains pending binding approval. Use the binding commands below to obtain and apply a reviewable proposal.
Runtime CLI admission and real remote provider qualification remain under
integration; these commands do not imply runtime readiness.

### Review and apply an R4 binding

Prepare a binding from the acknowledged realization. The local alias and client
intent are persisted before contacting the Server. Identical retries recover
the same proposal.

~~~powershell
okto-nexus-connector bind prepare --identity SUBJECT --realization-ref REALIZATION_REF --alias assistant --client-intent-id PREPARE_INTENT
okto-nexus-connector bind show assistant
~~~

Review the returned proposal, including its approved_diff_hash and required
approvals. If operator approval is required, obtain that approval through the
Nexus operator surface and supply its explicit proof reference:

~~~powershell
okto-nexus-connector bind apply --identity SUBJECT --prepare-intent-id PREPARE_INTENT --client-intent-id APPLY_INTENT --approved-diff-hash REVIEWED_HASH --operator-proof-ref APPROVAL_REF
okto-nexus-connector bind list
~~~

The proof reference does not grant permission by itself; the Server verifies
the operator decision. Apply persists its exact intent before HTTP. After a
lost response, reuse the same IDs, hash and proof reference. A different intent
cannot silently replace a pending application.

The acknowledged R4 binding and its local realization mapping commit atomically.
This does not create a legacy binding, start a provider, grant runtime execution
or infer an applied lease. Schema 10 stores these durable binding intents;
older schemas migrate with no implicit proposals or approvals. R4 runtime CLI
admission is the next integration step.
