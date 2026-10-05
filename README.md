# okto-nexus-connector

Remote harness connector application for [Okto Nexus](https://github.com/OktoLabsAI/okto-nexus):
the lightweight CLI + daemon that lives on the machines where Codex,
Pi and Claude Code run, imports canonical agent credentials, binds
identity/harness/project and administers managed runtimes through
[`nexus-connector-core`](https://github.com/OktoLabsAI/okto-nexus-connector-core).

### Configure an outbound proxy

```sh
okto-nexus-connector proxy set --url http://proxy.example.com:8080 --no-proxy ".internal.example.com"
okto-nexus-connector proxy show
okto-nexus-connector reach --server https://nexus.example.com
```

For an authenticated proxy, set a protected environment variable containing
`http://user:password@proxy.example.com:8080` (URL-encode reserved characters),
then run `okto-nexus-connector proxy set --url-env NEXUS_PROXY_URL`.
Only the variable name is saved; its value is never included in `proxy show`.
The variable must also be available to the daemon's OS account/service.

Settings apply to all Nexus HTTP and WebSocket connections in the selected
`--state-dir`, including `reach`, onboarding, and daemon reconnects. Restart
the daemon to replace existing connections, or after changing environment
variables: `daemon stop`, then `daemon start`. Proxy configuration is local
and is not part of portable harness exports. Harness provider and native MCP
network traffic use the harness's own network configuration.

Without saved settings, the Connector uses environment/system proxies:
`HTTPS_PROXY` for HTTPS/WSS, `HTTP_PROXY` for HTTP/WS, then `ALL_PROXY`.
`NO_PROXY` and `--no-proxy` accept comma-separated hosts, domain suffixes,
host:port entries, or `*`. Loopback destinations always connect directly.
HTTP and HTTPS proxies with optional Basic authentication are supported;
SOCKS and integrated NTLM authentication are not supported.
TLS verification remains enabled; corporate trust roots can be provided
through `SSL_CERT_FILE` or `SSL_CERT_DIR`.

Use `proxy set --direct` to disable proxy use, or `proxy clear` to restore
environment/system discovery. A missing configured secret variable fails
the connection without silently falling back to a direct connection.

### Check Server reachability

```sh
okto-nexus-connector reach --server https://nexus.example.com
okto-nexus-connector --json reach --server https://nexus.example.com
```

`reach` calls the public `GET /v1/reach` endpoint before identity setup or daemon
startup. It reports `server_version`, `server_core_version`,
`minimum_cli_version`, the local `cli_version`, and `latency_ms`. A successful
probe exits with code 0; a connection failure exits with code 3. Remote origins
use HTTPS; loopback HTTP is supported for local development. The Core version
is `null` when the optional Core distribution is absent on the Server.

Runtime message delivery and automatic recovery use Core's shared defaults.
With a Server that supports automatic inventory revalidation, unchanged selected
installations remain usable after Core upgrades or unrelated inventory changes.
The Connector preserves the original binding and local consent, checks the exact
selected executable and workspace at launch, and reuses version observations only
for an identical source on the same platform. A changed executable or execution
contract still requires review; a Core version change alone does not.
The daemon reconnects automatically and verifies retained receipts, resource
ownership and event history before enabling execution. Five unsuccessful
recovery attempts produce `RECOVERY_ATTENTION_REQUIRED` in executor status;
restart the daemon after resolving the reported condition to retry. Transport
reconnects use a separate backoff. Previously submitted work is never replayed
by the recovery supervisor. The `configure` wizard and JSON imports use the
same automatic-message default as Nexus.

**Managed execution hosts currently require Windows or Linux.** Native macOS
execution is unsupported; see [platform support](#platform-support-before-installation).

- Distribution `okto-nexus-connector`, import `okto_nexus_connector`,
  Python ≥ 3.11, entry point `okto-nexus-connector`, consuming
  `nexus-connector-core==0.0.1` (public `create_runtime`
  composition).
- **No MCP implementation of any transport.** Harness MCP clients talk
  directly to the Nexus Server over HTTP; the connector only *configures*
  those clients. Stdio of the native runtime protocols is not MCP.
- **No Nexus user login.** The canonical agent key already used by MCP
  authenticates onboarding; the OS account is only a local boundary.
- First release version: `0.0.1`; see
  [plans/implementation/status](plans/implementation/IMPLEMENTATION_STATUS.md)
  for gates, evidence and honestly-blocked items.

## Platform support before installation

| Executor platform | Managed native execution |
|---|---|
| Windows | Job Object backend; requires successful containment preflight and a qualified provider build |
| Linux | procfs, child tracking, pidfd and subreaper backend; requires successful preflight and a qualified provider build |
| macOS | Unsupported: no qualified Core containment backend; managed launches are refused |

Installation, passive discovery and an available macOS keyring do not qualify
managed execution. The final Windows/Linux provider matrix remains under
validation; platform backend availability alone is not release acceptance.
On a Mac, use a separate Windows/Linux executor or a Linux VM with its own
workspace, provider installation and credentials. Run `doctor` inside that
executor and verify every containment requirement. Containers may restrict
required kernel capabilities and are not automatically qualified. A Linux
guest cannot manage or attach to a native macOS provider process.
See the [platform runbook](docs/runbook.md#unsupported-executor-platform).
Future native macOS containment remains an open design decision tracked in
[issue #1](https://github.com/OktoLabsAI/okto-nexus-connector/issues/1); no native
macOS release support is promised by the current build.

## What it does

| Area | Delivered by |
|---|---|
| Identity | Protected import (masked/stdin/MCP entry), /me validation with hint comparison, OS keyring with explicit restricted-file fallback, rotation with epoch bump |
| Daemon | One instance per OS account + state dir under an OS lock with process birth identity; readiness = authenticated IPC ping; bounded drain with per-session reports |
| IPC | Unix socket (POSIX) / loopback+token (Windows), hello-authentication before any effect, streamed log follow |
| Transport | Authenticated R4 HTTP management and outbound NXL R4 WSS: scoped tickets, binding lanes, generation fencing, reconnect/reconcile and retained publication |
| Runtimes | Canonical start/reuse, submit, steer, interrupt and close intents; daemon-owned Core execution; retained operation/session reads from the Server |
| MCP config | Declarative direct-HTTP entries (Codex TOML / Claude JSON) with plan/apply/backup/CAS/ownership; tools-only needs no daemon; Pi gets the non-MCP native bridge |
| Ops | `doctor` layered diagnostics, `service install` per-OS plans with honest survival matrix, redacted exports |

## Quickstart

Use the canonical R4 flow on the execution host. Import the existing agent key
through the masked prompt, then register this host:

```bash
okto-nexus-connector identity add --server https://nexus.example --agent AGENT_ID --alias SUBJECT
okto-nexus-connector executor register --identity SUBJECT --label "Execution host"
okto-nexus-connector executor list
```

Use the returned Server ID to [configure discovery](#persisted-r4-executor-discovery),
[observe the selected installation and stage consent](#launch-consent-and-realization),
then [review and apply its binding](#review-and-apply-an-r4-binding).
Only after binding approval and execution authorization, use the explicit alias
with the [canonical runtime commands](#canonical-runtime-intents).
Registration alone does not approve a workspace or start a provider. The legacy
`connect` helper is not the R4 executor onboarding flow.

## Development

```bash
python -m pip install --find-links vendor/wheels ".[test]"
python -m pytest -q                           # unit + contract + integration + e2e
python -m build                               # wheel + sdist
```

Guides: [architecture](docs/architecture.md) ·
[runbook](docs/runbook.md) · [threat model](docs/threat-model.md).
Implementation status, backlog, decisions, the acceptance matrix and the
evidence log live under [plans/implementation/](plans/implementation/).

## License

Elastic License 2.0 with the Okto Labs SaaS/Branding Addendum —
Copyright 2026 Okto Labs. The full [LICENSE](LICENSE) is identical to
the Okto Nexus license and is authoritative. See [CONTRIBUTING.md](CONTRIBUTING.md)
for branch and review rules.

## Persisted R4 executor discovery

`discover` inventories provider installations on the computer running the
Connector. It does not search the LAN for Nexus servers or other Connectors.
The default display is a compact table: one row per known managed harness,
the number of installations found and a readable technical status. Multiple
installations are counted separately; mixed readiness requires reviewing the
details. A ready technical status is not runtime authorization. To inspect paths,
versions, fingerprints, selection references and all diagnostic reasons:

~~~powershell
okto-nexus-connector discover
okto-nexus-connector discover --verbose
okto-nexus-connector --json discover
~~~

`--verbose` also works with `--server-id` and `--harness`. JSON output retains
the complete structured inventory regardless of `--verbose`.
`Host containment unavailable` means the Connector cannot own managed processes
on this host; the `Found` count still reports detected installations. An
`untrusted` suffix means explicit selection is also required. This is distinct
from `Harness unsupported on this OS`, which concerns the adapter itself.
Zero candidates means none were found under the current discovery policy;
it does not establish that a provider is absent or that networking failed.
On Windows, inspect the local command locations without running a provider:

~~~powershell
Get-Command codex,claude,pi,node -ErrorAction SilentlyContinue | Select-Object Name,Source,CommandType
okto-nexus-connector --json discover
~~~

The standalone preview does not load a registered executor's approved roots.
Use `discover --server-id SERVER_ID` for that executor's persisted configuration.
A running desktop application alone does not establish a discoverable CLI.
Core 0.2.54 also observes unapproved PATH installations: they remain `untrusted`
with `selection_required`, rather than being reported as absent. On Windows,
discovery reads known Codex npm native payloads and Pi npm/managed release layouts
without executing their shell or JavaScript launchers. Unrecognized custom
wrappers are not interpreted. Pi identity includes Node and the package dependency
closure, so its first scan can take longer than a simple command lookup.

When the Server advertises `inventory_refresh_supported`, a reconciled daemon
checks for passive inventory refresh requests every five seconds. It claims a
request before discovery and correlates the resulting publication; a periodic
publication alone does not acknowledge a user request. This does not run a
version probe, change local discovery roots or authorize execution. Servers
without this capability keep the existing periodic-publication behavior.

After importing an identity and registering the executor, configure discovery
for that Server. This replaces the previous discovery configuration for that
executor; it does not approve a workspace binding or authorize a runtime.

~~~powershell
okto-nexus-connector executor configure-discovery --server-id SERVER_ID --harness-root "C:/Harnesses"
okto-nexus-connector discover --server-id SERVER_ID
okto-nexus-connector executor show SERVER_ID
~~~

The --harness-root option is repeatable (up to 32 directories). It marks
installations inside those directories as locally selected; it does
not add directories to PATH. Paths must be existing absolute paths. Directory
identity is checked again before discovery and daemon control operations.

For explicit Pi release selection, supply --pi-install-root and --pi-node together.
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
schema 9 state. Final release qualification remains under integration.

### Launch consent and realization

If the selected installation reports `NOT_PROBED`, explicitly observe its version
before creating a realization. The command runs only Core's contained version
probe for the exact installation and inventory revision selected locally:

~~~powershell
okto-nexus-connector discover --server-id SERVER_ID
okto-nexus-connector executor probe --server-id SERVER_ID --harness codex_app_server --candidate-ref CANDIDATE_REF --inventory-revision INVENTORY_REVISION
okto-nexus-connector discover --server-id SERVER_ID
~~~

Use the resulting `executor_revision` after the daemon has published that
inventory. A probe response marked `publication_pending` does not acknowledge
Server publication. The daemon detects the changed local evidence and reconciles
its control connection before publishing. A version observation can still be
`UNQUALIFIED_BUILD`; it never overrides Core's qualified-build or containment
requirements and grants no runtime authority.

Schema 13 stores at most 64 local observations per executor, bound to the exact
passive candidate evidence, Core version and platform. Changed bytes or discovery
scope invalidate reuse. Reconfiguring discovery clears these observations.
Migration from earlier schemas adds no observations; older clients refuse schema
13. Probe before binding: advancing inventory evidence can make an existing
binding stale and require explicit replacement/reapproval.

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
Final provider/platform acceptance remains under integration; these commands
do not imply runtime readiness.

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
older schemas migrate with no implicit proposals or approvals. Canonical runtime CLI admission is described below.


### Canonical runtime intents

An applied R4 alias selects its acknowledged binding and approved launch
configuration. Start the daemon first; the CLI reserves and admits operations
through Nexus. The Server dispatcher and daemon remain the execution owners.

~~~powershell
okto-nexus-connector runtime start assistant --new-session --client-intent-id OPEN_INTENT
okto-nexus-connector runtime operation --alias assistant --client-intent-id OPEN_INTENT
okto-nexus-connector runtime submit SESSION_ID "Your prompt" --alias assistant --client-intent-id TURN_INTENT
okto-nexus-connector runtime stop SESSION_ID --alias assistant --client-intent-id CLOSE_INTENT --reason "Work completed."
~~~

Use a distinct client intent ID for each intended operation. After timeout,
query the original intent or repeat exactly the same command and ID. The
Connector persists the request digest before resolution and the exact
resolution before admission. A changed request cannot overwrite that intent.
A blocked resolution is returned with its blockers and produces no admission;
its stable replay remains blocked. After resolving the reported cause, a new
intent may be created explicitly. Never replace an uncertain admitted intent.

Admission is not completion. The operation response includes the Server's
admission state and receipt revision. Query it to observe progress. The CLI
does not launch Core directly or infer an applied lease.

R4 start automatically reuses one compatible session; --new-session requests a distinct session.
Use `--text` with start for an initial child turn, or `runtime submit` for a
subsequent turn after the session is ready.
The project and harness come from the approved binding. Session controls accept
an explicit --alias; a session previously resolved here can identify that alias
when unambiguous. Steering and interruption accept --expected-turn-id or
--current-run according to the adapter's control contract.

Schema 12 retains runtime intents across restarts. Existing state migrates
without implicit runtime authorization. Retention is currently bounded to
32 intents and 3 MiB of state at resolution; archival/pruning remains pending.
The complete R4 status/logs UI and final provider/platform
acceptance remain part of the delivery plan.

Operation queries use the retained canonical agent and current approved Server
profile. They remain available after local workspace or binding mapping changes,
subject to Server read authorization. Such queries do not reauthorize launch.
Credential changes during a read discard the response; a later explicit query
can use the newly imported current credential.

### Protected OS credential storage

Install the keyring extra to use the operating system credential store:

~~~powershell
python -m pip install "okto-nexus-connector[keyring]"
~~~

The keyring backend must be available and unlocked for the current OS account.
Without it, restricted-file storage still requires explicit local approval.
The real-provider acceptance campaign requires the OS keyring and does not
approve plaintext fallback.

Acknowledging a submitted receipt does not stop observation of the operation.
The daemon reads subsequent durable Core facts and publishes increasing receipt
revisions without re-executing the operation. Restart recovery also checks
acknowledged nonterminal receipts. A lost publication remains durable, and
shutdown retains any receipt publication already in progress.

Running native tool calls revalidate the approved binding, launch configuration,
physical workspace and provider-home identities, executable/entrypoint fingerprint,
and current Core session lease before and after capability metadata retrieval.
Full installation dependency qualification is performed for every new runtime
composition; a running tool call does not authorize another launch.

### Read retained R4 sessions

Use runtime inspect SESSION_ID --alias ALIAS or runtime status --alias ALIAS
to query the Server's durable session observations. Inspect can infer the alias
when retained history identifies it uniquely. These commands do not start a
local daemon or authorize another runtime. Historical reads remain available
after the local binding mapping changes, using the original canonical subject's
current credential. A credential change during the query discards the response.

Status lists sessions referenced by locally retained R4 intents; it is not a
complete inventory of every session on the Server. Process/ownership UNKNOWN
means the Server has no verified observation; it does not mean STOPPED.

### Reuse a canonical session

runtime start ALIAS --client-intent-id ID automatically reuses one compatible
session with the same realization and current applied grant/lease. With no live
claim it reserves a new opening. An unresolved or ambiguous claim is refused;
use --session-id SESSION_ID to select a known compatible session, or explicitly
request a distinct opening with --new-session.

Reuse preserves the original opening operation and its receipt provenance.
The new client intent is confirmed durably without another dispatch or process.
Replaying a confirmed reuse recovers that acknowledgment even after the session
closes. A new unconfirmed reuse still requires current authority.
Use --text with start to admit an initial turn as a separate durable child
operation. The Server releases it only after the opening is ready and rechecks
current authority before dispatch. The opening response lists child IDs in
follow_up_operation_ids. Retrying the same client intent preserves those IDs
and does not send the prompt twice.


### Replace an idle canonical binding

Publish the newly approved realization, then use bind prepare with
--replace-binding-id BINDING_ID and the current local alias. Review the new
diff and obtain its operator proof before bind apply. The target must retain
the same agent, executor, workspace and adapter, with all target sessions
closed or reconciled to CLOSED.

Replacement advances the binding revision and preserves endpoint/profile
identity. It does not open a runtime. The previous application remains in the
local history as SUPERSEDED; bind show and runtime commands select the current
revision. Lost acknowledgments recover the same application rather than
performing a second replacement.

Schema 12 adds optional replacement selection and historical binding results.
Reading schema 11 preserves existing records and grants no replacement
authority. Older versions refuse schema 12 rather than interpreting it as
ordinary creation.

Before publishing another realization, the daemon compares current canonical
identity authority with its administrative ticket. If the ticket is stale, it
obtains a fresh derivative through the same persisted executor registration.
This does not restart a native operation or replace control-channel ownership.
