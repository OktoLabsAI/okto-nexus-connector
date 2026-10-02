# Runbook — okto-nexus-connector

Operational procedures required by plan section 10.1. Recovery guidance
never recommends deleting the journal and re-running an uncertain task,
and never recommends disabling TLS or the sandbox.

## Install (normal and without a service manager)

```bash
pip install okto-nexus-connector-<version>.whl nexus-connector-core-<version>.whl
okto-nexus-connector doctor            # layered diagnostics, no side effects
okto-nexus-connector daemon start      # background; returns after IPC readiness
```

Without a service manager, run `daemon run` in the foreground (container
path). No admin privilege is required for common use.

On Windows, a terminal, CI runner or service supervisor may prohibit Job Object
breakaway. If independent process creation is denied, `daemon start` reports
`DAEMON_UNAVAILABLE` with a corrective action. It does not retry inside the
restrictive job and claim independence: that daemon could die when the CLI exits.
Run `daemon run` under a persistent supervisor, then connect from another client
using the same state directory, or use a terminal that permits independent
daemon creation. Foreground mode shares its supervisor's lifetime; closing that
supervisor can terminate the daemon. No supervisor policy is modified.
See [Windows nested-job rules](https://learn.microsoft.com/en-us/windows/win32/procthread/nested-jobs).

## First use: import an existing canonical key

The Server screen proposes a command; the key enters through a protected
entry (masked input, `--credential-stdin`, or an MCP entry you select
explicitly). Nothing is scanned automatically.

```bash
okto-nexus-connector connect --server https://nexus.example --agent ag_123
# paste the canonical key when prompted; approve the aggregated binding
```

If no OS keyring is available, the restricted-file fallback is offered
once with an explicit warning; approve it or install a keyring backend.

## Bind and start a runtime

```bash
okto-nexus-connector runtime start codex      # from the project directory
okto-nexus-connector runtime status           # daemon/transport/runtime layers
okto-nexus-connector runtime logs <session> --follow
```

`interrupt` cancels the active turn and keeps the runtime; `stop` closes
resources owned by this daemon; attach targets are only detached.

## Vault locked or unavailable

`doctor` reports the vault layer as `warn`. In service mode, unlock the
OS keyring for the user session or set
`OKTO_NEXUS_CONNECTOR_VAULT=file` with the restricted-file fallback
approved interactively first. Never copy the key to a world-readable
location.

## Provider login missing

The launch environment passes only whitelisted essentials; provider
authentication happens through the provider's own local login. Run the
harness's login flow on this host, or approve the provider-home overlay
explicitly at connect time. The error is `PROVIDER_AUTH_REQUIRED` with a
local action; no secret is sent to the Server.

## Canonical key rotated or revoked

```bash
okto-nexus-connector identity replace-credential work --credential-stdin
```

The new key must authenticate as the **same** agent; otherwise the
operation aborts (`AGENT_ID_MISMATCH`). Local removal
(`identity remove`) never revokes the canonical agent centrally.

## Server changed its identity

`doctor --probe` reports `SERVER_ID_CHANGED` with the observed identity.
Confirm the new installation, then remove and re-import the identity for
that profile. Sessions under the old identity are not reused.

## Daemon/IPC stale

`daemon status` verifies readiness, the process birth token and a live
IPC ping. A stale PID file alone is never authority: `daemon start`
replaces it safely under the OS lock. If the daemon is wedged, use
`daemon stop` (bounded drain) and inspect the state directory log.

## Journal full

`doctor` warns as the technical journal approaches its ceiling; new
admissions fail closed with `JOURNAL_FULL` (no phantom facts). Stop the
daemon, archive or compact the state directory per your retention
policy, and restart; the daemon reconciles before new effects.

## Uncertain outcome (unknown result)

`OUTCOME_UNKNOWN` means an effect may have happened without proof. Do
not re-run the same work under a new operation ID: query
`runtime inspect`, `reconcile`, and the Server's operation by its ID.
Re-submitting is a **new** explicit intent.

## Update / rollback

Stop the daemon (drain is bounded and reported per session), install the
pinned wheels, and start again. Active native pipes do not survive the
swap; the daemon reconciles durable facts instead of pretending the old
conversation continued. Rollback = install the previous wheels; state
schema migrations are forward-only with explicit versions.

## Remove connector-owned configuration

`mcp-config remove` (and `bind remove --keep-config=false`) delete only
entries recorded as owned; third-party MCP entries and other settings
survive, with a backup written before any change. Full uninstall
preserves evidence by default; `identity remove` is a separate explicit
step from any central revocation.

## Diagnostic export (read-only)

```bash
okto-nexus-connector doctor --probe --json
```

Inspection never mutates state; exports are redacted (keys, tickets and
capabilities are replaced with `[redacted]`).
