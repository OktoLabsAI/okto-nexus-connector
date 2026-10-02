# Runbook — okto-nexus-connector

Operational procedures required by plan section 10.1. Recovery guidance
never recommends deleting the journal and re-running an uncertain task,
and never recommends disabling TLS or the sandbox.

## Install (normal and without a service manager)

```bash
python -m pip install "CONNECTOR_WHEEL[keyring]" CORE_WHEEL
okto-nexus-connector doctor
```

Without a service manager, run `daemon run` in the foreground (container
path). No admin privilege is required for common use.

Replace the wheel placeholders with the approved artifact paths and verify their
hashes. Use the pinned Core dependency. Complete identity import, registration and
discovery configuration below before expecting an executor to publish inventory.
Start the daemon with `daemon start`; its IPC readiness does not prove a qualified
provider, an approved binding or remote execution authority.

On Windows, a terminal, CI runner or service supervisor may prohibit Job Object
breakaway. If independent process creation is denied, `daemon start` reports
`DAEMON_UNAVAILABLE` with a corrective action. It does not retry inside the
restrictive job and claim independence: that daemon could die when the CLI exits.
Run `daemon run` under a persistent supervisor, then connect from another client
using the same state directory, or use a terminal that permits independent
daemon creation. Foreground mode shares its supervisor's lifetime; closing that
supervisor can terminate the daemon. No supervisor policy is modified.
See [Windows nested-job rules](https://learn.microsoft.com/en-us/windows/win32/procthread/nested-jobs).

## Unsupported executor platform

Managed execution currently requires the Core Windows or Linux containment
backend and successful preflight. macOS has no qualified backend, so doctor
reports `UNSUPPORTED_PLATFORM` and launches retain
`PROCESS_CONTAINMENT_UNAVAILABLE`. Installing a keyring or starting the daemon
does not remove that restriction. Use a Windows/Linux executor, or a Linux VM
with providers and workspace installed inside it; validate containment with
`doctor` there. A container is usable only if its kernel and supervisor allow
all required checks. Neither a VM nor a container controls native Mac processes.

Discovery and containment are separate. A running provider process is not
proof of an installed, approved executable candidate. Passive PATH discovery
lists candidates under the current trust policy and does not execute binaries.
For R4, configure the executor's approved harness roots; these restrict PATH
discovery and do not add directories to PATH. An explicit `--executable` selects
a file for the applicable CLI flow but does not bypass containment, build
qualification or approval. `NOT_INSTALLED` / `no_installed_candidate` means the
inventory found no candidate; it is not by itself evidence that containment
filtered one out. See [discovery configuration](../README.md#persisted-r4-executor-discovery).

## First use: import an existing canonical key

Import the existing agent key through the masked prompt or an explicitly selected
stdin/environment source. An agent ID hint is checked against the authenticated
identity; it is not a substitute for the key.

```bash
okto-nexus-connector identity add --server https://nexus.example --agent AGENT_ID --alias SUBJECT
okto-nexus-connector executor register --identity SUBJECT --label "Execution host"
okto-nexus-connector executor list
```

If no OS keyring is available, the restricted-file fallback is offered
once with an explicit warning; approve it or install a keyring backend.

## Bind and start a runtime

Follow [discovery, explicit observation and launch consent](../README.md#persisted-r4-executor-discovery)
using the returned Server ID. Publish the workspace realization through the
daemon, then prepare/apply the reviewed binding with any required operator proof.
Use its acknowledged alias; a provider name or current directory does not select
an approved R4 workspace. Execution authorization remains separate from binding.

```bash
okto-nexus-connector runtime start ALIAS --client-intent-id OPEN_INTENT
okto-nexus-connector runtime operation --alias ALIAS --client-intent-id OPEN_INTENT
okto-nexus-connector runtime status --alias ALIAS
okto-nexus-connector runtime inspect SESSION_ID --alias ALIAS
okto-nexus-connector runtime stop SESSION_ID --alias ALIAS --client-intent-id CLOSE_INTENT --reason "Work completed."
```

Admission is not completion. Query the retained operation until its outcome is
known. `runtime interrupt` uses the adapter's explicit turn/current-run target;
`stop` requests closure of owned resources. Status lists locally retained sessions,
not every Server session. The legacy `runtime logs` command is not a documented
complete R4 event viewer; use the Server's authorized operation/session views.

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
explicitly through `executor configure-launch --provider-home`. The error is `PROVIDER_AUTH_REQUIRED` with a
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

New admissions fail closed with `JOURNAL_FULL` when durable capacity is exhausted.
Preserve the journal and uncertain operations. Stop/drain through the daemon's
normal lifecycle and inspect its report before backing up state. No general
Connector CLI compaction command is provided: do not delete journal rows or move
live state to make space. Use a supported, reviewed retention/recovery procedure
for the exact artifact and keep execution disabled if its safety is unproved.

## Uncertain outcome (unknown result)

`OUTCOME_UNKNOWN` means an effect may have happened without proof. Do
not re-run the same work under a new operation ID. Query `runtime operation`
with the original alias/client intent, and inspect the related session. The daemon
owns reconciliation on the authenticated control channel; there is no standalone
`reconcile` CLI command. Repeat an admission request only with the original exact
intent. A different ID creates new work and can duplicate an uncertain effect.

## Update / rollback

Stop the daemon (drain is bounded and reported per session), install the
pinned wheels, and start again. Active native pipes do not survive the
swap; the daemon reconciles durable facts instead of pretending the old
conversation continued. State migrations have explicit versions: an older binary
refuses newer state. Installing previous wheels alone is not a rollback procedure.
Retain a consistent pre-upgrade backup and preserve later journals/uncertain effects
for reviewed recovery. Never restore old state while a newer owner can still act.

## Remove connector-owned configuration

Use `mcp-config remove --help` to select the exact harness, file and owned entry.
Owned-entry removal preserves unrelated settings and requires the stored
predecessor to match. `bind remove` belongs to legacy local configuration
management and is not canonical R4 binding revocation; `--keep-config` is a
boolean flag and does not accept `=false`.
Do not infer remote revocation from deleting a local alias or identity. Preserve
third-party entries and recovery evidence, and use the Server's authenticated
canonical policy surface for execution revocation.

## Diagnostic export (read-only)

```bash
okto-nexus-connector --json doctor --probe
```

`--probe` contacts configured Servers using the selected protected identities.
Diagnostics redact credential material; review the output before sharing host
paths or identifiers. Global options such as `--json` precede the subcommand.
