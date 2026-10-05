# Connector troubleshooting

## Start with evidence

Run on the computer hosting the Connector, under the same OS user and state directory as its daemon:

```sh
okto-nexus-connector status
okto-nexus-connector --json status
okto-nexus-connector logs --errors --tail 100
okto-nexus-connector doctor --probe
```

`status` queries current server and daemon facts. `daemon status` only describes the local process/IPC. `reach` checks the public server endpoint, not credentials or a harness. `doctor` checks individual environment layers; a mostly green report does not prove an approved, connected R4 binding. Its legacy binding count can be zero while an R4 setup is pending; use `status` for connection progress.

## Interpret progress

| Status | Meaning | Next action |
|---|---|---|
| `DAEMON_STOPPED` | No active local owner | `daemon start` |
| `AWAITING_APPROVAL` | Request submitted | Review it in Nexus |
| `COMPLETING_SETUP` | Approved but not applied/attached yet | Inspect logs; verify daemon is running |
| `CONNECTED` | Current control/binding connection is live | Send a test message; inspect execution logs |
| `ATTACHING` / `RECONNECTING` | Connection not yet ready | Check ticket, inventory and binding errors |
| `OFFLINE` / `SERVER_UNREACHABLE` | Server or executor not reachable | Check address, network, proxy and daemon |
| `AUTHORIZATION_REQUIRED` | Execution grant unavailable/exhausted | Review authorization in Nexus |
| `EXPIRED` | Proposal or scoped authority expired | Identify the expired item; request renewed approval |
| `REVOKED` / `REJECTED` | Access removed or request denied | Review in Nexus; do not repeat automatically |
| `CLEANUP_PENDING` | Resource/socket cleanup could not be confirmed | Inspect logs; update Connector and restart after resolving the cause |
| `RECOVERY_ATTENTION_REQUIRED` | Recovery attempts exhausted | Resolve the reported condition, then restart |

Exact text differs between CLI and dashboard. A process can be running while its connection supervisor is stopped. `EXPIRED` on a proposal does not necessarily mean the canonical agent key expired.

## Approval accepted but setup never completes

Check whether the request was actually submitted, then whether the daemon attempted binding application. A server-side 500 can prevent application after a valid approval. Check Nexus Execution log/server logs as well as Connector logs. After fixing the cause, resume the original request with `configure --identity ALIAS --request-id ORIGINAL_ID`. Do not reuse an expired approval or start many parallel wizards.

## Connection or inventory errors

Use the Nexus LAN hostname/IP from another computer; loopback addresses point to that computer. HTTP/WS are allowed when the Nexus transport policy permits them; HTTPS-only mode requires HTTPS/WSS. Check `reach`, proxy settings and server listening address. `PERMISSION_DENIED` can concern a scoped execution ticket even when `/me` accepts the agent key.

Inventory is published by the daemon. `awaiting_inventory` or `inventory_not_fresh` requires checking its control connection and selected installation. Automatic revalidation can recover unchanged installations. A changed executable, workspace or contract can require review.

## Existing binding

The wizard reports local alias conflicts early. Compatible applied bindings offer replacement or abort. Replacement preserves identity, harness and workspace and requires Nexus approval. Close/reconcile active sessions first. For a pending request, resume its printed ID. For another identity or scope, choose another connection name. `bind remove` is legacy local management, not R4 server revocation.

## Restart and update

```sh
okto-nexus-connector daemon stop
okto-nexus-connector daemon start
okto-nexus-connector status
```

Stop the daemon before replacing installed packages; start it after installation. A CLI package upgrade does not update an already running process. Automatic reconnect requires the daemon to remain alive. Use `service install` for user autostart or `daemon run` under a persistent supervisor. Windows supervisors can prohibit independent Job Object breakaway; use an appropriate terminal/supervisor rather than weakening containment.

## Provider, platform and credential errors

- `PROVIDER_AUTH_REQUIRED`: complete the harness login on the execution host and select the correct provider home.
- Keyring locked: unlock it for the daemon's OS user. A file fallback must be explicitly approved; it is permission-restricted plaintext.
- `AGENT_ID_MISMATCH`: the supplied key belongs to another agent. Names cannot replace authentication.
- `SERVER_ID_CHANGED`: verify the server installation before reimporting credentials.
- `PROCESS_CONTAINMENT_UNAVAILABLE` or `UNSUPPORTED_PLATFORM`: inspect `doctor` preflight and use a supported host/build. Windows, Linux and macOS have distinct backends; discovery success is not containment success.
- `PROFILE_DRIFT`: review the exact executable, workspace and login configuration. Do not bypass the check.

## Uncertain operations and journal capacity

`OUTCOME_UNKNOWN` means the server may have accepted work without an acknowledgment. Query the original operation:

```sh
okto-nexus-connector runtime operation --alias CONNECTION --client-intent-id ORIGINAL_ID
```

Do not resubmit the work with a new ID. There is no standalone `reconcile` command. The daemon reconciles retained facts; native pipes do not magically survive a restart. For `JOURNAL_FULL`, stop/drain and preserve evidence before reviewing retention. `clean` is a deliberate reset, not safe replay or journal compaction.

## Reset abandoned local configuration

```sh
okto-nexus-connector clean
```

Read the displayed directory and confirm (default No). This disables user autostart, drains/stops the daemon, removes Connector-owned local settings, identities/keys, pending requests, bindings, journals, runtime files and logs. Projects, harness installations and provider logins remain. Unknown files remain; an inert lock file may remain. Shutdown or credential-removal failure stops cleanup; a failure can leave partial progress, so inspect the report before retrying.

No Nexus records or authorizations are deleted/revoked. Reconfiguring creates a new machine identity subject to Nexus replacement policy. User autostart is account-wide; keyring credentials can be shared with another local state directory. Do not run configuration commands concurrently with cleanup. For explicit automation use `--non-interactive clean --yes`.

## State locations

| Platform | Default |
|---|---|
| Windows | `%LOCALAPPDATA%/okto-nexus-connector` |
| Linux/macOS | `$XDG_STATE_HOME/okto-nexus-connector`, otherwise `~/.local/state/okto-nexus-connector` |

`--state-dir PATH` overrides `OKTO_NEXUS_CONNECTOR_STATE` and the platform default. Use one consistent directory for CLI, daemon and service. Logs are `logs/daemon.log`, rotated at 2 MiB with three backups. `logs --errors` includes warnings. Review host paths/identifiers before sharing diagnostics.

## Reconnect all connections

Run okto-nexus-connector reconnect to gracefully stop and restart the daemon for the selected state directory. Configurations and credentials are preserved. Active harness executions may be interrupted during shutdown. The command returns when the daemon is ready; server connections complete asynchronously. Check okto-nexus-connector status and okto-nexus-connector logs --errors for progress. If shutdown cannot finish within 30 seconds, no second daemon is started.

Automatic connection retries use delays of 2, 4, 8, 16 and 30 seconds, then continue every 30 seconds until the daemon is stopped, including reconciliation failures. Pending socket cleanup is retried before opening another connection. Retrying does not replay submitted work or bypass Nexus authorization.
