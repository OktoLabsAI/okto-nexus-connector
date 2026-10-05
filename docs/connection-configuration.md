# Connection wizard and portable JSON

## Select the execution computer

| Flow | Where the harness runs | Credential |
|---|---|---|
| `connect` or `configure` (default) | Connector computer | Target agent key |
| `configure --host server` | Nexus server computer | Imported operator identity |
| `connection-config apply` (local destination) | Nexus server computer | Imported operator identity |

Select **Remote** for the agent in Nexus when using a Connector. A remote binding stays remote even if both processes run on one computer.

```sh
okto-nexus-connector configure
okto-nexus-connector configure --identity assistant
okto-nexus-connector configure --identity assistant --file connection.json
```

`assistant` is the local identity alias. The wizard reuses its stored key. Adding a new identity asks for a server URL and hidden agent key; identity import stores the key before the rest of setup.

## Wizard sequence

1. Select/import an identity, then select a harness.
2. Choose a connection name. Compatible existing bindings offer **Replace existing binding** or **Abort setup** (default). Pending or incompatible bindings are reported without overwriting them.
3. Select the exact installation on this machine.
4. Enter an existing absolute workspace path and optional provider login directory. Enter `-` to clear an optional default.
5. Select harness preferences, session policy, Nexus tool access and requested execution limits.
6. Review and Finish. The CLI registers the host, checks the installation, starts the daemon, publishes the workspace and submits the binding proposal.
7. Approve in Nexus. For current requests, one approval covers the displayed configuration and limits. The daemon applies it and attaches in the background; no operator key or second approval is needed on this computer.

Cancel before Finish to avoid submitting the binding. An identity explicitly imported earlier remains stored. Replacement preserves identity, harness and workspace; close active sessions first. The previous binding stays unchanged until replacement succeeds.

The terminal can close after submission, but the daemon must remain running. `awaiting_inventory` means submission has not reached approval yet. `saved: true` means configuration was applied, not that a model-response test passed. Verify `status` and send a message in Meta-harness.

## Resume

Keep the printed request ID. After resolving an error, resume that request:

```sh
okto-nexus-connector configure --identity assistant --request-id configure_ID
```

Replace `configure_ID` with the original value. Do not create multiple new requests to work around a connectivity failure. Expired, rejected or changed proposals need a new reviewed request. Older saved requests can retain a separate `--operator-identity` stage; this is compatibility behavior, not the current flow.

## Full connection JSON

Export from Nexus Connections, **Host & harness**. Example:

```json
{
  "format": "okto-nexus-connection",
  "version": 2,
  "adapter_id": "claude_stream",
  "alias": "claude",
  "runtime_enabled": true,
  "session_policy": "per_sender_session",
  "harness_settings": {},
  "automatic_reply": true,
  "tool_access": "ask",
  "authorization": {"minutes": 60, "actions": 20}
}
```

This is preferences plus requested limits, not an authorization grant. It excludes agent keys/identity, machine identity, selected installation, destination workspace and login paths. Version 1 input is accepted with destination fields discarded.

```sh
okto-nexus-connector connection-config validate --file connection.json
okto-nexus-connector configure --identity assistant --file connection.json --project /work/project --provider-home /home/user/.claude
```

Paths above are examples on the Connector computer. On Windows use existing absolute paths such as `"C:\Work\My Project"`. Model/account login remains on the execution computer.

Session policy values are `shared`, `per_sender`, `per_sender_session`, or `null` (inherit the server default). Automatic replies are enabled; imported `automatic_reply` is normalized to true. A remote runtime binding requires runtime access enabled. Configure MCP-only access in Nexus rather than submitting a disabled remote runtime binding. Remote runtime authorization requires finite duration and action limits.

## Server-hosted setup and automation

```sh
okto-nexus-connector configure --host server --identity operator --agent assistant
okto-nexus-connector --non-interactive connection-config apply --file connection.json --identity operator --agent assistant --request-id setup-assistant-1 --executor-id EXE --candidate-ref CANDIDATE --inventory-revision REVISION --project /server/work/project --workspace-label Project
```

Here all paths and installation identifiers refer to the Nexus server. Obtain EXE, CANDIDATE and REVISION from its inventory. The apply command uses the server test-and-finish workflow. Reuse the exact request ID/content after an uncertain response. A changed configuration needs a new ID.

For Connector-hosted automation use the explicit executor/realization/binding flow in the [CLI guide](cli.md#advanced-r4-automation). `connection-config apply` does not discover and provision the CLI computer unattended.
