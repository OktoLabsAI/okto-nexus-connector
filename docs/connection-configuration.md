# Guided configuration and portable JSON

Run `okto-nexus-connector configure` for ordered terminal questions; JSON is optional.
The default execution host is **the machine running the Connector**.

Active runtimes automatically receive eligible Nexus messages. There is no reply
toggle in the wizard. Legacy `automatic_reply` JSON fields are normalized to true;
use the runtime-enabled/MCP-only setting to stop runtime delivery. Execution and
tool approvals still apply.

Conversation context offers **Shared**, **One session per sender**, and
**One session per sender + source session** (`per_sender_session`), or global
inheritance. The third mode separates simultaneous conversations from the same
agent. Managed tools identify the source session automatically through their
authenticated runtime capability. Traditional MCP sends need a verified session
ID and secret. Sessionless messages share a separate conversation per sender.
Portable JSON preserves this choice; it requires a Nexus Server supporting the
new policy and Core 0.2.63.dev0 or later.

1. Select an existing identity or add one using a Server URL and a hidden canonical
   agent-key prompt. Existing identities reuse the vault.
2. Select the harness from the Core catalog.
3. Select an exact installation discovered on this machine.
4. Enter workspace and login directories. Core suggests an existing local login.
   Enter `-` to clear an optional default.
5. Name the connection.
6. Select model, native preferences, conversation/tool policies and requested limits.
7. Review and Finish to register the executor, check its installation, start the
   daemon, publish the workspace and prepare/apply the R4 binding.

Canceling before Finish does not create a binding. Adding an identity explicitly
stores its key in the vault before the runtime configuration steps.

If Server approval is required, the wizard reports pending references and a request
ID. Approve the binding in Nexus, then resume with the same identity/request ID.
Completed stages are not repeated. Applying Nexus policies and execution grants
requires an imported operator identity on the same Server. Without one, the wizard
reports local setup complete and operator configuration pending. It never bypasses
Server authorization or claims a pending connection is ready. No runtime is started;
successful configuration is not a model-response test.

```sh
okto-nexus-connector configure
okto-nexus-connector configure --identity assistant --file connection.json \
  --project /work/project --provider-home /home/user/.claude
okto-nexus-connector configure --identity assistant --request-id configure_ID \
  --operator-identity operator
```

Use `--candidate-ref`, `--workspace-label`, `--workspace-id` and `--binding-id`
to preselect destination choices. `configure --help` lists all flags.
`--host server` optionally configures a Nexus-hosted runtime with an operator
identity; its paths and discovery refer to the Server machine.

Export a version 2 `okto-nexus-connection` document from Nexus Connections,
Host & harness. It contains native settings, connection name, message/tool policies,
runtime/session policy and requested limits. It excludes identity, credentials,
active grants, installation IDs, execution-host selection, workspace paths/names,
login directories and local secret references. Legacy version 1 files are accepted
but all destination fields are discarded.
Null runtime and session policies inherit the destination Server's global defaults.

Validate without contacting Nexus:

```sh
okto-nexus-connector connection-config validate --file agent.connection.json
```

Apply to a server-local harness using an imported **operator** identity:

```sh
okto-nexus-connector connection-config apply --file agent.connection.json \
  --identity operator --agent assistant --request-id setup-assistant-1 \
  --executor-id EXE --candidate-ref CANDIDATE --inventory-revision REVISION \
  --project /server/work/project --workspace-label Project \
  --provider-home /server/home/user/.claude
```

Use the installation identifiers from the Server inventory, and optionally
`--workspace-id` or `--binding-id` to update an existing mapping/connection.
Paths refer to the execution host. This invokes the same temporary test and
atomic Finish as the dashboard. Reuse the same request ID after an uncertain
reply; the CLI retains the exact request and progress for safe recovery.
After a failed test, review the error and use a new request ID for a new test.

This automation command configures the Nexus Server's local runtime integration.
Use `configure --file` for guided provisioning on the Connector machine.
