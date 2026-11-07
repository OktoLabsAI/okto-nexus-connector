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

## Project review with connect

Run `connect` in the project directory. A sole discovered installation is proposed
without a selection question. Several installations require one explicit choice;
`--harness` filters the inventory and `--executable` selects a matching local path.
Pi discovery preserves the Node/script pair, including distinct scripts sharing
one Node binary. Discovery uses this executor's saved roots and observations.
Paths absent from that inventory require reviewing discovery configuration first.

The current folder, a workspace label, discovered provider directory and project
defaults appear in the final review with full paths. Existing Nexus workspaces
still require a deliberate selection; a shared path/name never proves access.
Defaults leave model and native harness settings unset, use per-sender sessions,
request Nexus tool approval and limit execution authorization to 60 minutes/20
actions. Use `connect --customize` to edit optional preferences through the same
wizard, or `configure` for the full sequence below.

Discovery and selection never run the executable. Version/protocol checks occur
after Finish through the existing persisted checkpoint. A changed inventory
requires a fresh review. Discovery does not prove provider login or session
readiness, and accepting the review does not submit project work. Canceling keeps
host registration, probes and daemon startup untouched; an identity explicitly
imported beforehand remains stored. Reconnect retains the original choices and
does not accept `--customize` as an implicit edit of an already reviewed request.

## Remote bootstrap without an agent key

On the Connector host, prepare a reviewed draft with passive installation selection:

```sh
okto-nexus-connector connect --server https://nexus.example --setup-draft host-draft.json --request-id remote-project --project C:\Work\Project
```

The private checkpoint retains the exact local folders, installation evidence,
configuration and optional startup choice. The public draft contains portable
preferences and labels, never local paths or a key. Its canonical proof and
inventory digests remain pending until the existing host pipeline produces them.
Preparing a draft does not probe an executable, start a daemon or submit work.
Without a saved identity, interactive `connect --server ...` also offers this flow.

On the operator's client, use `okto-nexus connect --remote-draft host-draft.json
--operator-profile OP --profile CONNECT_PROFILE --plan`, review the actual agent,
workspace and effects, then continue that same profile. Transfer the non-secret
handoff file separately from the one-time canonical agent key. Never transfer an
operator credential to the Connector.

```sh
okto-nexus-connector connect --setup-receipt handoff.json
```

Enter the canonical agent key through the existing protected input when needed.
Before storing it, the Connector checks the expected Server, agent and credential
epoch with `/me`. It also authenticates the current handoff with the Server;
the file alone grants no authority. Registration, installation checks, realization
and binding preparation use the persisted child request IDs. To finish the
operator side, resume **the same** `okto-nexus connect --profile CONNECT_PROFILE`.
The host prints the setup root for correlation; it does not know that profile's
local name. This bootstrap path uses the original profile review for the same
effects rather than requiring an independent approval review.

### Review changed host choices in the same root

Before Finish commits, explicitly review changes to the folder, provider login
directory, installation, preferences or startup choice:

```sh
okto-nexus-connector connect --setup-draft revised-draft.json --request-id remote-project --review-current --project C:\Work\OtherProject
```

Use `--customize` for portable preferences. Confirm the displayed local changes.
The Server origin, Connector, canonical agent and canonical workspace remain
attached to the original bootstrap. A different folder label does not select a
different workspace. Existing setup and apply receipts remain retained while the
new choices wait in a private pending review. Native configuration changes get
a new durable local consent ID; unchanged realization content keeps its ID.

On the operator client, resume the same profile with
`okto-nexus connect --profile CONNECT_PROFILE --remote-draft revised-draft.json
--review-current`, confirm the affected review, and export its current handoff.
Resume the Connector with that handoff. Only the authenticated current Server
child map can replace affected request IDs; retries never generate their own IDs
or reuse an old intent with a new body. Reexport a pending draft with the same
root and `--review-current`, omitting new installation/customization flags.

If the old apply committed while local revision was being prepared, resume with
the original handoff. Authenticated recovery preserves that Finish and discards
only the unapplied pending selection into history. If the original host files
changed, the committed result is still reported, but restore or explicitly
reconfigure the existing binding before attachment. A confirmed Finish cannot be
revised through this draft command. Credential rotation remains explicit.
Changing the concrete startup task while keeping startup enabled uses the existing
`service` controls. This draft command refuses that change before creating a
pending review; omit startup flags to retain the original startup plan.

The host retains up to 32 setup roots, one pending local review per root and 16
review-history entries. Public documents and each complete private checkpoint
are bounded to 64 KiB; the whole local state is bounded to 4 MiB by this flow.
Capacity exhaustion refuses the change and preserves recovery records. An
uncommitted local revision reserves additional room for a racing Finish receipt.
An uncertain installation probe is recovered from its durable observation or left
unconfirmed; it is not automatically repeated. This flow does not run a model turn.

## Full wizard sequence with configure

1. Select/import an identity, then select a harness.
2. Choose a connection name. Compatible existing bindings offer **Replace existing binding** or **Abort setup** (default). Pending or incompatible bindings are reported without overwriting them.
3. Select the exact installation on this machine.
4. Select an existing Nexus workspace (including one used by another remote harness), or choose **Create new workspace**. Then enter an existing absolute folder on this machine and optional provider login directory. Enter `-` to clear an optional default.
5. Select harness preferences, session policy, Nexus tool access and requested execution limits.
6. Review and Finish. On Windows, optionally include startup at user login. The CLI records the confirmed choices, applies any selected local startup transition, registers the host, checks the installation, starts/reuses the daemon, publishes the workspace and submits the binding proposal.
7. The authenticated operator reviews the printed approval ID in their Nexus terminal. For current requests, one approval covers the displayed configuration and limits. The daemon applies it and attaches in the background; no operator key or second approval is needed on this computer.

Cancel before Finish to avoid submitting the binding. An identity explicitly imported earlier remains stored. Replacement preserves identity, harness and workspace; close active sessions first. The previous binding stays unchanged until replacement succeeds.

New default Connector-hosted requests use the `project-generalist` preset, revision 1:
sessions per sender, native questions/permissions, Nexus tool access set to ask,
and a requested grant of 60 minutes/20 actions. Preferences remain editable in the
review. The operator's proposal also states whether Finish will initialize
Server-wide human review (`feature_hitl`), affecting native decisions and matching
coordination approval rules. Each decision still requires its authorized recipient.
An existing disabled policy is preserved and refuses this preset; it must be
changed explicitly by the operator before a new review. Policy changes between
review, approval and application require a new proposal.

Preset origin and policy activation share the canonical Finish receipt with the
configuration and grant. Repeating the same apply intent recovers the committed
result and current policy without reapplying defaults or restoring revoked grants.
Imported JSON and older saved requests retain their original choices and do not
implicitly request this preset. Preset origin is separate from portable JSON.

The workspace is the shared logical identity in Nexus; its folder belongs to this connection. For example, two harnesses can share one workspace while using `C:\Work\Project` and `/home/agent/project` on their respective computers. Selection does not copy files or reuse another host's path. The review screen shows the workspace name/ID and local folder separately.

Listing existing workspaces requires `workspaces.list` permission (or operator access). When listing is unavailable, the wizard offers an explicit choice to enter a known workspace ID or create a new workspace. `--workspace-id ID` selects it directly. Connection approval is still required; selecting a workspace does not grant execution access. Resuming a saved request preserves its workspace selection.

The terminal can close after submission, but the daemon must remain running. `awaiting_inventory` means submission has not reached approval yet. `saved: true` means configuration was applied, not that a model-response test passed. Verify `status` and send a message in Meta-harness.

## Optional startup at user login

On Windows, `connect` and Connector-hosted `configure` offer this choice in the
initial review. `--start-at-login` preselects it; `--no-start-at-login` keeps the
current service mode. The final confirmation still reviews the user, instance,
state directory, interpreter, working directory and concrete effects. Canceling
before Finish changes no user task. Declining startup leaves on-demand use available
and does not disable an existing task.

These wizards require interactive, human-readable output. `connect` and `configure`
reject `--json` or `--non-interactive` before accessing credentials; use the
explicit automation commands for scripts.

```sh
okto-nexus-connector configure --identity assistant --start-at-login
```

After the local confirmation is saved, the existing installer drains the selected
Connector, registers its user logon task and starts it through Task Scheduler.
An already enabled task is reused; a disabled task requires the displayed enable
effect. Active work can delay the drain; no forced termination is used. This
local host choice applies even while the separate Nexus operator approval is
pending. It does not authorize agent execution or submit a model turn.

The original request retains a separate startup checkpoint. After a timeout or
uncertain reply, resume with `configure --identity assistant --request-id ID`.
It recovers the same installation before onboarding continues, without creating a
second task or replacing a pending binding intent. Unknown native registration
requires inspection through the existing service controls; the CLI does not
adopt an unconfirmed task or fall back to a detached daemon. Conflicting project,
installation or startup choices are refused before this recovery.

After a completed transition, reconnect preserves a task that the user later
disables or removes. Use `service enable`, `service disable` or `service uninstall`
explicitly to change it. Startup status reports the current integration separately
from the completed request. Startup settings remain local and are excluded from
portable connection JSON and from the proposal sent to Nexus.

This uses the current user's Windows logon task, without elevation or an OS
service account. It does not promise availability before login or after logout.
Actual login/logout/crash qualification remains pending. POSIX integration through this wizard
is not qualified. `configure --host server --start-at-login` is refused: review
Server startup on that host with `okto-nexus connect --start-at-login`.

## Resume

Run `okto-nexus-connector connect` again in a previously configured project.
The saved canonical association supplies the Server, identity and original
request ID. If several saved connections match, select the intended one.
`--identity ALIAS` selects an imported identity; `--request-id ID` selects a
retained request explicitly. `--server` remains necessary for a new destination
that has no saved association.

Reconnect reads the existing protected vault entry and authenticates it with the
Server. It does not ask for the key again, run installation probes, recreate the
binding, reapply the preset or renew grants. For a completed request, before
starting/reusing the daemon it checks that the current Server binding is approved and matches this host's
canonical mapping and physical project directory. Current configuration and
authorization revisions are reported separately from the historical setup result.
This is configuration reuse, not a proof that a provider turn can execute.

A revoked credential, unavailable Server, changed installation mapping, disabled
binding or conflicting explicit choices stops the operation. There is no fallback
to another identity, key prompt or fresh setup. Credential replacement remains an
explicit action; credential source flags explicitly request import and validation.
Saved startup choices and later enablement changes retain their existing service
semantics. Pending approval resumes the original request and checkpoints.

### Operator decision from another terminal

The pending response preserves the original `request_id` and `required_approvals`,
and supplies `operator_review` with the Server ID and approval IDs. On the
operator's client, using Nexus `0.3.3` with binding proposal reviews:

```sh
okto-nexus operator login --profile team --server https://nexus.example --username human
okto-nexus operator approval APPROVAL_ID --profile team
okto-nexus operator approval APPROVAL_ID --profile team --decision approve
```

The last command presents the scope and asks for confirmation. Use `--decision
reject` to refuse. For scripts, retrieve `--json` first, then pass its
`review_revision` as `--confirm REVISION`. Keep the operator session on that
client; the Connector continues with its existing participant key. A local
participant confirmation cannot approve the request. The dashboard remains an
optional interface to the same canonical decision.

After a lost reply, inspect the same approval ID. The operator command recovers a
matching committed decision; the background Connector applies the original
binding intent. Neither path creates a replacement proposal to hide uncertainty.
Approval does not submit project work or prove that a provider is ready.

### Resume the Connector request

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
  "authorization": {"minutes": 60, "actions": 20, "no_expiry": false, "unlimited_actions": false}
}
```

This is preferences plus requested limits, not an authorization grant. It excludes agent keys/identity, machine identity, selected installation, destination workspace and login paths. Version 1 input is accepted with destination fields discarded.

```sh
okto-nexus-connector connection-config validate --file connection.json
okto-nexus-connector configure --identity assistant --file connection.json --project /work/project --provider-home /home/user/.claude
```

Paths above are examples on the Connector computer. On Windows use existing absolute paths such as `"C:\Work\My Project"`. Model/account login remains on the execution computer.

Session policy values are `shared`, `per_sender`, `per_sender_session`, or `null` (inherit the server default). Automatic replies are enabled; imported `automatic_reply` is normalized to true. A remote runtime binding requires runtime access enabled. Configure MCP-only access in Nexus rather than submitting a disabled remote runtime binding. In `authorization`, `0` means unlimited and is sent with `no_expiry: true` (duration) or `unlimited_actions: true` (actions); the wizard accepts `0` for either. Older files with `null` limits are read as `0` plus the flag. Remote runtime authorization requires finite duration and action limits.

## Server-hosted setup and automation

```sh
okto-nexus-connector configure --host server --identity operator --agent assistant
okto-nexus-connector --non-interactive connection-config apply --file connection.json --identity operator --agent assistant --request-id setup-assistant-1 --executor-id EXE --candidate-ref CANDIDATE --inventory-revision REVISION --project /server/work/project --workspace-label Project
```

Here all paths and installation identifiers refer to the Nexus server. Obtain EXE, CANDIDATE and REVISION from its inventory. The apply command uses the server test-and-finish workflow. Reuse the exact request ID/content after an uncertain response. A changed configuration needs a new ID.

Server-hosted resume first reads the durable Finish receipt and returns the
original result if it matches. No new binding, grant or test is created during
that recovery. If no Finish receipt exists, a previously submitted test is only
observed. Tests run a model turn; a missing or expired result, including after a
Server restart, is `SETUP_TEST_UNCONFIRMED` and is never automatically repeated.
Inspect the prior attempt before explicitly reviewing another test with a new
request ID. Servers without receipt recovery support leave the saved attempt
unconfirmed. `saved: true` means configuration committed; it does not mean a live
session or completed task. These checkpoints contain configuration and secret
references, never the imported operator key.

For the Server-hosted wizard, resume with
`configure --host server --identity operator --request-id ORIGINAL_ID`.
It restores the reviewed agent, installation, folders and preferences before
asking setup questions or checking an installation again. Explicit arguments
that conflict with the saved choices are rejected.

For Connector-hosted automation use the explicit executor/realization/binding flow in the [CLI guide](cli.md#advanced-r4-automation). `connection-config apply` does not discover and provision the CLI computer unattended.
