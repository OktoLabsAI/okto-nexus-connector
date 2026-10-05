# Harness parameters and JSON files

The Core describes the supported parameters for Codex, Claude Code and Pi.
Native discovery additionally supplies models and version/account constraints.
An imported settings file contains values, not native discovery or execution authority.

```json
{
  "format": "nexus-harness-config",
  "version": 1,
  "adapter_id": "codex_app_server",
  "settings": {
    "model": "gpt-6.1-sol",
    "effort": "low",
    "approval_policy": "on-request",
    "sandbox": "workspace-write",
    "user_input": "enabled"
  }
}
```

Use `claude_stream` with `model`, `effort`, `permission_mode`, or `pi_rpc`
with `model`, `provider`, `effort`. Omitted fields retain the harness defaults.
Model availability must be checked against the selected installation/account.
Files are UTF-8 JSON, at most 64 KiB. Credentials and login directories are not
part of this portable settings format.

```sh
okto-nexus-connector --json harness-config describe --harness codex_app_server
okto-nexus-connector --json harness-config validate --file codex.harness.json
okto-nexus-connector --json harness-config show --identity operator --endpoint-id ENDPOINT
okto-nexus-connector --json harness-config apply --identity operator --endpoint-id ENDPOINT --expected-revision 4 --file codex.harness.json
```

`show` and `apply` use an existing imported identity with operator permission on
the Server. The reviewed revision is mandatory; changes made by another client
cause a conflict. Close existing sessions first. Saving invalidates previous
execution grants, so authorize the new configuration before starting a session.
The Connector forwards the canonical model and harness settings in R4 launch
intents; receipt hashes bind those values to the execution.

For full connection import/export in Nexus, use Connections, Host & harness.
That document uses the okto-nexus-connection format and includes harness_settings.
It is different from the settings-only nexus-harness-config document above.
Use configure --file for a full connection document, and harness-config validate/apply
for settings-only documents. See [connection JSON](connection-configuration.md).

Native approval policy, Nexus tool approval and execution grants are separate.
The Core catalog defines accepted parameters; use harness-config describe for the
installed version instead of assuming every harness supports the same model or effort.
