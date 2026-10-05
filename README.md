# Okto Nexus Connector

Run Codex, Claude Code or Pi on the Connector computer while Nexus coordinates the agent. The CLI configures the connection; the daemon maintains it and runs the harness through Core.

## Install

Python 3.11 or newer is required. This release is `0.0.1` and requires `okto-nexus-connector-core==0.0.1`.

From a checkout, using the included Core wheel:

```sh
python -m pip install --find-links vendor/wheels ".[keyring]"
okto-nexus-connector --version
okto-nexus-connector doctor
```

For supplied release wheels, install the exact Core wheel and Connector wheel with pip. Do not assume an unpublished artifact is available on PyPI. After an upgrade, stop and start the daemon so it loads the new code.

Windows, Linux and macOS have platform-specific execution backends. Native execution still requires a successful containment preflight and a qualified harness installation. Discovery alone does not prove runtime readiness.

## First connection

1. In Nexus, create/select the agent, obtain its canonical agent key and select **Remote** execution.
2. On the computer that will run the harness, install the harness and complete its provider login.
3. Check the server address, then run the wizard:

```sh
okto-nexus-connector reach --server http://192.168.0.146:8202
okto-nexus-connector connect --server http://192.168.0.146:8202 --agent claude-coder
```

Replace the address and agent name with your values. `--agent` is a name/hint, not a credential. The CLI requests the canonical agent key through hidden input. On another computer, `127.0.0.1` means that other computer, not the Nexus server.

4. Select the installation, workspace, login directory and preferences; confirm submission.
5. Approve the request in Nexus. The CLI can exit while the daemon completes setup asynchronously.
6. Check the connection, then send a message through Nexus Meta-harness:

```sh
okto-nexus-connector status
okto-nexus-connector logs --errors
```

`CONNECTED` confirms the connection. A running daemon, an approved request, or a successful `reach` alone does not confirm that the harness can execute a turn.

## Documentation

- [CLI usage guide](docs/cli.md): identities, daily operations, automation, proxy and cleanup.
- [Complete command reference](docs/cli-reference.md): every command, argument and required flag.
- [Connection wizard and JSON](docs/connection-configuration.md): execution location, imports and resuming setup.
- [Harness parameters](docs/harness-configuration.md): model, reasoning and native settings.
- [Troubleshooting runbook](docs/runbook.md): status meanings, failures and recovery.
- [Architecture](docs/architecture.md) and [threat model](docs/threat-model.md): internal design.

User-facing guides describe the current source tree. Documents under `plans/`, `FIX_UPDATE_PLAN/` and the original implementation plan are historical design/evidence, not current CLI instructions. Features in a checkout may require installing that checkout before use.

## Development

```sh
python -m pip install --find-links vendor/wheels ".[test]"
python -m pytest -q
python tools/generate_cli_reference.py
python -m build
```

## License

Elastic License 2.0 with the Okto Labs SaaS/Branding Addendum. [LICENSE](LICENSE) is authoritative and matches the Nexus license. See [CONTRIBUTING.md](CONTRIBUTING.md) for branch and review rules.
