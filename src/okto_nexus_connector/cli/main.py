"""``okto-nexus-connector`` CLI entry point (plan section 6).

Command surface: connect, identity (add/list/show/remove/replace-credential),
discover, bind (create/list/show/remove), runtime
(start/status/inspect/logs/interrupt/stop/submit), daemon
(start/run/status/stop), service (install/uninstall/status), doctor.
Every automation surface honors ``--json`` and ``--non-interactive`` with
stable exit codes; secrets only ever enter through stdin, masked input or
the vault.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from .. import __version__
from ..errors import ConnectorError
from .output import EXIT_OK, EXIT_USAGE, Output
from .presentation import styled


class ConnectorParser(argparse.ArgumentParser):
    def print_help(self, file=None):
        stream = file or sys.stdout
        text = self.format_help()
        for title in ('usage:', 'positional arguments:', 'options:'):
            text = text.replace(title, styled(title, 'heading', stream))
        self._print_message(text, stream)

    def parse_args(self, args=None, namespace=None):
        tokens = list(sys.argv[1:] if args is None else args)
        self._output_context['json'] = '--json' in tokens[:tokens.index('--') if '--' in tokens else len(tokens)]
        return super().parse_args(tokens, namespace)

    def error(self, message):
        if getattr(self, '_output_context', {}).get('json'):
            Output(json_mode=True).error(ConnectorError('VALIDATION_ERROR', 'cli', message))
            self.exit(EXIT_USAGE)
        super().error(message)


def build_parser() -> argparse.ArgumentParser:
    parser = ConnectorParser(
        prog="okto-nexus-connector",
        description="Remote harness connector for Okto Nexus")
    parser.add_argument("--version", action="version",
                        version=f"okto-nexus-connector {__version__}")
    parser.add_argument("--json", action="store_true",
                        help="machine-readable JSON output")
    parser.add_argument('--verbose', action='store_true', help='include technical details')
    parser.add_argument("--non-interactive", action="store_true",
                        help="never prompt; ambiguity fails with guidance")
    parser.add_argument("--state-dir", type=Path, default=None,
                        help="override the per-user state directory")
    sub = parser.add_subparsers(dest="command", required=True)
    clean = sub.add_parser('clean', help='reset local Connector state, credentials and pending configurations')
    clean.add_argument('--yes', action='store_true', help='explicitly confirm cleanup without prompting')

    proxy = sub.add_parser('proxy', help='configure the host-local HTTP and WebSocket proxy')
    proxy_sub = proxy.add_subparsers(dest='subcommand', required=True)
    proxy_set = proxy_sub.add_parser('set', help='save outbound proxy settings')
    proxy_mode = proxy_set.add_mutually_exclusive_group(required=True)
    proxy_mode.add_argument('--url', help='HTTP(S) proxy URL without credentials')
    proxy_mode.add_argument('--url-env', help='environment variable containing the proxy URL (supports credentials)')
    proxy_mode.add_argument('--direct', action='store_true', help='disable proxy use, including environment proxies')
    proxy_set.add_argument('--no-proxy', help='comma-separated bypass hosts, domains, or host:port entries; * bypasses all')
    proxy_sub.add_parser('show', help='show settings without resolving credentials')
    proxy_sub.add_parser('clear', help='restore environment/system proxy discovery')

    reach = sub.add_parser('reach', help='test Server reachability and report supported versions (no credentials required)')
    reach.add_argument('--server', required=True, help='Nexus Server base URL')

    configure = sub.add_parser('configure', help='interactive connection wizard (JSON optional)')
    configure.add_argument('--file', type=Path, help='portable connection template; destination paths are ignored')
    configure.add_argument('--identity', help='existing identity alias, or alias to register with a hidden key prompt')
    configure.add_argument('--server', help='Server URL when adding an identity')
    configure.add_argument('--agent', help='identity hint, or target agent for Server-hosted configuration')
    configure.add_argument('--credential-stdin', action='store_true')
    configure.add_argument('--credential-env')
    configure.add_argument('--host', choices=['connector','server'], default='connector', help='execution host (default: this Connector machine)')
    configure.add_argument('--harness')
    configure.add_argument('--project', help='workspace path on the execution host')
    configure.add_argument('--workspace-label')
    configure.add_argument('--provider-home', help='explicit login directory on the execution host')
    configure.add_argument('--candidate-ref', default='')
    configure.add_argument('--executor-id', default='')
    configure.add_argument('--inventory-revision', default='')
    configure.add_argument('--workspace-id')
    configure.add_argument('--binding-id')
    configure.add_argument('--request-id', help='stable ID to resume configuration after an uncertain response')
    configure.add_argument('--operator-identity', help='imported operator identity to apply Nexus policies after binding approval')
    configure.add_argument('--operator-proof-ref', help='operator approval reference from the binding proposal')

    connect = sub.add_parser("connect", help="guided first-use flow")
    connect.add_argument("--server", required=True,
                         help="Nexus Server base URL")
    connect.add_argument("--agent", default=None,
                         help="agent id hint from the Server screen")
    connect.add_argument("--alias", default=None,
                         help="local identity alias (default: agent id)")
    connect.add_argument("--binding-alias", default=None,
                         help="local binding alias (default: harness)")
    connect.add_argument("--harness", default=None,
                         help="adapter: codex_app_server|pi_rpc|claude_stream")
    connect.add_argument("--executable", default=None,
                         help="explicit harness executable to select")
    connect.add_argument("--pi-node", default=None,
                         help="trusted Node binary for the Pi CLI")
    connect.add_argument("--credential-stdin", action="store_true",
                         help="read the canonical key from stdin")
    connect.add_argument("--credential-env", default=None,
                         help="environment variable holding the key")
    connect.add_argument("--mcp-entry", default=None,
                         help="explicitly selected MCP config file")
    connect.add_argument("--mcp-entry-name", default=None,
                         help="entry name inside --mcp-entry")
    connect.add_argument("--project", type=Path, default=None,
                         help="project directory (default: cwd)")
    connect.add_argument("--start", action="store_true",
                         help="also start a runtime after connecting")
    connect.add_argument("--trusted-provider-home", action="store_true",
                         help="approve using the provider's own home dir")
    connect.add_argument('--provider-home', help='provider login directory on this computer')
    connect.add_argument('--request-id', help='resume the same R4 onboarding request')
    connect.add_argument('--operator-identity', help='imported operator identity for Nexus configuration')
    connect.add_argument('--operator-proof-ref', help='operator approval reference from Nexus')

    identity = sub.add_parser("identity", help="imported identity management")
    identity_sub = identity.add_subparsers(dest="subcommand", required=True)
    add = identity_sub.add_parser("add", help="import a canonical key")
    add.add_argument("--server", required=True)
    add.add_argument("--agent", default=None)
    add.add_argument("--alias", required=True)
    add.add_argument("--credential-stdin", action="store_true")
    add.add_argument("--credential-env", default=None)
    identity_sub.add_parser("list")
    show = identity_sub.add_parser("show")
    show.add_argument("alias")
    remove = identity_sub.add_parser("remove")
    remove.add_argument("alias")
    replace = identity_sub.add_parser("replace-credential")
    replace.add_argument("alias")
    replace.add_argument("--credential-stdin", action="store_true")
    replace.add_argument("--credential-env", default=None)

    executor = sub.add_parser("executor", help="R4 executor registration")
    executor_sub = executor.add_subparsers(dest="subcommand", required=True)
    register = executor_sub.add_parser("register", help="register this host using an imported identity")
    register.add_argument("--identity", required=True, help="imported identity alias")
    register.add_argument("--label", required=True, help="host label shown by the Server")
    register.add_argument("--client-intent-id", default=None,
                          help="explicit registration intent ID; otherwise persisted automatically")
    executor_sub.add_parser("list", help="list local registration intents and executor IDs")
    executor_show = executor_sub.add_parser("show", help="show one Server's executor registration")
    executor_show.add_argument("server_id")

    launch = executor_sub.add_parser("configure-launch", help="stage local launch consent without starting a runtime")
    launch.add_argument("--identity", required=True)
    launch.add_argument("--harness", required=True)
    launch.add_argument("--local-consent-id", required=True)
    launch.add_argument("--profile-revision", type=int, required=True)
    launch.add_argument("--provider-home", type=Path, default=None,
                        help="explicitly approved existing provider login directory")
    launch.add_argument("--secret-ref", action="append", default=[], metavar="NAME=REFERENCE",
                        help="protected vault/provider reference; never credential material")
    realize = executor_sub.add_parser("realize", help="publish this executor workspace and selected installation")
    realize.add_argument("--identity", required=True)
    realize.add_argument("--client-intent-id", required=True, help="reuse this ID after a lost response")
    realize.add_argument("--harness", required=True)
    realize.add_argument("--candidate-ref", required=True)
    realize.add_argument("--inventory-revision", required=True)
    realize.add_argument("--configuration-digest", required=True)
    realize.add_argument("--project", type=Path, required=True)
    realize.add_argument("--workspace-id", default=None)
    realize.add_argument("--label", required=True)
    discovery = executor_sub.add_parser("configure-discovery",
        help="replace the Server executor local passive discovery configuration")
    discovery.add_argument("--server-id", required=True)
    discovery.add_argument("--harness-root", action="append", default=[],
                           help="approved absolute directory for PATH discovery; repeat as needed")
    discovery.add_argument("--pi-install-root", default=None, help="approved Pi releases directory")
    discovery.add_argument("--pi-node", default=None, help="Node executable for the Pi releases")

    probe = executor_sub.add_parser("probe", help="explicitly run the selected installation's sealed version probe")
    probe.add_argument("--server-id", required=True)
    probe.add_argument("--harness", required=True)
    probe.add_argument("--candidate-ref", required=True)
    probe.add_argument("--inventory-revision", required=True)

    discover = sub.add_parser("discover",
                              help="local harness inventory (redacted)")
    discover.add_argument("--server-id", default=None,
                          help="preview persisted executor discovery configuration")
    discover.add_argument("--harness", default=None)
    discover.add_argument("--pi-releases-root", default=None,
                          help="passively enumerate Pi release layouts "
                               "under this installation root")
    discover.add_argument("--pi-node", default=None,
                          help="trusted Node executable pairing the Pi "
                               "releases")
    bind = sub.add_parser("bind", help="advanced binding management")
    bind_sub = bind.add_subparsers(dest="subcommand", required=True)
    bind_prepare = bind_sub.add_parser("prepare", help="prepare a reviewable R4 binding from a published realization")
    bind_prepare.add_argument("--identity", required=True)
    bind_prepare.add_argument("--realization-ref", required=True)
    bind_prepare.add_argument("--replace-binding-id", help="Explicitly replace the reviewed binding realization.")
    bind_prepare.add_argument("--alias", required=True)
    bind_prepare.add_argument("--client-intent-id", required=True)
    bind_apply = bind_sub.add_parser("apply", help="apply the exact reviewed R4 binding proposal")
    bind_apply.add_argument("--identity", required=True)
    bind_apply.add_argument("--prepare-intent-id", required=True)
    bind_apply.add_argument("--client-intent-id", required=True)
    bind_apply.add_argument("--approved-diff-hash", required=True)
    bind_apply.add_argument("--operator-proof-ref", default=None,
                            help="explicit Server operator proof reference, when required")
    bind_create = bind_sub.add_parser("create")
    bind_create.add_argument("--identity", required=True)
    bind_create.add_argument("--harness", required=True)
    bind_create.add_argument("--executable", required=True)
    bind_create.add_argument("--pi-node", default=None)
    bind_create.add_argument("--alias", required=True)
    bind_create.add_argument("--project", type=Path, default=None)
    bind_list = bind_sub.add_parser("list")
    bind_show = bind_sub.add_parser("show")
    bind_show.add_argument("alias")
    bind_remove = bind_sub.add_parser("remove")
    bind_remove.add_argument("alias")
    bind_remove.add_argument("--keep-config", action="store_true")

    runtime = sub.add_parser("runtime", help="managed runtime operations")
    runtime_sub = runtime.add_subparsers(dest="subcommand", required=True)
    start = runtime_sub.add_parser("start",
                                   help="open or reuse an authorized session")
    start.add_argument("alias")
    start.add_argument("--client-intent-id", default=None)
    start.add_argument("--project", type=Path, default=None)
    start.add_argument("--harness", default=None)
    start.add_argument("--new-session", action="store_true")
    start.add_argument("--session-id", default=None, help="select an existing compatible R4 session")
    start.add_argument("--text", default=None,
                       help="initial turn text")
    status = runtime_sub.add_parser("status")
    status.add_argument("--alias", default=None, help="read retained R4 sessions from the Server")
    inspect = runtime_sub.add_parser("inspect")
    inspect.add_argument("session_id")
    inspect.add_argument("--alias", default=None, help="select the retained R4 binding alias")
    logs = runtime_sub.add_parser("logs")
    logs.add_argument("session_id")
    logs.add_argument("--follow", action="store_true")
    submit = runtime_sub.add_parser("submit")
    submit.add_argument("session_id")
    submit.add_argument("text")
    submit.add_argument("--alias", default=None)
    submit.add_argument("--client-intent-id", default=None)
    steer = runtime_sub.add_parser("steer")
    steer.add_argument("session_id")
    steer.add_argument("text")
    steer.add_argument("--alias", required=True)
    steer.add_argument("--client-intent-id", required=True)
    steer.add_argument("--expected-turn-id", default=None)
    steer.add_argument("--current-run", action="store_true")
    interrupt = runtime_sub.add_parser("interrupt",
                                       help="cancel the active turn")
    interrupt.add_argument("session_id")
    interrupt.add_argument("--alias", default=None)
    interrupt.add_argument("--client-intent-id", default=None)
    interrupt.add_argument("--expected-turn-id", default=None)
    interrupt.add_argument("--current-run", action="store_true")
    interrupt.add_argument("--reason", default=None)
    stop = runtime_sub.add_parser("stop",
                                  help="close owned session resources")
    stop.add_argument("session_id")
    stop.add_argument("--alias", default=None)
    stop.add_argument("--client-intent-id", default=None)
    stop.add_argument("--reason", default=None)
    operation = runtime_sub.add_parser("operation", help="query a retained canonical runtime intent")
    operation.add_argument("--alias", required=True)
    operation.add_argument("--client-intent-id", required=True)

    daemon = sub.add_parser("daemon", help="local daemon lifecycle")
    daemon_sub = daemon.add_subparsers(dest="subcommand", required=True)
    daemon_sub.add_parser("start", help="background; returns after readiness")
    daemon_sub.add_parser("run", help="foreground (containers/diagnostics)")
    daemon_sub.add_parser("status")
    daemon_sub.add_parser("stop")

    service = sub.add_parser("service", help="OS autostart management")
    service_sub = service.add_subparsers(dest="subcommand", required=True)
    install = service_sub.add_parser("install")
    install.add_argument("--dry-run", action="store_true")
    uninstall = service_sub.add_parser("uninstall")
    uninstall.add_argument("--dry-run", action="store_true")
    service_sub.add_parser("status")

    sub.add_parser('reconnect', help='gracefully restart the daemon to reconnect all configured connections')
    status = sub.add_parser('status', help='show live server and agent connection health')
    status.add_argument('--agent', help='show one agent by canonical agent id')
    logs = sub.add_parser('logs', help='inspect persistent daemon logs')
    logs.add_argument('--follow', action='store_true', help='stream new log entries until Ctrl+C')
    logs.add_argument('--errors', action='store_true', help='show warnings and errors only')
    logs.add_argument('--tail', type=int, choices=range(1, 10001), default=50,
                      metavar='1..10000', help='maximum recent lines (default: 50)')
    doctor = sub.add_parser("doctor", help="layered diagnostics")
    doctor.add_argument("--probe", action="store_true",
                        help="attempt live network probes")

    approvals = sub.add_parser("approvals",
                               help="pending HITL requests (decision "
                                    "authority stays on the Server)")
    approvals_sub = approvals.add_subparsers(dest="subcommand",
                                             required=True)
    approvals_sub.add_parser("list", help="list pending requests")
    decide = approvals_sub.add_parser("decide")
    decide.add_argument("request_id")
    decide.add_argument("decision", choices=["approve", "deny"])
    decide.add_argument("--cas-token", default="",
                        help="CAS token correlating the pending request")

    mcp = sub.add_parser("mcp-config",
                         help="direct-HTTP MCP client configuration")
    mcp_sub = mcp.add_subparsers(dest="subcommand", required=True)
    mcp_plan = mcp_sub.add_parser("plan")
    mcp_plan.add_argument("--server", required=True)
    mcp_plan.add_argument("--harness", required=True,
                          choices=["codex_app_server", "claude_stream",
                                   "pi_rpc"])
    mcp_plan.add_argument("--capability-ref", required=True,
                          help="mcp-cap: reference issued by the Server")
    mcp_apply = mcp_sub.add_parser("apply")
    mcp_apply.add_argument("--server", required=True)
    mcp_apply.add_argument("--harness", required=True)
    mcp_apply.add_argument("--capability-ref", required=True)
    mcp_apply.add_argument("--file", type=Path, required=True)
    mcp_apply.add_argument("--entry-name", default="nexus")
    mcp_remove = mcp_sub.add_parser("remove")
    mcp_remove.add_argument("--harness", required=True)
    mcp_remove.add_argument("--file", type=Path, required=True)
    mcp_remove.add_argument("--entry-name", default="nexus")

    harness = sub.add_parser('harness-config', help='inspect, validate and apply Core harness parameters')
    harness_sub = harness.add_subparsers(dest='subcommand', required=True)
    describe = harness_sub.add_parser('describe', help='show portable Core parameters; native availability needs a live observation')
    describe.add_argument('--harness', required=True, choices=['codex_app_server', 'claude_stream', 'pi_rpc'])
    validate = harness_sub.add_parser('validate', help='validate a reusable JSON settings file without applying it')
    validate.add_argument('--file', type=Path, required=True)
    validate.add_argument('--harness', default=None)
    for command in ('show', 'apply'):
        item = harness_sub.add_parser(command, help='read or update canonical settings using an authorized imported identity')
        item.add_argument('--identity', required=True)
        item.add_argument('--endpoint-id', required=True)
        if command == 'apply':
            item.add_argument('--file', type=Path, required=True)
            item.add_argument('--expected-revision', type=int, required=True, help='revision reviewed with harness-config show')
    connection = sub.add_parser('connection-config', help='reuse a complete Nexus connection JSON')
    connection_sub = connection.add_subparsers(dest='subcommand', required=True)
    for command in ('validate', 'apply'):
        item = connection_sub.add_parser(command)
        item.add_argument('--file', type=Path, required=True)
        if command == 'apply':
            item.add_argument('--identity', required=True, help='imported operator identity')
            item.add_argument('--agent', required=True)
            item.add_argument('--request-id', required=True, help='stable ID for safe retries')
            item.add_argument('--executor-id', default='')
            item.add_argument('--candidate-ref', default='')
            item.add_argument('--inventory-revision', default='')
            item.add_argument('--workspace-id', default=None)
            item.add_argument('--binding-id', default=None, help='existing connection to replace')
            item.add_argument('--project', help='workspace folder on the destination host; never imported')
            item.add_argument('--workspace-label', default=None)
            item.add_argument('--provider-home', help='login directory on the destination host; never imported')
            item.add_argument('--execution-location', choices=['local','remote','all'], default='local')
    context = {'json': False}
    def shared_options(item):
        item._output_context = context
        if item is not parser:
            item.add_argument('--json', action='store_true', default=argparse.SUPPRESS,
                              help='machine-readable JSON output')
            item.add_argument('--verbose', action='store_true', default=argparse.SUPPRESS,
                              help='include technical details')
        for action in item._actions:
            if isinstance(action, argparse._SubParsersAction):
                for child in action.choices.values():
                    shared_options(child)
    shared_options(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    output = Output(json_mode=args.json, verbose=args.verbose)
    from .commands import dispatch
    try:
        result = asyncio.run(dispatch(args, output))
    except ConnectorError as error:
        return output.error(error)
    except EOFError:
        return output.error(ConnectorError('VALIDATION_ERROR','cli','Terminal input ended. Configuration canceled.'))
    except KeyboardInterrupt:
        output.error(ConnectorError('INTERRUPTED', 'cli', 'Operation interrupted.'))
        return EXIT_USAGE
    if result is None:
        return EXIT_OK
    if isinstance(result, int):
        return result
    if args.command == "discover" and not args.json and not args.verbose:
        from .commands.discover import render_summary
        render_summary(result, output, harness=args.harness)
    elif args.command == 'doctor' and not args.json and not args.verbose:
        output.heading('Diagnostic checks')
        output.table(['Layer', 'Check', 'Status', 'Detail'],
                     [[c['layer'], c['name'], c['status'], c['detail']] for c in result['checks']])
        actions = [c for c in result['checks'] if c.get('action')]
        if actions:
            output.heading('Next steps')
            for check in actions:
                output.line(f"  {check['name']}: {check['action']}")
        output.line('\nDetails: doctor --verbose | JSON: doctor --json')
    else:
        output.result(result)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
