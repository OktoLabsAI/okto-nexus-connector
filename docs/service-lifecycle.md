# Connector lifecycle readiness

`okto-nexus-connector daemon status --json` reports the local component,
instance identity, generation, active and installed versions, and local
readiness separately from remote link/work readiness. Select an instance with
the global `--state-dir` option. Status does not create an absent state directory.

Ownership requires both an OS-held guard and matching process birth evidence.
The authenticated IPC response must agree on PID, instance, generation and
protocol/package version. A live owner with unavailable IPC is degraded, not
absent. Start waits for that same owner within its observation deadline and
never treats a transport error as permission for a competing launch. A draining
or incompatible owner is not reported as ready.

The instance is identified by canonical state directory, effective OS principal
and component. It is independent of Server/executor/agent identity. `remote_link`
and `work_state` remain `not_checked` in this local readiness projection; use the
existing status/doctor and canonical runtime operations for those facts.

Lifecycle stop sends the observed generation through the existing authenticated
IPC shutdown path. A durable shutdown receipt correlates its outcome. If the
process disappears without that confirmation, the CLI returns an uncertain
outcome instead of claiming successful drain. No PID file authorizes a kill.
Status exits 0 for ready/stopped, 3 for unready states; an unconfirmed stop exits
4. Existing foreground and daemon command names remain valid.

`python -m okto_nexus_connector.daemon --state-dir /absolute/path` respects the
explicit selection and remains in foreground for an external supervisor. The
existing state, vault, execution records, remote registrations and native
reconciliation remain authoritative. Transport shutdown joins attachment cleanup
without issuing a second cancellation during HTTP connection closure.

R3 qualification so far covers Windows disposable-process readiness/reuse/drain
and the installed wheel. The instance-scoped OS service definitions, saved
execution context, full launch serialization, broader passive diagnostics and
real remote/provider/login/logout campaigns remain in progress. Existing service
templates must not be interpreted as evidence for logout or boot survival.
