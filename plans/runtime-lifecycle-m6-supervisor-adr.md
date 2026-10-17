# ADR M6 — Evaluate the existing Connector as the independent local host

Status: experiment design, not adopted. 2026-10-17.

## Decision to test

Reuse Connector's daemon and Core host as the candidate supervisor. Do not create
a fourth product or duplicate its process, journal, lease and containment owners.
Run it against a disposable Nexus home and a private Connector state directory.
Nexus remains the admission/authorization owner; the daemon owns only processes
covered by current grants. Keep the global installation on the qualified path
until an installed comparison demonstrates an improvement.

This decision selects the experiment, not the eventual default architecture.

## Evidence and the actual availability constraint

The M8 process checkpoint proves that the installed CLI can exit without stopping
the daemon and that authenticated local shutdown remains required. The existing
daemon already holds Core journals and native handles independently of that CLI.

However, process separation alone does not preserve availability across Nexus
restart. In `R4DaemonControl._attempt` cleanup, a lost connection calls
`R4DaemonExecution.close(preserve_leases=True)`. That method first stops the old
execution consumer, retains the host while its existing leases settle, then joins
`shutdown_executor` before allowing connection replacement. This is a deliberate
authority fence, not evidence that sessions can resume on the next connection.
The experiment must measure the resulting unavailable interval. It must not call
a still-running but uncontrollable native process an available agent.

Removing this fence or extending TTLs is not an acceptable prototype shortcut.
M5 must establish any transfer/rebind policy in Core with proof of the exact
session, current generation, valid lease and prior consumer fenced. If that
cannot be established, retain containment and report its measured downtime.

## IPC experiment boundary

Connector currently uses a user-restricted Unix socket where supported and a
token-authenticated loopback TCP port on Windows. The latter is not the named
pipe required by M6. The experimental Windows adapter must use a local-only named
pipe with an explicit DACL for the intended user/logon trust domain, a private
random name and the existing authenticated request protocol. It must reject
remote clients, cap frames/connections, bound authentication time and retain
cancelled I/O until its handles are actually closed. No pickle deserialization,
implicit administrative service, broad Everyone ACL or public TCP bind.

Use overlapped I/O rather than `PIPE_NOWAIT` as an asynchronous substitute. The
Microsoft API documents that mode as compatibility behavior. The Windows access
check uses the pipe DACL; a logon SID can restrict access across logon sessions.
Sources: [CreateNamedPipeW](https://learn.microsoft.com/en-us/windows/win32/api/namedpipeapi/nf-namedpipeapi-createnamedpipew),
[Named pipe security](https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipe-security-and-access-rights),
[Pipe modes](https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipe-type-read-and-wait-modes).

The prototype must call the existing daemon dispatcher and host. A separate
ping-only pipe server would qualify IPC mechanics but would not qualify M6.
Keep the adapter isolated from packaged defaults until the adoption decision.

## Comparison and acceptance

Use identical harness versions, configuration, authorized task and concurrency
for embedded and the separate local Connector. Record package hashes, process
identities, physical memory and monotonic timestamps at each boundary.

1. Start one persistent session and a warm one-shot pool; verify actual responses.
2. Restart only the disposable Nexus, preserving its home. Measure last usable
   operation, disconnected interval, first valid new admission and first result.
3. Interrupt the control connection without stopping the host. Work already
   authorized may settle only within its original lease. New operations must be
   refused or queued by Nexus with deadlines, never accepted from stale IPC.
4. Expire/revoke a lease, change generation and restart the host. Confirm retained
   spool replay, exact receipt correlation, no duplicated native effect, no
   recovered capacity before physical release and no unauthorized continuation.
5. Stop during open, publication and pipe authentication. Join the original
   producers and prove all experiment handles/processes are gone.
6. Exercise upgrade/rollback of the host package with explicit incompatible
   contract refusal and preserved configuration/evidence. Measure installation,
   memory and shutdown cost as well as availability.
7. Run the corresponding provider flows through Nexus UI at UI-B/UI-C. Automated
   synthetic process tests are preparatory evidence, not that gate.

Adopt only if usable availability improves without weaker revocation, duplicate
owners or an additional permanent recovery path. Otherwise retain the shared
Core library in the embedded host and record why this experiment did not improve
the outcome. A local Connector is not a second physical machine qualification.

## Pending

Named-pipe adapter/prototype, separate-process M5 differential cases, installed
comparison measurements and adoption decision are all still pending. This ADR
does not claim a supervisor implementation or M6 completion.
