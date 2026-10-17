# M6b: synchronous CLI client for the opt-in pipe

The existing `IPCClient` now accepts a `named_pipe` readiness record from the
experimental M6 Connector daemon. A private Proactor thread owns overlapped
pipe reads/writes and joins on close; CLI calls retain the existing framed JSON,
token hello and response semantics. The client rejects nonlocal or non-M6 pipe
names, bounds connection and hello waits, and does not change the default
Unix-socket/loopback selection. The pipe server remains explicit opt-in.

The disposable daemon process test used the **real** `DaemonApp`, Core host,
birth-token-verified readiness, `IPCClient` and its CLI `ping` helper. It
rejected a wrong token, then executed ping, status, reconnect/ping and shutdown
through the pipe. It checked that the client I/O threads had joined. A separate
test refuses a remote pipe path before starting a worker. Four selected
regressions covered the default IPC path.

Eight selected source tests and the same eight against an installed Connector
wheel and the exact vendored Core 0.0.12 wheel passed. Both installed modules
resolved under the test environment's `site-packages`. Connector 0.0.10 wheel
SHA-256: `819365a3f7b3dd9f690ee736b7fb731755d5c52053dda444b5a469b9b5e785dc`.
The environment reused third-party packages already present on this machine;
it was not a clean-index dependency resolution.

This proves only the CLI IPC client and a short daemon lifecycle. It does not
prove live native sessions survive Nexus disconnect, valid reauthorization,
lease revocation, stale-generation refusal, upgrade/rollback, cost advantage
or UI-B/UI-C. The global Nexus remains stopped, the installed Connector still
uses its established default IPC transport, and M6 adoption remains undecided.
