# M6c: pipe shutdown with pending authentication and Windows process liveness

An opt-in named-pipe client can connect and then leave the hello frame pending.
The disposable **separate** daemon process now has a test that sends an
authenticated CLI shutdown through a second pipe connection while the first
is blocked in its overlapped hello read. It verifies the shutdown response,
EOF on the stalled connection, removed readiness, and exit of the original
birth-token-identified process. No test daemon remains.

That stronger exit check exposed a production liveness defect. The Connector's
Windows `process_identity` read `GetProcessTimes` and accepted the creation
time even after process termination while a caller retained an OS handle.
Consequently `same_process` could report an exited daemon as live. The probe
now opens with `SYNCHRONIZE`, checks the process object's zero-timeout
`WaitForSingleObject` state both before and after reading its birth time,
and retains the existing birth-token format. An exited, still-handle-backed
process is no longer a live lock owner. [Microsoft's wait API](https://learn.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-waitforsingleobject)
defines the signaled and timeout states; [GetProcessTimes](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-getprocesstimes)
does not by itself certify liveness.

Sixteen selected source tests passed for lock identity, process liveness and
the pipe process lifecycle. Twenty selected tests passed from a built and
installed Connector wheel with the exact vendored Core 0.0.12 wheel; the
installed campaign also included the default daemon process lifecycle tests.
The first attempt at those subprocess tests used a test venv that omitted
`rfc8785` and `websockets` from its isolated site-packages; after installing
the declared dependencies into that venv, the installed campaign passed.
Connector wheel SHA-256:
`beb7302c34049de36dcd36a06a508fcb3b05c2d7a27149f70e3a68e7dd55d8e6`.

The global Nexus stayed stopped. This checkpoint covers one shutdown frontier
and repairs liveness observation; it does not establish session continuity,
control reconnect, revocation, cost advantage, or the UI-B/UI-C gate. M6
adoption remains undecided.
