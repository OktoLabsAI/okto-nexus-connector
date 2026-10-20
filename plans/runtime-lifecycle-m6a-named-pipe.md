# M6a: opt-in Windows named-pipe prototype

The existing Connector daemon dispatcher and Core host can now be reached
through an experimental local named pipe. The default Unix-socket/loopback
transport is unchanged. `IPCServer.start(..., transport='named_pipe')` and
`DaemonApp(..., ipc_transport='named_pipe')` are explicit opt-ins; the CLI and
installed service do not select them.

The adapter creates a private random pipe name and an explicit DACL with one
allow ACE for the current logon SID. It uses `FILE_FLAG_FIRST_PIPE_INSTANCE`,
`PIPE_REJECT_REMOTE_CLIENTS` and overlapped I/O through the Windows Proactor
loop. Existing newline-bounded JSON frames, 64-connection application cap,
15-second hello deadline and independent IPC token check remain in the
dispatcher path. The opt-in dependency is `m6-prototype` (`pywin32` on
Windows). [Microsoft's named-pipe security documentation](https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipe-security-and-access-rights)
explains the logon-SID boundary; [CreateNamedPipeW](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-createnamedpipea)
documents the remote-client and overlapped flags.

Eight focused tests passed from source and the same eight from the built
Connector 0.0.10 wheel installed with the exact Core 0.0.12 vendor wheel in
a separate environment. The tests include wrong-token refusal before
dispatch, a DACL with only the logon SID, normal status dispatch, the existing
IPC protocol cases, and a disposable **separate** daemon process that opens
the real Core ledger, publishes birth-token-verified readiness, answers
status over the pipe and exits after authenticated shutdown. The installed
module path resolved under the environment's `site-packages`; Connector wheel
SHA-256 is `d9cc9bf7a7ca5202eb37193a0e280aa2deef5be1d23a8a4fe2adb7ec15496bf1`.
Third-party dependencies were reused from the machine rather than resolved
from a clean package index.

This proves the experimental IPC path, not independent availability. The
standard synchronous CLI client does not yet speak named pipes. No active
native session, Server reconnect, lease revocation, stale-generation refusal,
upgrade/rollback or memory/latency comparison was qualified. The global
Nexus and Connector services stayed stopped. The M6 adoption decision remains
open, and this adapter is not an installed default.
