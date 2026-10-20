# M8 — Installed child-process qualification foundation

Status: validated checkpoint, 2026-10-17. Full M8 and M5 remain pending.

The lifecycle fixture previously forced every child to import the checkout via
`PYTHONPATH`, even when its parent qualified an installed wheel. It now removes
inherited Python paths, disables user-site imports, runs children outside the
checkout and uses the same installed package as the parent. Source-mode tests
explicitly retain source imports only when their parent actually imports source.
A poison-package test proves inherited shadow modules cannot replace that package.

The terminal-independence test now executes the actual `daemon start` CLI and
waits for that client to exit before checking the daemon through another client.
Previously it only tested the exit of a status client. A forged IPC token cannot
shut down the surviving daemon. Concurrent starts retain exactly one starter and
one authenticated owner; either contender may win. Cleanup first asks that private
test daemon to drain, joins owned starter handles and checks that no live owner
remains, without clearing evidence of a possible survivor.

## Qualification

**11 installed tests passed, zero skips**, with unchanged inputs and package-byte
verification against Core and Connector wheels. Tests include two real Windows
Job Object restrictions (detached startup refusal and supported foreground stop),
concurrent real daemon processes, actual CLI exit, authenticated shutdown and IPC
framing. The first campaign found a test assumption: the Windows virtualenv
launcher PID need not equal its child daemon PID. The test now checks starter
lifetimes and the private authenticated readiness record independently.

Artifacts remain the already qualified M3i wheels: no runtime product code changed
in this checkpoint. Exact hashes, input digest, duration and test files are in
[runtime-lifecycle-m8-process-results.json](runtime-lifecycle-m8-process-results.json).
The installed runner executed in a temporary cwd using isolated Python (`-I`) and
checked every packaged file before running pytest. Local JUnit and manifest:
`build/runtime-lifecycle-m8-process-installed/`.

To repeat the selected cases with the same isolated, non-editable environment:

```powershell
rtk proxy ../okto-nexus-connector-core/build/inventory-m3i-env/Scripts/python.exe -I -m pytest -c pyproject.toml --rootdir=. --confcutdir=tests tests/integration/test_process_lifecycle.py tests/integration/test_windows_job_daemon.py tests/unit/test_ipc_protocol.py
```

These are process and transport tests. They do not qualify native provider work,
cross-host behavior, or the full M5 differential workflow. The global Nexus and
Connector installations were not restarted or changed.
