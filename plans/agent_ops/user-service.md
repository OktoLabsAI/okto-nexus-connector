# R3 lifecycle increment — Connector user-service host

Branch `feature/v0.0.12`; Core pin remains `0.0.14`. Fetched `origin/main` again
before continuing; the feature branch contains all commits from that base.

The Windows host now uses the shared Core task definition with per-instance
ownership, native-export fingerprints and saved execution context. Install
preflights before drain, start routes through an installed task, stop persists
same-login suppression before canonical IPC drain, and uninstall preserves data.
Foreign/legacy tasks and changed definitions are retained for explicit review.
Lost native responses remain pending until ownership can be established.

Detached launches now serialize the parent/child handoff with an OS guard and a
durable nonce receipt. Both detached and foreground production entrypoints own
the lifetime lock before state/vault/runtime construction. Three concurrent real
CLIs converge on one generation, survive launcher exit, reuse the same daemon,
restart only after drain and verify the final generation-correlated receipt.
No second runtime authority, supervisor loop or work queue was introduced.

The stable context excludes arbitrary environment variables and provider keys.
Existing proxy references resolve ephemerally at launch; existing vault approval
continues to control secret access. Windows receipt replacement tolerates bounded
sharing contention without overwriting the previous value on persistent failure.

Validation evidence is tracked in Nexus under
`plans/agent_ops/evidence/pr02-connector-user-service*`: 72 focused tests,
90 runtime/lock/proxy regressions and 2 clean-venv packaging/lifecycle tests.
The final candidate wheel is compared byte-for-byte with every Python source
payload, including the five new lifecycle modules. Native checks only validate
task XML and read absence; registration/removal tests use a fake backend.

An initial COM scope returned objects while uninitializing their owning thread;
that run emitted access violations despite a pytest success summary and was
rejected as evidence. The adapter now returns plain data and releases its COM
references/exception frames before balancing initialization on the calling
thread. Subsequent native worker-thread validation is included in the final run.

Full SVC acceptance remains open: actual service/login/logout/crash/credential
and provider campaigns, enable/disable/reconciliation UX, interrupted pre-spawn
recovery, version upgrade, remaining passive diagnostics and POSIX qualification.
Legacy systemd/launchd global installers are explicitly not qualified for the
R3 ownership contract. Native Task Scheduler has no atomic fingerprint CAS
against an external editor; this limitation is not hidden by the fake tests.

Rollback uses canonical drain before reverting code or changing supervisor;
remove an owned task through its matching version before discarding the receipt.
No schema, NXL protocol, credentials or personal installation was migrated.
