# Receipt observation diagnostics — 2026-10-02

CI run 36954905240 at 1f9b29c, Windows/Python 3.12 job 110675607291:
676 passed, 2 failed, 2 skipped. Both failures were the three-second
`observed` deadline in `test_approved_mcp_launch.py`, positive secret-reference
cases for Codex and Claude. No owner failure was reported before timeout.
The log does not identify the pending stage; cause remains unverified.

The common receipt observer now reports pending task names and nested await
function/line locations on its own timeout. An underlying TimeoutError from
the producer remains unchanged. Diagnostics do not include frame locals or
coroutine arguments. The same helper is shared with the renewal cleanup
diagnostic. Deadlines, lease values, product code and assertions are unchanged.

Directed diagnostic verification passed: an underlying producer TimeoutError
retains object identity; an actual observer deadline includes the held task's
await location; a synthetic secret in that coroutine's locals is absent from
both diagnostic outputs.

Local installed-package suite: 37 passed in 17.10 seconds for
`test_approved_mcp_launch.py`, `test_r4_lease_renewal.py` and
`test_r4_execution.py`. JUnit: `receipt-observation-local.xml`.
Packages are the same Core .52 and Connector issue-1 wheel documented in
`renewal-cleanup-ci.md`. Root-cause diagnosis and hosted regression remain open.
