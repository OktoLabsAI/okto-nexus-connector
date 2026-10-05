# Receipt observation watchdog — 2026-10-02

Hosted run https://github.com/OktoLabsAI/okto-nexus-connector/actions/runs/36957189122
at `0b71e23d445fc7ab00012672dc5a0aaea27e4eed` completed with five matrix
cells passing and Windows/Python 3.11 failing: 674 passed, 5 failed, 2 skipped.
The controlled-clock lease ordering cases passed in every cell.

The failure-only task diagnostics locate all five receipt observation timeouts
in Core durable storage: journal open, owned-slot ledger initialization, or
receipt lookup. They do not establish a deadlock or native provider failure.
The previous three-second observation watchdog included thread scheduling and
filesystem initialization, although these tests specify behavior rather than
a three-second performance requirement.

The shared `observed` helper now allows fifteen seconds and still surfaces owner
failure immediately, bounds the wait, and reports pending coroutine locations.
No production timeout, lease duration, cancellation deadline, retry, assertion,
or skip was changed. This does not prove production latency acceptable.

Directed validation against the installed development Core/Connector packages:
`test_approved_mcp_launch`, `test_approved_native_launch`, `test_r4_publications`,
`test_r4_execution`, and `test_r4_lease_renewal`: **66 passed in 39.72 seconds**.
JUnit: `storage-observation-watchdog.xml`. Hosted confirmation remains pending.
