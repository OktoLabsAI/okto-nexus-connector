# Renewal ordering clock — 2026-10-02

Run 36956057104 at 64c9e9a passed five cells; Windows/Python 3.13 failed
`test_interrupt_progresses_while_productive_work_waits_for_renewal` with
`LEASE_EXPIRED` (677 passed / 1 failed / 2 skipped). This test held a renewal
while relying on the remainder of a 1600 ms real lease for disk-backed receipt
publication and a subsequent productive call. Runtime expiry was correct.

That ordering test now injects the same monotonic clock into Core's public
`create_runtime` and the Connector execution owner. The renewal request advances
the clock before send; waiting for disk cannot consume this synthetic window.
A second case moves the clock to the original deadline before the grant reply
and requires LEASE_EXPIRED with only the interrupt reaching the native peer.
The existing automatic-renewal test still uses real monotonic time. Product
lease checks and all observation deadlines remain unchanged.

Installed Core .52 and Connector issue-1 wheel: seven renewal tests passed in
7.95 seconds. Report: `renewal-ordering-clock.xml`. The first controlled-clock
attempt correctly failed because renewal send time equaled initial send time;
the corrected fixture advances before creating the renewal attempt. Hosted CI
and any other intermittent observation failures remain open.
