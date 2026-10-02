# Cleanup renewal scheduling — October 2, 2026

Hosted run `36981974955` at `37726bd` failed on Windows/Python 3.13 while
`test_cancelled_cleanup_observer_does_not_abandon_pending_renewal` waited for the
renewal request to enter. This test used a real-clock 1.6-second synthetic lease
to verify cleanup ownership, making entry depend on scheduling and storage time.

A deliberate 1.7-second event-loop pause reproduced the same observation timeout
before cleanup started. The normal case passed and the delayed case failed.
The test now uses the public clock injection shared by Core and the Connector,
as the existing renewal/control ordering test does. Its lease duration and both
three-second observation limits are unchanged. Both normal and delayed-loop
cases verify that cancelling the cleanup observer leaves the renewal and the
same cleanup task owned until the held request returns and native cleanup ends.

A separate negative case explicitly advances the shared clock to the initial
deadline. It requires LEASE_EXPIRED, a closed connection, no renewal request,
unchanged deadline and exactly one native open. Existing real-time automatic
renewal and refusal tests remain unchanged. Application code and package bytes
are unchanged; this fixes test-purpose isolation, not a demonstrated daemon bug.
The controlled reproduction does not establish the exact timing of the hosted
failure, which still requires a new CI result.

The first experiment delayed the synthetic peer response rather than scheduling
the renewal task; both cases passed. Its `renewal-order-before.xml` report is
retained and is not evidence of the defect. The corrected experiment is
`renewal-scheduling-before.xml` (one pass, one expected failure).

## Installed verification

- Windows/Python 3.13.1: 59 passed in 28.99 s.
- WSL Linux/Python 3.12.13: the same 59 cases passed in 31.75 s.
- Scope: full renewal, daemon control and publication modules.
- Commands: installed interpreter `-I -m pytest -q -o pythonpath=.` followed by
  the three modules. The repository root is exposed only for `tests.unit`
  fixtures; `src` is not added. Post-run package checks matched the installed
  Connector and Core bytes to the pinned wheels.
- Connector wheel SHA-256:
  `508eaf654ec34e8fa8ff513d3c78c46d86cfc0555475cd91a721c9458b511bea`.
- Core wheel SHA-256:
  `cc873031378793d374a9bbc00572c7a525f99c4246324a941713d866cc62c6b1`.

[Windows report](evidence/renewal-scheduling/renewal-final-windows.xml),
[Linux report](evidence/renewal-scheduling/renewal-final-linux.xml),
[Windows package check](evidence/renewal-scheduling/windows-package-check/installed.json),
[Linux package check](evidence/renewal-scheduling/linux-package-check/installed.json).

After adding failure-observer diagnostics, its execution/publication/renewal
modules passed 43 checks on Windows (40.20 s) and the same 43 on Linux (34.57 s).
[Windows](evidence/renewal-scheduling/failure-observer-windows.xml) and
[Linux](evidence/renewal-scheduling/failure-observer-linux.xml) reports overlap
the preceding 59-case campaigns; do not add these as unique test counts.

The same hosted run had four Windows/Python 3.12 timeouts: lost publication reply,
local change during discovery, nonempty claim history, and recovery after Core
commit before projection. All passed locally. The first three now supply public
daemon status to the existing timeout helper; no deadline or polling interval
changed. The shared failure observer also retains pending-count and task-location
diagnostics without serializing exception text or coroutine arguments. These
four causes remain unresolved. Full CI, final artifacts and independent
host/provider acceptance remain pending. No delivery gate closes.
