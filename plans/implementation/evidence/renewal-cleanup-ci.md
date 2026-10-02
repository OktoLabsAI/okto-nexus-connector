# Renewal cleanup CI follow-up — 2026-10-02

Run [36954093904](https://github.com/OktoLabsAI/okto-nexus-connector/actions/runs/36954093904)
at 959d5f7 completed with five passing matrix cells and one failure:
Windows/Python 3.13, job 110673092951, 677 passed / 1 failed / 2 skipped.
The failure was
`test_cancelled_cleanup_observer_does_not_abandon_pending_renewal[True]`:
the second public `owner.close()` exceeded the test's three-second observer
deadline while awaiting its shielded cleanup task. The hosted traceback does
not identify the nested producer or cleanup stage. Root cause remains open;
this is not evidence of a fixed production defect or a passing matrix.

Local installed-wheel baseline: all six renewal tests passed in 7.01 seconds.
The diagnostic revision also passed all six in 6.93 seconds. It preserves the
same three-second limit and reports task names and nested await function/line
locations on timeout, without printing coroutine arguments or local values.
It additionally asserts that the second close awaits the original retained
cleanup task. Production code and lease timing are unchanged.

Command: installed environment Python `-I -m pytest
tests/unit/test_r4_lease_renewal.py -q -o pythonpath=.
--junitxml=.../connector-renewal-diagnostic.xml`.
JUnit: `renewal-cleanup-diagnostic.xml`.
Packages: Core 0.2.52.dev0 SHA256
`470eb23b28d917a3a154c2ef7cd9fdddf972ca6402c0668961b4c01909f1b732`,
Connector 0.5.0.dev0 SHA256
`6e294c0ed92993a93f5a16b01eee86e3ff4f8e8c63f941300d3e25fd88a165fc`.
Synthetic native peer; no provider or independent-host acceptance claimed.
