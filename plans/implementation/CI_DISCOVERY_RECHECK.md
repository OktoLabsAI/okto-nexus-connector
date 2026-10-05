# Discovery CI recheck — October 2, 2026

At `e09fa21`, CI run 36969483960 passed five cells but Windows/Python 3.13 failed
`test_default_daemon_publishes_retained_observation_without_probing`: the three-second
wait for control readiness expired. That cell reported 698 passes and two skips;
build was skipped. The preceding run 36969272692 at `768a688` failed instead on
Windows/Python 3.11 while waiting for the passive reader to be entered in
`test_stop_joins_cancelled_passive_reader_without_publishing`.

Neither log includes the daemon phase or sanitized error at timeout. These logs
do not distinguish slow startup from a failed attempt. The cause remains open;
no timeout increase, retry or product behavior change is justified by this evidence.

## Current verification

On Windows/Python 3.13.1, using the installed Connector development wheel
`508eaf654ec34e8fa8ff513d3c78c46d86cfc0555475cd91a721c9458b511bea` and Core wheel
`cc873031378793d374a9bbc00572c7a525f99c4246324a941713d866cc62c6b1`:

- The observation module passed 11 tests in 1.45 s.
- The unchanged full suite passed **699 tests, two skips, in 322.25 s**. This includes
  the clean-environment packaging tests. It did not reproduce either hosted timeout.
  Its three CLI E2E tests still injected the checkout through `PYTHONPATH`; this
  full run alone is therefore not evidence that every child used installed code.
- That CLI fixture now removes `PYTHONPATH`/`PYTHONHOME`, uses `-I` for CLI and
  foreground daemon processes, and starts them in the temporary state directory.
  All three CLI scenarios passed again in 19.59 s against the installed package:
  identity/daemon/refusal, MCP configuration plan/apply/remove, discovery/doctor.
  [Isolated CLI report](evidence/ci-discovery-recheck/connector-cli-installed-only.xml).
- The shared test wait helper now adds the caller-supplied public daemon status
  to its TimeoutError. The two affected waits supply that status. The timeout,
  polling interval, assertions and application code are unchanged.
- After that diagnostic-only edit, 54 affected tests passed in 4.75 s.
- Post-run package byte verification matched the installed Connector and Core
  against their pinned wheels. The test runner also contains Nexus; the packaging
  test separately checks a clean Connector installation without Nexus/MCP.

Commands used the installed interpreter with `-I -m pytest`, repository test files
and the repository pytest configuration (which has no source `pythonpath`). The
full run used `--maxfail=1`; all tests completed. Connector tracked files were clean
before the full run and remained clean until the diagnostic edit afterward.

[Full report](evidence/ci-discovery-recheck/connector-current-full.xml),
[directed report](evidence/ci-discovery-recheck/connector-discovery-diagnostics.xml),
[isolated observation report](evidence/ci-discovery-recheck/connector-observation-current.xml),
[installed package check](evidence/ci-discovery-recheck/installed.json).

Current hosted confirmation, timeout diagnosis, final artifacts, independent-host
and platform/provider acceptance remain pending. No release gate is closed.
