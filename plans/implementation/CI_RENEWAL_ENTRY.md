# Renewal entry scheduling and installed regression

The hosted `d2e240e` run finished with five successful OS/Python cells and one
Windows/Python 3.13 failure: two renewal cases timed out before entering renewal.
An intentional 1.7-second event-loop pause reproduced both failures locally with
the original 1.6-second lease. Added diagnostics identified `LEASE_EXPIRED` in
both owners. The product correctly refused a late renewal; the test never reached
the renewal-success or authority-refusal behavior it intended to observe.

The cases now share Core's public injected clock with the daemon, as the existing
renewal ordering tests already do. They still run normal and delayed-loop cases,
retain the 1.6-second grant and three-second observation limit, and run the real
renewal task. The positive case advances monotonically past the initial deadline
only after a renewed grant is installed, then checks ACTIVE ownership and one
native open. Refusal and revoked-authority cases preserve the original deadline.
The separate expired-lease negative still refuses another renewal request.
No product lease, timeout, retry or containment policy changed.

## Results on 2026-10-02

- Before: two delayed-loop failures and one passing cleanup control. A second
  diagnostic reproduction confirmed `LEASE_EXPIRED` for both failures.
- Affected installed regression: 45 passed on Windows/Python 3.13.1 (33.48 s)
  and WSL Linux/Python 3.12.13 (29.05 s).
- Complete installed Windows suite: 716 passed, two skipped (309.53 s).
- Complete installed Linux attempt: 710 passed, six skipped, two packaging
  setup errors (255.43 s). The laboratory environment lacked the declared
  `build` test dependency. After installing it, both packaging cases passed
  (25.58 s), including a newly built wheel in a clean venv with pinned Core.
  The original failed full report is retained; this is not a single green Linux
  full-suite run.

The full suites preceded the final event-observer adjustment described below.
Its affected event/renewal follow-up passed 23 checks per OS. A final monotonic
boundary guard passed both positive renewal variants per OS. Tests use `-I` and
`pythonpath=.` for test helpers, without adding application `src` paths.

The Windows skips are the legacy audit peer's incomplete binding contract in
`test_27_start_session_capability_is_referenced_in_effective_environment`, and
the platform-conditional shim case marked covered above. Linux skips include
Windows Job Object/npm-shim/composite-selection conditions and that legacy peer
limitation. They are not claimed as successful scenario execution.

Application bytes remain the `c110246` development wheel:
`46991823a500fef326917edeb6063fb6e570126731e35d8c9c349104a1740b8f`.
Core remains .53 with wheel hash
`cc873031378793d374a9bbc00572c7a525f99c4246324a941713d866cc62c6b1`.
Post-campaign checks compared installed package trees with the pinned wheels on
both OSes. This tests/docs increment does not require a new application wheel.
It is not the final M13 package/commit freeze or independent-host qualification.

## Later hosted event timeout

The [c110246 Windows/Python 3.12 job](https://github.com/OktoLabsAI/okto-nexus-connector/actions/runs/36990826663/job/110786384717)
recorded 713 passes, two skips and a timeout in
`test_retry_without_new_event_converges_without_repeating_native_effect[read]`.
The case passes in both local full runs, so its hosted cause remains unconfirmed.
Its observation loop performed SQLite reads directly on the event loop. The
observer now reads through `asyncio.to_thread`, matching the product publisher,
and reports last durable acknowledgment counters, injected-failure/send counts
and pending await locations on timeout. The three-second limit, injected faults,
single-producer and no-repeat assertions remain unchanged. This is diagnostic
and observer correction, not proof that the hosted occurrence is resolved.

## Evidence and reproduction

[Reproduction](evidence/renewal-entry-delay-before.xml),
[diagnosed reproduction](evidence/renewal-entry-delay-diagnosed.xml),
[Windows full suite](evidence/renewal-entry-full-windows.xml),
[Linux full attempt](evidence/renewal-entry-full-linux.xml),
[Linux clean packaging follow-up](evidence/renewal-entry-linux-packaging.xml),
[Windows observer follow-up](evidence/renewal-event-observers-windows.xml),
[Linux observer follow-up](evidence/renewal-event-observers-linux.xml),
[Windows package check](evidence/renewal-entry-byte-check-windows/installed.json),
[Linux package check](evidence/renewal-entry-byte-check-linux/installed.json).

With the pinned Connector/Core wheels and declared test dependencies installed:

```powershell
python -I -m pytest -q --tb=short -o pythonpath=. tests
```

Final hosted CI, the legacy-peer coverage reconciliation, full provider/platform
acceptance and release freeze remain open.
