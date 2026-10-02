# Human-readable discovery and issue #1 follow-up

`discover` now defaults to a three-column table: harness, number of installations
found, and technical status. Core's catalog/assessment remains the source of
families and readiness; there is no new discovery scan or provider execution.
Distinct installations count separately. Different statuses within one family
show mixed readiness rather than promoting the whole family to ready.
Unsupported containment is shown even when the primary Core state is NOT_PROBED.

`discover --verbose` retains full paths, fingerprints, selection references,
versions and diagnostic reasons. `--json discover` retains the complete structured
result with or without `--verbose`. The table is rendered only at the human CLI
boundary; the discovery service and machine-readable inventory are unchanged.
The previous connect-time selection note now describes explicit local selection.

## Verification

- Installed Windows Python 3.13.1: 28 directed CLI/discovery/configuration/doctor
  checks passed. WSL Linux Python 3.12.13: the same 28 passed.
- Actual installed CLI outside the checkout found Codex, Pi and Claude, each with
  count 1. Codex/Claude showed `Needs selection and probe`; Pi showed
  `Needs local preparation`. No provider was launched.
- All 76 installed Connector package files match the wheel. SHA-256:
  `5572e7fdaf8e36e17dc64a9b9e71f9c27d59b2762c812c5ebc2d8eab7e8df44b`.
  Core remains .55, SHA-256
  `b1a389d247a470571c485e27adfa7277b11b4ae39382a52ebefc0ef0cc76453f`.
- Operational documentation audit: 37 command examples and 12 local links passed
  against the installed parser/help. Evidence is `evidence/discover-summary-*`.
- The initial source run retained 12 passes and one failure: the renderer used
  the raw Core projection key instead of Connector's `availability.rows` wrapper.
  The corrected installed campaigns exercise the actual empty-inventory wrapper.

These are directed development checks, not full release or native-provider
acceptance. The user's global environment was not replaced by these tests.

## Updated macOS issue

[Issue #1](https://github.com/OktoLabsAI/okto-nexus-connector/issues/1), updated
2026-10-02T11:46:37Z, reports a native macOS retest of Connector `7cf54e1` and
Core .55 at `10585bb`: doctor now reports 10 ok, 3 warn, 0 fail and 1 unsupported.
Claude is passively discovered as untrusted; Codex and Pi are still not detected.
This is user-reported macOS evidence, not a macOS campaign run by this agent.

The README already contained a platform support table. Its opening now highlights
the Windows/Linux executor requirement, corrects the stale Core .53 pin to .55,
and links the unresolved macOS design question. Native macOS containment remains
unimplemented; no future support decision is inferred and the issue stays open.
The missing Codex/Pi discovery on that host is a separate investigation: lack of
containment does not prove lack of installed providers. The current automatic
wrapper-layout support is documented for Windows, not claimed as macOS coverage.
