# Audit A10: current R4 environment positive control

2026-10-02. The old audit test 27 used the retired RuntimeManager startup path
and skipped on BINDING_NOT_AUTHORIZED. That skip was not acceptance evidence.

The same test now uses an acknowledged R4 selection, daemon execution owner,
session capability issuance, real Core preparation and the production environment
renderer. It requires exactly one MCP token with the issued capability value,
the prepared capability reference, the approved provider credential, and a
secret-free generated Codex configuration referencing that token environment name.
Neither the persisted state/config nor the environment may contain the agent key.
Contract refusals fail rather than skip; lifecycle cleanup is fixture-owned.

The native peer and network authority are synthetic. No actual provider process,
independent host, or production credential is exercised by this test.

Installed Connector c110246 application bytes / Core 0.2.53.dev0:

- Windows Python 3.13.1: 70 passed, 14.02 s.
- WSL Linux Python 3.12.13: 70 passed, 15.04 s.
- Cases: the complete reconstructed audit, R4 daemon execution and session
  capability suites; neither run skipped a case.
- Invocation: `python -I -m pytest -q -o pythonpath=.` with those three files.
  Repository root exposes test helpers; application imports remain installed.
- Evidence: `evidence/audit27-r4-regression-windows.xml` and
  `evidence/audit27-r4-regression-linux.xml`.

Only tests/documentation changed. Historical full-suite counts and skips remain
historical; this directed run is not a new full-suite or final release pass.

GitHub issues were reread: issue #1 remains the sole open issue. The documented
unsupported macOS status and Linux guest workaround do not qualify a native
macOS containment backend. No issue was closed or commented on.
