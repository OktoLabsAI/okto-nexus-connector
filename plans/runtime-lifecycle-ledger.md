# Runtime lifecycle development ledger

Version: **0.0.10**. Branch: **feature/v0.0.10**. Core pin: **0.0.12**.

Canonical plan: Nexus repository, `plans/runtime_lifecycle/plan.md` on
`feature/v0.3.1`. Scope: shared lifecycle recovery, transport fairness and the
local Connector qualification before a separate physical host is available.

## M0 — Baseline validated

Preserve the existing artifact transfer and replay implementation, including
capability negotiation, durable event reference publication and chunk recovery.
Existing installed campaign: 41 Connector cases passed; the three-package
integration campaign totaled 106 cases.

2026-10-16 checkpoint: package and vendored Core metadata agree with the pins.
Diff whitespace checks and credential-pattern scan passed. Published in the
baseline commit on this branch; the canonical ledger records its hash next.

The new Core lifecycle behavior must be tested from an installed wheel and the
vendored Core rebuilt when integrated. Loopback qualification is not evidence
of behavior on another physical machine. No new lifecycle milestone is installed.

## M1 — Consume early process-birth recording (validated)

- Rebuilt and synchronized vendored Core 0.0.12 from Core commit `8520deb`.
  Dependency pin remains 0.0.12. Wheel SHA256:
  `d2799bb6249c28fca5e56d9e17862ca1b3f72b15a59c5f9e2d15d509a0d2da8f`.
- Installed-package execution and daemon regressions: **43 passed** with the new
  Core wheel, using the real Core journal and synthetic native peers.
  Tests: `tests/unit/test_r4_execution.py`, `tests/unit/test_r4_daemon_execution.py`.
  Local JUnit evidence: `build/runtime-lifecycle-m1-installed.xml`.
- Core's opening callback is used through the existing host composition; no
  duplicate process-birth implementation was introduced in Connector.
- This is an isolated installation qualification. The user's running installation
  was not updated, and no second physical host was qualified.
- Breaking changes are authorized for subsequent shared contracts; all three
  components may upgrade together instead of retaining old protocol paths.

Remaining: durable launch intent/fencing (M2), independent readiness (M3),
incremental recovery (M4) and shared local/remote policies (M5).

## M2e — Shared launch recovery (validated checkpoint)

Connector now consumes the Core producer/container recovery proof and publishes
never-started openings through the negotiated extension. Cancelled negotiation
retains native recovery work while revoking the old attempt's authority.
**103 installed tests passed**, including five real disposable Windows crash
boundaries. Vendored Core is synchronized to M2e (`90b4ef6`); pin remains 0.0.12.
See [`runtime-lifecycle-m2e.md`](runtime-lifecycle-m2e.md) for artifacts and scope.
This checkpoint does not complete M2/M5 or qualify another physical host.

## M3c — Shared original-owner containment (validated checkpoint)

Vendored Core 0.0.12 now recovers failed planned launches without requiring an
owner restart. **103 installed consumer tests passed**, with package byte checks
and stable inputs. A scoped-denial test fixture now delays only its intended
session. See [`runtime-lifecycle-m3c.md`](runtime-lifecycle-m3c.md) for the initial
failure, final qualification and remaining renewal-isolation scope.
