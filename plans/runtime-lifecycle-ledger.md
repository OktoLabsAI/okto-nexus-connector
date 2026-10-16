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
