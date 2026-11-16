# R3 selected user-task enablement

`service enable/disable` now mutates only the owned Windows task's enabled state.
It preserves active runtime/control state, validates the selected definition and
persists intent before the OS effect. The same command can reconcile a lost
response if the current export differs only in the requested enabled value.
An opposite request or unrelated external edit leaves the intent pending.
The scheduled entry checks disabled state before runtime bootstrap.

Disabling an unavailable saved environment is supported; enabling requires a
valid context. Status shows enabled login policy separately from process state.
No implicit drain/start, detached fallback, forced stop or new runtime authority
was added. General definition reconciliation, upgrades and native campaigns
remain open in the fixed R3 plan.

62 focused Connector tests passed. 90 coordinated tests passed in a fresh venv
with all three candidate wheels, outside the checkout, including both consumers'
real concurrent detached lifecycle. Native tests only validate XML/read absence;
mutation tests use fake task backends. Candidate Connector has 109 Python
payloads matching source; Core has 130 and remains pinned at 0.0.14.
Detailed evidence: Nexus `plans/agent_ops/evidence/pr02-enablement.md`.
