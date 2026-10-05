# R4 session capability consumer — 2026-09-30

Partial CON-R4-03 / coordinated M02, M06 and M09. The shared Core remains
`0.2.28.dev0`, SHA-256
`27df75100dea033ca5456f2d571eb41b6311fa3ce530a723ecd6c606d257953c`.
No readiness flag or product gate is promoted.

## Implemented

`NexusHTTPClient.request_r4_session_capability` freezes and validates the
canonical opening frame through Core, then sends the request identity,
binding, audience and requested action ceiling to the public session route.
The caller must persist that request identity before sending it.

The response must match the full opening scope with exact field types, the
requested audience/actions, the capability reference and the approved MCP
URL. TTL is bounded to 1–120 seconds. A local deadline starts before HTTP,
including a safety margin; a late response is rejected. There is no automatic
replacement or retry after an uncertain response.

The secret is excluded from the DTO representation. Connector diagnostic
redaction now recognizes `nxc4_` session credentials and `nxt4_` tickets.
`CapabilityMaterialUnavailable` preserves only the capability ID and the
Server's recovery eligibility, allowing a future durable host workflow to
revalidate a replacement instead of inventing a new execution intent.

## Evidence and limits

[The report](evidence/r4-session-capability-report.json) records a complete
source regression: **343 passes and two existing skips**. The initial helper
import collection failure and the subsequent 17-case response campaign are
preserved. Counts overlap. The coordinated Nexus campaign additionally
exercises this client against the real HTTP route and immutable wheels;
its authoritative final report is
`okto_labs_okto_nexus/plans/r4_execution/test_runs_20260930_capabilities.json`.

This port does not complete the daemon launch provider. Persisted issuance
intent, protected local material, approved launch configuration, automatic
lane adoption/renewal, nonempty recovery and the actual MCP/native use-case
guards remain required. A reserved credential alone does not authorize a
tool call. Synthetic fixtures do not qualify a provider or multi-host release.
