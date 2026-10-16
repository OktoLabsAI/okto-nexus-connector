# M3e — Isolate local lease expiry to its opened session

2026-10-16. Connector 0.0.10 / feature/v0.0.10; Core pin 0.0.12.

Core expiry/revalidation failures during renewal now fence an already-opened
session before its queued productive work wakes. The retained containment task
stops only that session, retries incomplete resource cleanup and negotiates its
release. Siblings continue on the shared connection. Queued work gets a durable
no-effect refusal through Core; it is never forwarded to the fenced harness.
Publication recovery resumes the association/receipt without native replay.

Validation: **71 installed cases passed**, no errors/failures/skips; all Core
(156) and Connector (98) files compared with wheels; changed_inputs=[]. Tests
cover isolated expiry/revalidation/denial, failed containment retry, sibling
execution, durable release and recovery at association/admission/terminal/
publication cuts. An initial source expectation was corrected to accept the
actual late-grant code; a second test fixture was corrected to signal renewal
entry only for the target session (it had signaled on a sibling). Final source
renewal/containment/refusal suite: 32 passed. Full installed campaign: 71 passed.

Core SHA256: `d658de647db7e9dec106ce496525703d9284492ded401cb3ad55c901e460e8cf`.
Connector SHA256: `64f0bb0567dec34d73500981f0bbab43b2a9f78ecda695bd63518127bc4dc25a`.
Evidence: `build/runtime-lifecycle-m3e-installed/manifest.json`, `results.xml`.

The scope is already-opened remote sessions. Initial preparation/opening without
an opening fact still uses conservative host reconciliation. Transport failures
and uncorrelated authority failures retain global handling. This checkpoint does
not complete M3/M5 and is not a live global rollout. The running Nexus remains
on the M3c installation qualified through the UI.
