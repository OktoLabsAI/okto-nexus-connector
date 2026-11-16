# Completed remote setup after credential replacement

Connector 0.0.12 passed 233 installed tests, including 28 new source cases. The exact 123 source/build/wheel/install files are pinned by `a8cf18105fa1cdca0443b6a3be08972d5576f72f3ce07e5a19dd9fc7cd59ae5a`; Core 0.0.14 is unchanged. Nexus integration candidate: `5ddbbc782d9f81f1db6b257c25e0e8c1df12bc8ea586ba5a0060e74eb60c1ae7`.

Current authenticated identity and exact approved binding permit only completed setup reuse after explicit key replacement. Bootstrap/Finish history remains unchanged; pending setup, local review, missing or drifting bindings, unfinished startup and concurrent host changes remain refused. Reconnect does not apply a proposal, probe, issue a grant or submit work. Existing daemon reuse/reload remains the lifecycle path.

The source regression had 232 passes and one error-code expectation mismatch for a profile change; four overlapping CAS retests passed after correcting that expectation. The final installed suite reruns all 233 identities with zero failure/error/skip. Initial epoch failures are preserved. Full Ruff on the changed product/new test files and whitespace checks pass.

The Nexus campaign adds four installed cases with real HTTP/WebSocket/TCP IPC and DaemonApp, synthetic native/provider behavior and isolated vaults. It qualifies renewal, loss recovery, explicit rotation, stale credential/ticket refusal and historical revocation recovery; it does not qualify real TLS/two-host deployment, OS vaults, native services or provider work. No previous evidence was rewritten.

[Usage](../../connection-configuration.md), [reports](reports.json), [package proof](package-proof.json), [raw-byte manifest](artifact-manifest.json).
