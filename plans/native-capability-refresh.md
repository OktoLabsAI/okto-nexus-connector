# Native capability refresh

ApprovedToolLaunchProvider now supplies the native Pi owner with authenticated capability metadata reads. It checks approved local configuration before and after the read. RefreshingNativeActionBridge requires the original capability identity, scope, audience and actions plus the exact currently installed Core lease ID/serial. The unchanged Core ScopedNativeActionBridge remains the authority gate before and after each canonical domain call.

The renewed deadline is bounded by the Server metadata deadline and Core lease. The same protected material is retained; it is not reissued, sent to Pi or persisted in a new plaintext location. A stale context or mismatched metadata is rejected before mutation. Lost mutation responses retain the original operation identity and OUTCOME_UNKNOWN classification.

Core remains pinned to 0.2.38.dev0, wheel SHA-256 f9bc5c3002593845416187b91802dfe914fd89b04036fd699be48e81dfff321b. Direct factories without a metadata callback retain fixed-deadline behavior.

Verification: 81 selected Connector cases passed against the installed wheel outside the source tree, including invocation through ApprovedToolLaunchProvider. Canonical Server integration and both-consumer metadata mismatch/renewal tests are recorded in the Nexus repository at plans/r4_execution/test_runs_20260930_native_refresh.json. The runner verifies source/wheel/installed byte equality. The Nexus isolated environment has no Connector module or distribution.

These are technical integration campaigns, not real provider, multi-host or release acceptance. M05/M06/M09 and remaining fixed delivery-plan gates stay open.
