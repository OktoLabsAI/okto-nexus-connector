# M2e — Connector launch recovery

2026-10-16. Connector 0.0.10, feature/v0.0.10, Core pin 0.0.12.

The replacement authenticated control owner recovers interrupted launches through
Core's shared producer fence and native container adapter. It verifies the
original receipt binding and claim before containment, releases existing journal
and physical slots only after proof, and publishes any recovered FAILED receipt
before reporting readiness. Busy producers and uncertain containers keep capacity.

The `nexus-launch-recovery-v1` capability distinguishes an opening whose producer
never started from a stopped process. Original possible-effect/retry markers are
preserved. Missing initial-turn receipts may be omitted only for Server evaluation;
the Server still requires its own unsent-operation proof and a consistent next
snapshot. No native command is replayed and no close success is invented.

Negotiation retains recovery tasks after cancellation, revokes their per-attempt
authority before joining them, and keeps journal/HTTP owners alive until their
workers finish. Authority checks pin the registration, bootstrap deadline and
replacement connection identity. A late worker cannot release a slot after the
negotiation loses authority.

Installed Windows campaign: **103 passed**, including five disposable producer
crashes (intent, claim, container, spawn, birth), absent/existing slots, lost
authority, publication failure/missing ACK, cancelled negotiation, initial-turn
omission, session containment, capacity composition and publication regressions.
All installed Core/Connector package files were byte-compared with their wheels;
application source was not imported and inputs did not change during the campaign.

- Core wheel SHA256: `5ab1f03165c5dcdb7e879a6c773f766a8a6b5cd479c529588f833d47096d13c8`
- Connector wheel SHA256: `fa3e82c9e2b2359c81f4a16e39f6e7ac4b547392a5a593c56cb14f4c02bf3abe`
- Evidence: `build/runtime-lifecycle-m2e-installed/manifest.json`, `results.xml`
- JUnit SHA256: `5c43e6c6881a307174f3d8e7f79cd5cb09ff534a9100af519530c2667c2d1c52`
- Exact test list and input digest are in the manifest. The local isolated runner
  is `build/run_m2e_installed.py`; Windows directory cleanup was corrected after
  the first 103-pass run, then the complete runner passed with exit code zero.

This is technical Windows qualification with disposable processes and synthetic
native peers. Live Nexus UI results, joint installation and Server validation are
in the Nexus canonical ledger. A local Connector end-to-end provider campaign,
Linux/macOS qualification, per-entity readiness and incremental recovery remain
pending in the overall plan. Another physical host has not been qualified.
