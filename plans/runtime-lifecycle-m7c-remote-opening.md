# M7c — Remove redundant Pi build reads during remote opening

Status: installed-wheel and one real Nexus UI comparison passed on Windows, 2026-10-18 UTC. M7 and UI-B remain open.

The durable Nexus receipts in `../okto_labs_okto_nexus/plans/runtime_lifecycle/m7c-remote-open-receipts.md` placed roughly 94–105 seconds before the first `runtime.open` receipt but could not identify which step was slow. A disposable daemon tracer around the **installed** Connector/Core entry points measured one additional Pi opening through the Nexus Meta-harness. It logged only UTC/monotonic time, phase name, process/thread and call number; it did not log message text, credentials, argv or paths. The original installed Connector wheel was `beb7302c34049de36dcd36a06a508fcb3b05c2d7a27149f70e3a68e7dd55d8e6` (SHA-256), Core 0.0.12 wheel `55b58a13f7112e63a44a08efb787ca5e43128f4d090fd639a7a9b307e5e43ecd`, and Nexus 0.3.1 wheel `b98b88e9f37bb2e16e48cc7a3a7c65532826b5385bb12da263ae0ec40ee50062`.

The tracer found ten full Pi dependency-closure hashes in one admitted open. Five finished before `Core.prepare`, one ran at `Core.open`, and four ran during the native factory/environment. They consumed 122.957 seconds cumulatively of a 146.316-second R4 open. `Core.prepare` took 0.682 seconds and `PiRpcConnector.start` 2.397 seconds. This establishes redundant identity work as the dominant measured cost in this sample; it does not attribute unrelated prior delays or a population p95.

The Connector now performs scope/consent/root/entrypoint selection revalidation without rereading the entire Pi closure at four intermediate waits: after the capability request, after runtime-composition store waits, before the approved launch environment materializes, and before the host's environment callback. The full initial qualification remains. Core still performs its own full build check at admitted `runtime.open`, and full checks remain after environment materialization at the launch configuration and host layers. Changed helper bytes during the environment callback are refused by the post-callback full check before native spawn; the focused test covers a Pi dependency that the Node/CLI entrypoint fingerprint cannot see. No identity cache persists across operations or process restarts.

The new Connector 0.0.10 wheel SHA-256 is `b839bf1639cd42e9b2f76dc07a6a04fa29efcc17e43fe34ce151f135600c2a29`, with the **same** Core wheel as above. Its imports were verified from the isolated venv's `site-packages`. The selected source and installed-wheel campaigns each passed 102 cases (two existing schema-14 expectations were excluded because the current source state schema is 13; those failures also occur before this patch). The focused material-change and shutdown-during-composition cases passed from source. A real private Pi message sent through the Nexus UI returned `M7C_AFTER_OPTIMIZATION_OK` with ACK 1/1 using the new installed Connector.

| Single real UI sample | Original wheel | New wheel |
| --- | ---: | ---: |
| Full Pi closure hashes | 10 | 6 |
| Sum of full hash durations | 122.957 s | 46.706 s |
| First R4 open duration | 146.316 s | 61.994 s |
| Core prepare | 0.682 s | 0.623 s |
| Pi adapter start | 2.397 s | 1.416 s |

The comparison used the same local Pi 0.87.1 installation, disposable Nexus home/workspace, remote-agent binding, Core wheel and machine. Both calls created new `per_sender` sessions after a normal shutdown/restart; neither reused a warm pool. Hardware load and filesystem cache were not held constant, so the time difference alone is not a performance distribution. The lower hash count is directly attributable to the code path change; the remaining six hashes and other startup work still require optimization and load qualification.

Both test daemons, Nexus instances and browsers were closed normally. The disposable Nexus registered `DRAINED` with no error codes and finalized its lifespan; port 8204 has no listener. The global Nexus installation was not started or changed.
