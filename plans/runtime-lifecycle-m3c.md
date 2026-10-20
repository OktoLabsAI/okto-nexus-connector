# M3c — Consume original-owner launch recovery

2026-10-16. Connector 0.0.10 / feature/v0.0.10; Core pin 0.0.12.

Vendored Core now contains failed managed launches under the original runtime
owner even when startup returned no native handle. Connector consumes the shared
implementation through its existing runtime composition; no second containment
algorithm was introduced.

Installed campaign: **103 passed**, no failures/errors/skips, exit 0; package
files byte-compared with the wheels and source/test inputs unchanged. It covers
launch recovery/lifetime, daemon control, WSS contract, session containment,
capacity and publication. Additional source renewal/containment cases: 18 passed.

An initial installed run had 102 passes and one isolation-test failure. The
scoped-denial fixture held *both* sessions' lease requests while advancing its
shared logical clock, injecting an unrelated late grant into the healthy sibling.
It now holds only the target scope. The complete installed campaign then passed.
This is a correction to the scenario, not evidence that all remote lease failures
are isolated; general renewal failure isolation remains in M3/M5 qualification.

- Core wheel SHA256: `a24e21028e997cc5db3f1a55b179dd8e01d410fb7483ead9cb7350e1ca170398`
- Connector wheel unchanged: `fa3e82c9e2b2359c81f4a16e39f6e7ac4b547392a5a593c56cb14f4c02bf3abe`
- Evidence: `build/runtime-lifecycle-m3c-final-installed/manifest.json`, `results.xml`.
- Manifest SHA256: `19cba7be936db700ecea5d8de707e75c257c9730e84283a67175a822679e751b`
- JUnit SHA256: `2f422ac046cba40b2cdf860054afd60a34f397fb901b83260b5159de824310ed`
- Compared installed files: 156 Core, 98 Connector.

Windows technical qualification with synthetic peers/disposable processes;
real provider UI results and joint rollout are recorded in the Nexus ledger.
No other physical host was qualified, and this checkpoint does not complete M3/M5.
