# M3h — Consume bounded parallel Core discovery

2026-10-17. Connector 0.0.10 / feature/v0.0.10; Core pin remains 0.0.12.

Updated the vendored Core development wheel. The shared discovery facade now
observes the supported harnesses concurrently with bounded, joined readers.
Connector source and wire inventory semantics are unchanged: no partial snapshot
is published as though it were a complete observation.

**35 installed cases passed**, no failures/errors/skips. The campaign covers
executor inventory, discovery configuration and retained inventory refresh.
Byte comparison verified Core (156 files) and Connector (98 files), and
changed_inputs was empty. Evidence:
`build/runtime-lifecycle-m3h-installed/{manifest.json,results.xml}`.

Core SHA256: `e95a306637c784dedd8e07215e957cfc7f343116bf4517400c5d46bfde605917`.
Unchanged Connector wheel SHA256:
`64f0bb0567dec34d73500981f0bbab43b2a9f78ecda695bd63518127bc4dc25a`.

The Nexus canonical ledger records the corresponding pool startup correction
and remaining per-harness readiness work. This is an installed technical
checkpoint; no global deployment or independent remote-host qualification is
claimed. M3/M5 remain open.
