# M4d3 — Released pre-open capacity dependency qualification

The vendored Core 0.0.12 artifact is synchronized with the Server's no-stream
retirement work. Core exposes an exact, read-only association for already
released provisional capacity. Connector execution and transport code are
unchanged; its occupied-slot reconciliation keeps the existing contract.

Installed Windows qualification covers 70 distinct capacity, wait/renewal,
containment and launch-recovery cases. The initial run passed 68 and failed two:
the renewal-gate test exceeded two seconds, and the disposable crash fixture's
interpreter exceeded its 30-second startup deadline. After the concurrent wheel
build finished, both unchanged cases passed in 15.44 seconds with the same
installed artifacts. This is combined qualification, not one all-green 70-case
run. No product fix for those timing failures is claimed.

Both campaigns verified installed package bytes and recorded unchanged source
and test inputs. Evidence paths retain the original working label `m4d2`:

- `build/runtime-lifecycle-m4d2-installed/{manifest.json,results.xml}`
- `build/runtime-lifecycle-m4d2-installed-retry/{manifest.json,results.xml}`

Core SHA256: `fffcfad6a82a67fb4b9cc4b25b0718a3c3e30bf119dde71ee07dcb062b814898`.
Unchanged Connector SHA256:
`41331e7b9405e830d990141e48e7702d461559a779b66ee39ec8aedc9e52d7c2`.

No global rollout or physical remote-host test occurred. M4/M5 and the remaining
real UI gates stay open in the canonical Nexus ledger.
