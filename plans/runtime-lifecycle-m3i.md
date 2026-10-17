# M3i — Partial inventory selection foundation

2026-10-17, Connector 0.0.10, branch `feature/v0.0.10`.

Publication and exact-installation selection accept the same format-3
observation coverage. A partial batch with another harness PENDING cannot be
reconstructed as a full scan that claims ABSENT; that produces a different
revision and selection is refused. The full-scan default remains usable until
the independent daemon workers are integrated.

The vendored Core 0.0.12 is the qualified M3i artifact:
`13d72e63d1406a8cd6fcb154ce31bb218c0e042cce0bf946cc8e0350f69a3b9a`.
Connector wheel:
`cc7c9151d13d4115be38fd0d2c3fd728af5b109af459121b44b5533c349ca514`.

**36 installed tests passed** in 4.04 seconds: executor inventory, discovery
configuration and inventory refresh. The isolated interpreter verified all
158 Core and 98 Connector package files against these wheels. No source/test
inputs changed during the campaign; dependency validation passed. Reports:
`build/runtime-lifecycle-m3i-installed/{manifest.json,results.xml}`.
The initial isolated Core environment lacked the Connector dependency
`websockets`; after installing declared compatible version 17.2, collection and
all tests passed. No global environment was modified.

Pending: daemon-owned independent observations/publication, refresh frontiers,
exact published-batch consumption by onboarding, coordinated Server rollout,
joint installed execution tests and the major UI gate. This foundation does
not complete M3/M5 or qualify an independent physical remote host.
