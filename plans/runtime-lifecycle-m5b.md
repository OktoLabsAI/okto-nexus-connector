# M5b — Scoped native preparation refusal

Status: selected installed qualification passed; M5 remains in progress.

A native preparation failure used to escape the opening producer and close the
Connector's entire control connection. The R4 owner now stops and joins that
session's renewal, contains its runtime and native action owner, confirms that
Core never claimed a native session, and records a scoped startup refusal. It
releases the exact consumed capacity reservation before persisting and
publishing the terminal Core receipt. An unproved release or existing native
claim continues to require reconciliation. The normal publication outbox
retains the association/receipt across a lost reply or restart.

The new unit reproduction checks Pi, Claude and Codex selections with an
already healthy sibling. A separate Connector process injected a version-probe
failure, returned `PREPARATION_FAILED`, kept control ready, and then opened a
second session for a new handoff without restarting. The corresponding Nexus
session closed, the first delivery became FAILED_FINAL, and its capacity
reservation was RELEASED. See Nexus `plans/runtime_lifecycle/m5b-startup-refusal.md`.

Installed validation passed 54 selected Connector cases against the exact
Core/Connector wheels. Core 161 and Connector 99 installed package files matched
those wheels, with no changed campaign inputs. Nexus passed 17 installed
integration/capacity/recovery cases. Hashes and JUnit results are in
`runtime-lifecycle-m5b-results.json`. The global installation is unchanged.

Remaining M5 work includes failures before lease installation, uncertain native
admission, storage failures during containment/release/publication, and the
full differential reconnect/revocation/reset and real UI gates.
