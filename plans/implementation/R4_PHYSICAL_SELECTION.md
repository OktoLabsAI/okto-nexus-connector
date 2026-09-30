# R4 approved physical selection — partial M03/M06 integration

State schema 4 adds `execution_bindings` separately from legacy bindings.
Loading schema 3 preserves its existing records and preferences; it does not
invent R4 approvals. An authenticated `APPROVED` binding response is joined
to exactly one published local realization in the same Server/executor/agent
namespace, then persisted atomically with a digest of the complete local
evidence. Replay returns the same mapping. A changed response cannot silently
replace it, and a repeated realization publication cannot downgrade `BOUND`.

`resolve_execution_selection` accepts a Core-validated `runtime.open` frame
and this host's complete candidate inventory. It checks the approved binding,
agent/workspace and revisions, realization reference/revision, candidate and
inventory revision. It verifies the persisted snapshot, canonical executable,
root identity, current fingerprint, recorded architecture and portable build
identity. Pi retains Node, the CLI script and package/dependency content.
Wire paths or arbitrary launch arguments are never used to select resources.

`CoreRuntimeHost.build_r4` freezes the opening frame and candidates before
waiting. It revalidates after journal/ledger initialization, composes through
the public Core factory and keys runtime instances by Server, executor,
binding and session. Cache reuse checks the same approved selection. The
environment callback rechecks selection before and after its own await.
Composition itself does not prepare, open or install an execution lease.
The connection owner still supplies current lane authority and a Core-installed
canonical lease before runtime effects.

## Evidence and limitations

Directed tests cover schema migration/roundtrip, approval replay, namespace and
revision mismatch, binary/root replacement, local-state drift, duplicate
bindings, stale inventory, a configuration change before publication ACK,
Pi package changes outside the CLI file and revocation during store waits.
The real loopback WSS journey now stages and publishes the physical mapping,
persists the actual binding result, and composes the host through this resolver.
It proves the approved workspace reaches Core, no open occurs before lease
installation, and the five-action cycle opens once and closes the session.

Commands, results, skips and artifact hashes are in
`evidence/r4-selection-report.json` and the Nexus coordinated report.
The initial two failures were test setup issues: a malformed candidate-ref
fixture and an uncreated private runtime directory. Corrected fixtures passed.

This is not daemon acceptance. CLI/daemon onboarding must call these services,
refresh persisted approved revisions explicitly, and connect the R4 executor
registry, dispatcher consumers, credential lifecycle and recovery. Profile /
environment preparation, nonempty reconciliation, durable errors/receipts,
events and all seven actions still require product composition. The native
peer and readiness qualification remain synthetic in this campaign.

Schema 4 makes older binaries refuse the newer state; downgrade uses a verified
backup until M12 supplies the complete migration/rollback runbook. Root/process
ownership under concurrent filesystem mutation, global resource limits,
provider/SO/independent-host qualification and the remaining gates stay open.
The Core wheel and public wire are unchanged.
