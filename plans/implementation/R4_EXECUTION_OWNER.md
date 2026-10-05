# R4 daemon operation consumers — partial M04/M06

`DaemonApp.own_r4_connection` adopts a negotiated authenticated connection and
starts `R4ExecutionOwner` with separate productive and control consumers.
It rejects duplicate ownership, an overlapping legacy transport, a disconnected
channel, draining, or exhausted connection capacity. The daemon stops consumers
and observes their producers before closing the shared Core host and journals.

Each received reservation belongs to a producer until execution and receipt
publication finish. Cancellation of a consumer or a stop waiter cannot cancel
that producer. Queues remain bounded by the connection owner; each execution
owner has at most two active producers and 64 tracked sessions. A daemon admits
at most 32 execution connections. Closed/uncertain session records are retained
until lifecycle recovery; reaching the limit refuses a new open.

Opening resolves the approved host selection using the complete candidate
inventory, checks the lane after each composition wait, installs the initial
Core lease and then prepares/opens. Managed mode and model come from the exact
validated envelope; credential references and environment come from trusted
host composition ports. The prepared operation preserves the Server operation
ID. No separate intent is resolved or admitted by the consumer.

Submit, steer, interrupt and close use typed public Core arguments, preserving
target and reason. Approval/input use the Core decision bridge; a protected
response reference requires a host resolver and an exact content digest. After
the resolver waits, lane and installed context are checked again. Core verifies
that the native request was observed before a reply is sent.

Receipt projection uses the Core's public bridge and retains the original wire
hash, scope and operation ID. A publication or execution failure fences the
connection and remains observable on the owner. It does not retry a native
effect, invent a successful receipt or create a new operation. Durable Core
facts remain the basis for the future reconciliation/publication recovery path.

## Tests and evidence

`tests/unit/test_r4_execution.py` exercises real Core journals with a synthetic
native peer: control while a productive receipt publisher is blocked, cancelled
stop waiter, invalidation during launch setup, lost receipt publication,
immutable opening data, typed controls, duplicate open, cross-binding refusal,
and one native application for both approval and protected input. The Nexus
real loopback WSS journey now uses daemon-owned consumers; the test no longer
calls Core prepare/open/control or publishes Core receipts itself.

Commands, attempts, final counts and hashes are in
`evidence/r4-execution-report.json` and the Nexus coordinated report.
Early fixtures incorrectly expected an action field in a receipt and omitted
the close budget; a zero close budget correctly yielded uncertainty. An actual
stop-observation race was fixed by explicitly consuming completed producer
results. The first WSS cleanup closed the channel before stopping its consumers,
which correctly reported a disconnect; cleanup now uses the daemon's lifecycle.

## Remaining integration

This is adoption and effect ownership, not full daemon acceptance. Automatic
R4 startup still needs persisted executor registration, credentials, reconnect,
ticket/lease renewal and explicit refresh of approved revisions. Production
profile/environment/capability composition still must supply the host ports;
the tests use an empty environment and synthetic provider qualification.

Nonempty reconciliation, durable receipt publication retry, events, native
request ingress and canonical decision UI/CLI are still pending. The two
consumers serialize work within each class; fairness and latency under blocked
control work require the M11 campaign. A global shutdown deadline and live
recovery ownership under unresolved outcomes are not qualified by this change.
No provider, Linux, separate-process/host or product gate is accepted, and
executable/readiness flags remain unchanged.
