# M4d4k: retire releases accepted through reconciliation

A replacement connection can prove an old release in an accepted reconciliation
cycle without receiving `session.release.accepted` on the original channel.
The outbox keeps the original request and the separate authenticated recovery
proof. It now pages either kind of resolved release for terminal checkpointing.
For a reconciliation proof, the final sequence comes from its accepted stream
watermark, and the proof digest comes from the retained recovery bytes. Invalid
or incomplete reconciliation evidence does not enter this page.

After the accepted reconciliation proof is durably stored, an independent task
checkpoints the event stream and Core claim, then marks the outbox. The task
does not hold the Ready transition. A later accepted cycle requests another
pass if cleanup is already running. Reconnect repeats interrupted checkpoints
idempotently before ordinary event replay. Schema 5 rebuilds the pending index
so schema 4 outboxes can find reconciliation proofs as well as direct ACKs.

The final isolated installed Connector 0.0.10 wheel with Core 0.0.12 passed 56
focused and 57 adjacent tests. Tests cover a deliberately held background
checkpoint while the accepted call returns, a repeated accepted cycle, exact
final ACK retention, reconnect repair, schema migration, and eleven rejected
reconciliation-proof variants. Imports resolved to site-packages. This was a
technical checkpoint, without a Nexus UI run.

M4 remains open: an existing history with more than 4096 resolved outbox rows
still exceeds the pre-ready checkpoint pass. Historical backfill must be
resumable outside readiness, and the major real UI gate remains pending. The
physical directory audit retains its admission fence.
