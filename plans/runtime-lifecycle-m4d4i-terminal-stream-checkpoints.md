# M4d4i: terminal Connector event checkpoints

The event store previously counted every registered stream against its 4096-row
limit, including sessions whose release had already been accepted. After 4096
sessions, a healthy Connector could no longer register a new stream.

An accepted `session.release` now checkpoints the final remote and Core event
ACKs before removing the stream from the active replay table. The checkpoint
retains the exact scope and release proof, so historical reconciliation can
still read the final ACK and the same stream cannot be registered again. The
release outbox marks the event checkpoint separately. If the process stops
between the two SQLite commits, reconnect repeats the idempotent checkpoint.
Only a Server-accepted release with equal final ACKs qualifies. The live-stream
limit remains 4096; terminal records have a 512 MiB physical store ceiling.

Validation: 71 focused and adjacent tests passed against the freshly built,
isolated installed Connector 0.0.10 wheel with Core 0.0.12. They include the
4096-active-stream boundary and subsequent admission, exact ACK preservation,
idempotence, an interrupted outbox marker, reconnect retirement, and migration
from an old release-outbox schema. Import resolution confirmed site-packages,
not the source tree. No Nexus UI run was made for this storage checkpoint;
the next major UI gate remains separate.

This does not complete M4: terminal rows and Core historical claims still grow,
Core reconciliation still scans historical claims, and releases resolved by a
reconciliation proof rather than a direct Server acceptance do not yet retire
their event stream. The physical directory audit remains on admission. The
M4 acceptance requires measured 1k/10k/100k history with work proportional to
pending sessions and the existing unknown-file fence preserved.
