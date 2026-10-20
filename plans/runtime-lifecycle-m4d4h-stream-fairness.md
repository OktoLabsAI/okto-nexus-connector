# M4d4h: fair event replay across Connector streams

The Connector's pre-ready replay drained one Core stream completely before
visiting the next. A stream that produced more events during replay could keep
another stream waiting. The recovery worker now discovers the pending streams
under the existing bounded scan and processes one finite event page per stream
per round. A stream with more work returns to the back of the queue.

A per-stream failure while publishing or applying its ACK is retained as the
attempt's error, but other streams may make durable progress before the worker
retries. The failed stream does not advance its Core ACK or replay cursor. The
current authority is checked before and after each step. The existing 4096-page
attempt budget and strict failure result remain in place.

Validation: 50 adjacent tests passed against Connector source and again against
the freshly built, isolated installed 0.0.10 wheel with Core 0.0.12. New tests
show that a hot first stream yields to a second before its next page, and that
an injected Core ACK failure leaves the first stream pending while the second
advances. Retrying applies the first stream's retained Server ACK without
resending its event. No live Nexus/Connector instance or real harness was used
for this technical checkpoint; the planned major UI gates remain separate.

The scan still starts at row zero and examines at most 4096 registered streams
per recovery attempt. The event store itself currently rejects a 4097th stream,
so a larger scan cannot occur without changing its capacity and retention.
Lifecycle-safe retirement of completed streams, or a larger durable store with
a resumable scan cursor, is required for the M4 historical-scale acceptance.
