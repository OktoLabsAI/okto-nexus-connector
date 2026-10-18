# M4d4j: reconcile pending Core claims

After a durable `session.release.accepted`, the Connector now checkpoints both
the terminal event stream and the Core pending-claim projection, then marks
the release outbox complete. A crash between stores leaves the outbox row
eligible for an idempotent replay at reconnect. Schema-four migration also
revisits schema-three accepted rows whose event stream was retired before the
Core projection existed.

Both the normal reconciliation report and interrupted-launch recovery now page
only pending claims. An installed test seeded 1k, 10k and 100k Core claims with
the same three pending identities; each report contained exactly those three
in one page. The final isolated installed Core 0.0.12 and Connector 0.0.10
wheels passed 23 Core session-claim tests, 53 focused Connector tests and 65
adjacent Connector capacity/control/containment tests. Two adjacent test
fixtures were updated to provide the required recovery authority and expect
the already-current schema 15; no product behavior changed for those cases. Package
imports resolved to site-packages. This technical checkpoint used no Nexus UI;
the plan reserves that for major gates.

M4 remains open. Releases inferred only from an accepted reconciliation cycle
do not yet retire their event and Core pending projections. Accepted release
backfill from large pre-existing histories is still bounded to 4096 per
reconnect attempt, rather than running as resumable background maintenance.
The physical directory audit remains an admission prerequisite.
