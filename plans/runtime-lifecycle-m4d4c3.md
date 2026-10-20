# M4d4c3 — Connector physical ledger binding

Status: selected source and installed validation passed. This is a technical checkpoint;
the complete Core audit frontier and major real Nexus UI gate remain pending.

The Connector daemon now binds Core's physical owned-slot ledger identity to its
independent `state.json` snapshot (schema 14). Before advertising IPC readiness
or connecting to a Server, it opens the ledger and verifies that the persisted
identity still matches. The host also validates every lazy ledger opening. A
missing previously bound ledger, a retained shared journal or orphaned SQLite
sidecar prevents creation of a replacement empty ledger. A different ledger
identity is rejected; an opened ledger is closed on rejection. A first fresh
ledger is durably bound before the host returns it for capacity or native work.

The binding is held in the Connector's atomic, locked state adapter, separate
from Core's SQLite file. A full-snapshot `StateStore.save()` cannot erase or
replace an existing identity. Retaining the same state and ledger across a
directory restore remains allowed. This does not replace registration, claim,
receipt or native release evidence. Core's journal-location registry and the
host's normal reconciliation remain required.

Source qualification: 200 selected storage, host, schema and control tests
passed before the last full-snapshot guard and final synchronization change;
8 focused source follow-up cases passed after those edits. The 32-case final
installed campaign passed (`build/m4d4c3-installed.xml`) with Connector
`0.0.10`, Nexus `0.3.1`, and Core `0.0.12` wheels in one disposable Python
environment. All installed package files matched their wheels byte for byte:
Connector 101 files, SHA-256
`a687bebfaac5b9e7d205725f537a15534cd9f790bfc981441243910b9b17a18a`;
Nexus 573 files, SHA-256
`727d5885a8590019ec0eb068e8059ab25937d276479cd66c0b34923bb9776fa1`;
Core 162 files, SHA-256
`1d5bdb7a510ad0ad1b8767a156ed3d8c7a042425809227217c0dd46d1eb294ae`.
The global Connector/Nexus installation was not changed.

Remaining: the first verified physical-store audit frontier, partial-restore
and crash matrix, combined metadata budget, background scan strategy, real
Connector process restart and the major Nexus UI campaign. No production
rollout or remote-host qualification is claimed.
