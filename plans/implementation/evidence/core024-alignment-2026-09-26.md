# Evidence — Core 0.2.4.dev0 alignment suite run (2026-09-26)

**Test ID:** EV-CORE024-ALIGNMENT-2026-09-26
**Status:** PASS
**Scope:** full automated suite after adopting `nexus-connector-core`
0.2.4.dev0 (C5 reaudit U01–U07).

## Environment

| Item | Value |
|---|---|
| OS | Windows 11 (win32), x86_64 |
| Python | 3.13.1 |
| Connector | okto-nexus-connector 0.1.0.dev0 (working tree, pin updated to core==0.2.4.dev0) |
| Core wheel | nexus_connector_core-0.2.4.dev0-py3-none-any.whl, SHA-256 `d1a4d004c35605387d8c01d6b04c2fb5900c6277687966df812078d8265cdb65` (source HEAD `df3baaf`) |

## Connector impact assessment (C5)

The C5 campaign is entirely Core-internal: `composition.py` and
`models.py` are untouched and `create_runtime`'s signature is
byte-identical (verified by introspection). The findings harden the
effect frontiers inside the Core:

- U01/U02: thread-scoped DispatchGuards now also cover approval replies
  and every native creator re-validates deadline/drain after its own
  waits.
- U03/U04/U06: the open producer becomes a shielded OpeningAttempt task
  (late handles are contained, never abandoned); the scheduled force is
  a waiting worker on a wake event with minimum-deadline tightening; CAS
  exceptions reconcile against the durable lease record.
- U05: `launch_artifact_signature` (module-level, factory-internal)
  seals Node+CLI+dependency closure at every frontier — an ordinary CLI
  edit after prepare is `PROFILE_DRIFT` before spawn, strengthening the
  connector's binding revalidation from inside the Core.
- U07: incremental bounded directory enumeration.

**No connector code change was required** — only the version pin. All
connector flows (managed launches, approvals, leases, shutdown) exercise
the new frontiers through the public API and remain green.

## Command

```
python -m pytest tests/ -q --tb=no
```

## Result

```
115 passed, 1 skipped (layered: unit 55+1s, contract 35,
integration 20, e2e 5)
```

## Built artifacts (this evidence run)

| Artifact | SHA-256 |
|---|---|
| `okto_nexus_connector-0.1.0.dev0-py3-none-any.whl` | `2edb196fb045691c930624ac507a97a5f1032be645a6a1b89ed61294fa58d209` |
| `okto_nexus_connector-0.1.0.dev0.tar.gz` | `78adbba9b0aefe59aea61731c4aa25cb1e62eb3a26c73b8c2aa2df956811d5ef` |

## Honest limits

- External blockers unchanged: real-Server campaign (C11/J-matrix) and
  real-provider qualification remain NOT_RUN; no publication performed.
- POSIX qualification still staged (Windows-only evidence run).
