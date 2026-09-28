# Evidence — Core 0.2.7.dev0 alignment suite run (2026-09-26)

**Test ID:** EV-CORE027-ALIGNMENT-2026-09-26
**Status:** PASS
**Scope:** full automated suite after adopting `nexus-connector-core`
0.2.7.dev0 (C8 reaudit X01–X03).

## Environment

| Item | Value |
|---|---|
| OS | Windows 11 (win32), x86_64 |
| Python | 3.13.1 |
| Connector | okto-nexus-connector 0.1.0.dev0 (working tree, pin updated to core==0.2.7.dev0) |
| Core wheel | nexus_connector_core-0.2.7.dev0-py3-none-any.whl, SHA-256 `ffe92eb8be7b9f7416e5021403de35ad3bc60b1c5a732d5f31f2231c1ee4b180` (source HEAD `6a43e90`) |

## Connector impact assessment (C8)

Entirely Core-internal (`runtime.py` only): the shared lease classifier
compares the durable row against both the proposal and the base context
(SUPERSEDED → `STALE_GENERATION` fencing of the obsolete binding while
deny/interrupt/force and known receipts stay available), identifiable
`_ReleaseObligation` records that the next shutdown retries
idempotently, and physical close/force units owned solely by the late
handle record (peak=1 in-flight unit; proven stops are observed, never
re-forced).

`composition.py` and `models.py` are untouched; `create_runtime`'s
signature is unchanged (verified by introspection). The connector
already surfaces `STALE_GENERATION` through its typed error mapping —
the new superseded fencing changes nothing in the connector contract.

**No connector code change was required** — only the version pin.

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
| `okto_nexus_connector-0.1.0.dev0-py3-none-any.whl` | `dbdbfd8eeffed13741932a954f87071fd08d9ef07b5b18fd33b37429e3898b14` |
| `okto_nexus_connector-0.1.0.dev0.tar.gz` | `27a987042620a64541b59898a42d5dffb2c857cced5c579f9da879ac3646db92` |

## Honest limits

- External blockers unchanged: real-Server campaign (C11/J-matrix) and
  real-provider qualification remain NOT_RUN; no publication performed.
- POSIX qualification still staged (Windows-only evidence run).
