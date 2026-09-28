# Evidence — Core 0.2.6.dev0 alignment suite run (2026-09-26)

**Test ID:** EV-CORE026-ALIGNMENT-2026-09-26
**Status:** PASS
**Scope:** full automated suite after adopting `nexus-connector-core`
0.2.6.dev0 (C7 reaudit W01–W04).

## Environment

| Item | Value |
|---|---|
| OS | Windows 11 (win32), x86_64 |
| Python | 3.13.1 |
| Connector | okto-nexus-connector 0.1.0.dev0 (working tree, pin updated to core==0.2.6.dev0) |
| Core wheel | nexus_connector_core-0.2.6.dev0-py3-none-any.whl, SHA-256 `4c0987150cfd41a9ac25a6c16ed9d555c4f4220d370e51820ed37f0cb7cca55f` (source HEAD `9f0ebab`) |

## Connector impact assessment (C7)

Entirely Core-internal: late-handle containment through the common
model (ownership registered before any await, parallel physical force,
OWNED_UNKNOWN re-containment on the next shutdown), a single lease
transition function shared by every finalizer/reconciler, containment
classification for strictly negative approval replies across all three
frontiers, and admission fencing before reading lease state on
never-existing sessions. `composition.py` and `models.py` are untouched;
`create_runtime`'s signature is unchanged (verified by introspection).

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
| `okto_nexus_connector-0.1.0.dev0-py3-none-any.whl` | `f44ee30ca4faf47a1765d6e8c4bb47d69556995dd8ba5b4bc58ea40990ad22d7` |
| `okto_nexus_connector-0.1.0.dev0.tar.gz` | `f909b6d0872ec0d69f1d5eb978a857bb6161c0d01cfacd58fc320796c8a4318b` |

## Honest limits

- External blockers unchanged: real-Server campaign (C11/J-matrix) and
  real-provider qualification remain NOT_RUN; no publication performed.
- POSIX qualification still staged (Windows-only evidence run).
