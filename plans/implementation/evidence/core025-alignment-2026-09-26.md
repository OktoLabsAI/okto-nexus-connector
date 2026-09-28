# Evidence — Core 0.2.5.dev0 alignment suite run (2026-09-26)

**Test ID:** EV-CORE025-ALIGNMENT-2026-09-26
**Status:** PASS
**Scope:** full automated suite after adopting `nexus-connector-core`
0.2.5.dev0 (C6 reaudit V01–V03 + M01).

## Environment

| Item | Value |
|---|---|
| OS | Windows 11 (win32), x86_64 |
| Python | 3.13.1 |
| Connector | okto-nexus-connector 0.1.0.dev0 (working tree, pin updated to core==0.2.5.dev0) |
| Core wheel | nexus_connector_core-0.2.5.dev0-py3-none-any.whl, SHA-256 `82c97d1e29936d673f1d32b17cdbe34565e0928e4bbe76589b9dec0e11967bfa` (source HEAD `12dae55`) |

## Connector impact assessment (C6)

Entirely Core-internal: one lease-update state machine with a
productive-grant barrier (`LEASE_UPDATE_PENDING`, additive stable
retry-safe code surfaced transparently through the connector's typed
error mapping), opening ownership surviving its waiter with a supervised
late-handle registry, two-phase approval reservation in the Codex
adapter, and an enumeration budget boundary (M01).

`composition.py`, `models.py`, discovery and journal are untouched;
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
| `okto_nexus_connector-0.1.0.dev0-py3-none-any.whl` | `b0abff1ca8e1dc1141d23184609966d94fc92c425a87d0d4ee41bec1a8e785c6` |
| `okto_nexus_connector-0.1.0.dev0.tar.gz` | `8fb324743345cad0a0cf0671e39864c811d9a11bee0ebcd2510d98057de2f85b` |

## Honest limits

- External blockers unchanged: real-Server campaign (C11/J-matrix) and
  real-provider qualification remain NOT_RUN; no publication performed.
- POSIX qualification still staged (Windows-only evidence run).
