# Evidence — Core 0.2.8.dev0 alignment suite run (2026-09-26)

**Test ID:** EV-CORE028-ALIGNMENT-2026-09-26
**Status:** PASS
**Scope:** full automated suite after adopting `nexus-connector-core`
0.2.8.dev0 (C9: public runtime catalog + recovery fixes Y01/Y02).

## Environment

| Item | Value |
|---|---|
| OS | Windows 11 (win32), x86_64 |
| Python | 3.13.1 |
| Connector | okto-nexus-connector 0.1.0.dev0 (working tree, pin updated to core==0.2.8.dev0) |
| Core wheel | nexus_connector_core-0.2.8.dev0-py3-none-any.whl, SHA-256 `6f4823f348732801cbe93318efbb2efd339e066f268dfd617d378adc9de0e23a` (source HEAD `c5bd955`) |

## Connector changes for the C9 catalog contract

C9/C01 hands the **public runtime catalog** to both host applications
("enumerate via `get_runtime_catalog`, no arrays, no private imports").
The connector adopted it:

1. `services/discovery_service.py` no longer keeps adapter arrays:
   `ADAPTERS`/`ADAPTER_LABELS`/`_SHIM_NAMES` were replaced by
   catalog-derived `adapter_ids()`, `display_name()`, `known_adapter()`
   and `catalog_runtimes(discoverable_only=...)`. The only local table
   left is npm-command naming keyed by the catalog's `native_kind`
   (an npm packaging detail: `claude_code` ships as the `claude`
   command) — adapter knowledge itself comes from the catalog.
2. `discover_inventory(adapter_ids=None)` defaults to the catalog's
   discoverable managed set (matching the Core's new
   `DiscoveryRequest(adapter_ids=None)` semantics).
3. `connect --harness` validates against the catalog;
   `claude_attach` is surfaced as `registered_unqualified` and never
   enters managed flows merely by existing (tested).
4. `mcp_config_service`'s HTTP/native-bridge classification stays
   connector-owned: it describes harness MCP *client* capability, which
   the adapter catalog deliberately does not model.

Y01/Y02 (force retries not gated by a stalled observer; bounded public
shutdown around pending durable releases) are Core-internal and apply
automatically.

## Command

```
python -m pytest tests/ -q --tb=no
```

## Result

```
122 passed, 1 skipped (layered: unit 62+1s, contract 35,
integration 20, e2e 5)
```

## Built artifacts (this evidence run)

| Artifact | SHA-256 |
|---|---|
| `okto_nexus_connector-0.1.0.dev0-py3-none-any.whl` | `0ab8b1e6de6a43df6f84ae75b53af08a0513b9269c88d72693a4da703f368f92` |
| `okto_nexus_connector-0.1.0.dev0.tar.gz` | `e6700a2d3b777a0cb5c38900765f9f7e2bf36a75d231d3b813ff573fddc3023f` |

## Honest limits

- External blockers unchanged: real-Server campaign (C11/J-matrix) and
  real-provider qualification remain NOT_RUN; no publication performed.
- POSIX qualification still staged (Windows-only evidence run).
