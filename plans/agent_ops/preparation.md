# Agent operations R3 preparation

`feature/v0.0.12` starts at fetched main
`a3017d97b6a295cb2b46b9dd391b4c04ed121ee2` (0.0.11), following the user's request
to refresh main and bump versions before implementation. The old branch remains
unchanged. Package/public metadata is now 0.0.12, pinning Core 0.0.14.

The local Connector wheel's 103 Python payload files match source byte-for-byte.
The vendored Core wheel matches the Nexus candidate (SHA-256
`5b9ffe72bef8c320e06fd9afee922f9c242960ca3cd3546daab5547ed157e8eb`).
`python -m pytest tests/unit/test_boundaries.py tests/unit/test_lock.py -q`:
19 passed on Windows. `python -m pytest tests/e2e/test_packaging.py -q`:
2 passed, including clean venv installation, CLI execution outside the checkout
and matching Core pin. Evidence is tracked in the Nexus ledger.
No NXL revision, schema, personal service or credential was changed.
These local candidate wheels are not published releases or provider qualification.

The full plan and canonical ledger are in the Nexus repository at
`PLANO_CODEX_NEXUS_INTEGRACAO_ATENCAO_ONBOARDING.md` and
`plans/agent_ops/ledger.json`. Next: BASE inventory and then extend/reuse the
existing daemon, IPC, vault and user service operations under SVC-11. This bump
needs no data migration; source metadata can be reverted without data loss.
