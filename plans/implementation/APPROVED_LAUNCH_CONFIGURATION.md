# Approved launch configuration

The default R4 execution launch port now resolves configuration persisted under the digest approved by the realization and binding. Schema 8 adds scoped records with provider credential references and provider-home identity. Existing records migrate additively.

The host revalidates binding, profile revision, configuration content and physical directory identity before and after environment resolution. Core retains environment policy and prepared-reference checks. Vault access runs off the event loop. Missing or conflicting configuration fails before lease installation and native open.

Verification:
- Source unit/contract campaign: 262 passed, one skipped.
- Installed Connector campaign (unit, contract and native actions): 305 passed, one skipped.
- Exact artifact hashes and installed-package/source byte checks: Nexus repository, plans/r4_execution/evidence/approved-launch-artifacts.json and approved-launch-installed-connector.json.
- Tests use a technical native peer with real Core prepare, leases and journals; they do not qualify a real provider.

Remaining under the existing delivery plan: public onboarding capture, automatic tool capability/MCP/Pi composition, daemon lane adoption, lifecycle renewal, nonempty reconciliation and complete local/remote acceptance. This increment does not close M02 or M06.

## Pi native tool composition

The approved launch port now composes the Core Pi native owner from a durably issued capability. The default R4 execution owner and daemon adoption port accept trusted native tool services, retaining the existing approved environment and including the capability reference in Core prepare. Scope/configuration drift and lane loss stop opening; received material remains vaulted.

Installed Connector validation: 310 passed, one skipped. A Nexus technical Pi child campaign also exercises actual capability issuance, the approved environment, domain claims/completion and pending-action shutdown. This remains technical provider evidence; automatic daemon startup adoption and direct MCP composition remain pending.

During development, the initial fixture omitted the vault directory; after correcting it, the socket launch assertion exposed a missing capability prepare reference. The reference is now included and the real Core owner launches successfully.

## Direct HTTP MCP session configuration

ApprovedToolLaunchProvider selects native Pi or direct HTTP MCP composition from the approved adapter. Codex and Claude receive a dedicated session home containing only the Core-rendered client entry and an ownership marker. Provider secrets and the session capability are resolved into the process environment. The original approved provider home is not modified.

The capability requests the protocol ceiling and eight canonical tool permissions. Configuration, scope, origin, expiry, directory identity and file content are checked before launch/environment return. Conflicting files are never overwritten. Home-only provider login without imported secret references is explicitly refused for isolated MCP configuration; importing/migrating native login remains part of the incomplete onboarding work.

Installed Connector campaign: 328 passed, one skipped. The initial run had 320 passes, one failure and one skip because the boundary test matched the local helper name as an SDK import. The test now checks Python import nodes and includes seven cases, including combined imports and aliases. The initial evidence is preserved in Nexus under approved-mcp-installed-connector-initial.*.

A source integration obtained the capability over Nexus HTTP, applied a real Core lease, rendered the approved environment and used its bearer for agent identity, handoff claim and completion. Native provider behavior remains unqualified; automatic daemon boot adoption, renew/reconcile and full acceptance remain pending.
