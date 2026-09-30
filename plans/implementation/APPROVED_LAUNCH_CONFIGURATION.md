# Approved launch configuration

The default R4 execution launch port now resolves configuration persisted under the digest approved by the realization and binding. Schema 8 adds scoped records with provider credential references and provider-home identity. Existing records migrate additively.

The host revalidates binding, profile revision, configuration content and physical directory identity before and after environment resolution. Core retains environment policy and prepared-reference checks. Vault access runs off the event loop. Missing or conflicting configuration fails before lease installation and native open.

Verification:
- Source unit/contract campaign: 262 passed, one skipped.
- Installed Connector campaign (unit, contract and native actions): 305 passed, one skipped.
- Exact artifact hashes and installed-package/source byte checks: Nexus repository, plans/r4_execution/evidence/approved-launch-artifacts.json and approved-launch-installed-connector.json.
- Tests use a technical native peer with real Core prepare, leases and journals; they do not qualify a real provider.

Remaining under the existing delivery plan: public onboarding capture, automatic tool capability/MCP/Pi composition, daemon lane adoption, lifecycle renewal, nonempty reconciliation and complete local/remote acceptance. This increment does not close M02 or M06.
