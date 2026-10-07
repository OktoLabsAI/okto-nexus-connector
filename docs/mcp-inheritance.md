# MCP inheritance (Connector 0.0.2)

This version requires Connector Core 0.0.3 and pairs with Nexus 0.2.2.
The configuration wizard offers **Include global harness MCPs** for Codex/Claude:
inherit the agent/global policy, enable or disable. Harness configuration JSON
uses `inherit_global_mcps: "enabled"` / `"disabled"`; omit the key to inherit.

The Server resolves policy into the hashed R4 opening payload. The Connector
preserves its locally approved provider home when inheritance is enabled, even
with explicit provider secret bindings, and delegates MCP/environment composition
to Core. It does not read the Server machine's harness files or upload local MCP
configuration or credentials. An approved harness directory is required.

Native config scopes, OAuth and workspace trust still apply. Nexus tools remain
injected with their own scoped capability. A third-party MCP does not inherit the
Nexus tools' always-allow setting. Pi's native tools bridge is unchanged.

`test_approved_mcp_launch.py` covers both adapters, both inheritance modes,
provider-login and explicit-secret configurations, capability scope, expiry,
origin validation and cleanup.
