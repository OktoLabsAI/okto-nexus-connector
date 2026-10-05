# Core .56 discovery adoption

Connector pins Core `0.2.56.dev0` from commit `7a964db`. This adopts passive
Codex npm payload and Pi Node/CLI layout resolution on POSIX; the compact
default table, `--verbose` diagnostics and full JSON contract are unchanged.
Discovery still does not execute providers or grant trust/runtime authority.
Native macOS containment remains unsupported; this is not closure of issue #1.

Installed Windows Python 3.13.1 and same-machine WSL Linux Python 3.12.13 each
passed 48 directed discovery, summary/JSON, configuration, inventory/refresh
and platform-diagnostic checks. See `evidence/posix-discovery-056-windows.xml`
and `evidence/posix-discovery-056-linux.xml`. Tests use installed application
packages with isolated Python; checkout test helpers are permitted.
The operational documentation audit passed 37 command examples and 12 local
links against the installed parser/help (`evidence/posix-discovery-056-docs.json`).

Artifacts:

| Artifact | SHA-256 |
| --- | --- |
| Core .56 wheel | `7f19885f28b16dbfce66b969ab42c79b9947246416c71147315006b6e618b1f6` |
| Connector 0.5.0.dev0 wheel | `b77e7c020a3e17dba69200f7f88c8a96fab273c29a6fcb941c7221f58ceaedf2` |
| Connector 0.5.0.dev0 sdist | `6be7f9ba29e3020e04166feb64420a23b742324a751bdf8d31fa819608c40c1f` |

The same Core/Connector wheel hashes are adopted by Nexus's installed campaign
manifest. These remain development artifacts; full regression/CI, final frozen
three-repository acceptance and manual independent-machine acceptance remain
open. The user's global installation has not been changed.
