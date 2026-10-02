# Installed harnesses missing from discovery

2026-10-02: reproduced the user's zero-candidate result with installed Core .53
despite PowerShell locating codex.ps1, claude.exe, pi.ps1 and node.exe. The Core
filtered unapproved PATH executables out of inventory, and Connector's legacy
shim fallback did not resolve the actual npm/managed layouts.

Connector now uses the shared public Core discovery facade for standalone preview,
as the registered daemon already does. Core .54 observes these installations as
untrusted, retaining selection and execution guards. Its Windows layout reader
does not execute or interpret cmd/PowerShell/JavaScript wrappers. This is detection
of fixed package payloads, not a claim that arbitrary shell aliases resolve there.

The new installed CLI `--json discover` found all three real Windows providers:

| Adapter | Availability | Trust | Reasons |
|---|---|---|---|
| codex_app_server | NOT_PROBED | untrusted | no_build_observation, selection_required |
| claude_stream | NOT_PROBED | untrusted | no_build_observation, selection_required |
| pi_rpc (0.87.1) | PREPARATION_REQUIRED | untrusted | selection_required |

No provider was launched or version-probed. Pi's package dependency identity can
take substantial time to read; no performance acceptance is claimed. The global
CLI on this development machine separately resolves an editable Connector with
old Core 0.2.10 and raises ImportError. The repaired CLI was tested in an isolated
installed environment; that global installation was not silently replaced.

Installed discovery/configuration/catalog regression: Windows Python 3.13.1:
36 passed, one platform skip; Linux Python 3.12.13: 34 passed, three platform skips.
Evidence: `evidence/discovery-054-windows.xml`, `evidence/discovery-054-linux.xml`.
Core's own installed checks and refusal assertions are in Core commit `1c7cf79`,
`plans/DISCOVERY_054.md`. This does not close independent-host/provider acceptance.

Pinned Core wheel SHA-256:
`e79c4b9ccc5205bd2cfd05f750dbef543d771355e85c6853975f22367d36b9ff`.
Connector wheel SHA-256:
`f5a5df5e38f52857f0ffa415b524507d86ceae3cde96ea7b6121d1ab09f43696`.
Packaged Python files match reviewed source bytes. The Nexus CI wheel is updated
with that exact Connector artifact. Final M13 freeze and full regression remain open.
