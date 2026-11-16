# Installed Connector SVC-07

The exact Connector wheel in `reports.json` was built from a frozen copy, installed into a new Temp venv with Core wheel `1407014…`, pinned third-party dependencies, setuptools 84.0.0 and wheel 0.45.1. Both pip checks passed. No Nexus wheel was needed.

The installed run used Python `-I`; only snapshot root/tests were added for fixtures. All loaded Connector/Core modules came from this venv's site-packages. Source paths were absent from sys.path. The 124 Connector package files matched source → wheel → install, and all 166 Core package files matched wheel → install, before and after tests. All 332 source/snapshot inputs retained their hashes.

`installed.xml`: 194 passed, one preexisting conditional non-Windows-only skip, zero failures/errors; 26 new drain cases are included. The source run and this installed run overlap and must not be summed. All preparation/test logs are retained; no preparation failed.

Scope is Windows/local synthetic: real installed packages/Core/SQLite and fake native/HTTP/WSS peers. No personal provider, credential vault, native service installation, productive M1/M2, or Nexus SVC-07 campaign is claimed. The allowlist excludes all fixture homes, databases, venv files and caches. The wheel remains separately in the adjacent wheels directory with its hash pinned in reports.json.
