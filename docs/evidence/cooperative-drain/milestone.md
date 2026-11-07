# SVC-07 — Connector cooperative shutdown

An operator stop now fences new productive admission and waits while existing
decisions, leases, results and acknowledgements continue. Timeout does not force
shutdown. Force requires an expired existing drain and the reviewed generation;
the CLI preserves explicit owner refusals. Direct authenticated IPC also persists
the selected supervisor stop intent before effects.

The host uses Core's public shared-journal drain reports and revisions. Historical
unobserved state stays UNKNOWN. A blocked observation cannot prevent an explicit
force decision from starting containment, but its owned reader must join before
stores/IPC/locks close. Cleanup owners remain tracked across unknown retries.

26 new cases are included in **194 passed /1 conditional non-Windows skip** in
both source and installed qualification; these totals are not additive. The
new wheel's124 Connector payload files match frozen source and installed bytes;
166 Core files match wheel1407014e… from Core commit3cdba87. All332 input hashes
were unchanged,173 imported modules resolved to site-packages, and pip check
passed. Source Ruff findings match15 preexisting items, with no new findings.
Earlier fixture and product-diagnostic attempts are retained in source/.

No wire-format change or new canonical authority. The technical run/drain.json
marker is additive; the existing supervisor CONTROL remains authoritative for
stop suppression. Runtime/journal data is retained. Rollback requires confirmed
drain and reverting this feature candidate with its prior Core dependency;
never remove an uncertain owner's lock or receipts to force rollback.

Tests use isolated homes, SQLite and synthetic native/HTTP/WSS peers. No provider,
personal vault, native service registration or actual login/logout was exercised.
Nexus integration and real M1/native service campaigns remain separate gates.
Commands, original outcomes, package/import proof and source hashes are retained
under their exact-byte manifests.
