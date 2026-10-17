# M4d4b — Host-owned journal registration and storage lifetime

The Connector now registers its shared `executor-journal.db` in Core's physical
location registry before opening it. The immutable shared-host location is
distinct from Nexus's per-session locations. Registration is creation intent;
it grants no execution authority and proves no native process has stopped.

Opening the ledger and journal uses retained, single-flight tasks. Cancelling
a caller cannot lose the SQLite object created by its worker. Global shutdown
fences new compositions, joins these tasks and retains its own cleanup task
when an observer cancels. A composition that finishes selection after the
stores close cannot publish a runtime using stale handles. Executor-scoped
shutdown leaves the installation's stores available to other connections.

The unused synchronous store constructors were removed. Storage diagnostics
use the same async lifetime. Native containment and unresolved resource
retention are unchanged; no uncertain slot is released by registration.

Installed Windows qualification: **118 passed, 1 skipped**, 68.03 seconds.
The skipped test asserts non-Windows shim behavior; its Windows counterpart
ran. Exact bytes of 160 Core and 99 Connector package files matched the wheels,
and source/test inputs were unchanged during the campaign. `pip check` passed
in the isolated qualification environment.

Coverage includes registration before file creation, failed-open retry,
cancelled construction and shutdown observers, delayed capacity/runtime
composition, executor reconnect, approved selection revalidation, execution,
daemon integration and retained history without the current harness binary.

Exploratory source runs exposed a fixture that bypassed the host constructor;
the four affected fixtures now use normal initialization. Two new concurrency
tests initially patched the wrong import location and were corrected before
qualification. Two legacy namespace fixtures also leaked tasks; their cleanup
now joins their exact pump/lease tasks and closes their owned Core runtime and
journal. The installed campaign reported no teardown warnings.

Artifacts:

- Core 0.0.12, commit `2afb894`:
  `e93dc1e1cdcc5d0868265c36c0e1200a1ff048aa79371e410727b3dfe74cad3b`.
- Connector 0.0.10:
  `2e222043b889314a21492191a77b41b1ba890f1a8cc12b3885efecc651ce0893`.
- Reports: `build/runtime-m4d4b-host-installed/{manifest.json,results.xml}`.

No global installation or real-provider UI test occurred at this technical
checkpoint. It does not remove Nexus's startup directory audit or complete
the first-audit frontier, restore detection, M4/M5 or final UI gates. Those
remain tracked in the canonical Nexus ledger.
