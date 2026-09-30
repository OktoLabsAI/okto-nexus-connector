# R4 automatic control startup — partial M02/M06

The real daemon lifecycle now selects R4 for an explicitly registered executor.
It checks public protocol compatibility, replays the durable registration with
the exact imported identity, negotiates an authenticated control channel and
publishes the Core inventory. Servers without an R4 registration retain the
existing transport. Adding R4 to a running legacy transport requires a drain;
reload does not silently transfer resource ownership.

Schema 6 adds the inventory publication sequence to the registration record.
The sequence is reserved atomically before HTTP and never reused after a lost
response or restart. The current reconciled connection ID identifies a remote
inventory producer. A new connection cannot reset the sequence. Before remote
qualification, the initial registration may publish bootstrap inventory, but
the daemon does not open WSS or report execution readiness.

The startup owner retains one task per Server, at most 32, with bounded retry
backoff. Reconnect obtains a fresh ticket; control proof renewal reconnects
before expiry. It refreshes inventory before its advertised freshness expires
and revalidates registration, identity and origin after waits. Tickets stay
out of state and IPC status. Cancelling a stop observer does not cancel an
in-flight inventory/state producer. Shutdown joins startup before closing Core
stores and reports unresolved recovery or socket cleanup.

## Recovery boundary

The initial reporter opens the durable Core journal and reads claims plus both
ownership ledgers through public APIs. It reports empty only for a verified
empty namespace. Nonempty Server requests, local claims, reservations, failed
reads or an incomplete bounded scan cannot become control readiness. This is
an explicit recovery boundary; nonempty reconciliation remains to implement.

No lanes are attached automatically by this increment. The earlier execution
owner remains available for negotiated connections, but production environment
and capability composition, lane/ticket rotation, lease renewal, durable
receipt/event recovery and native decision ingress must be connected before
automatic runtime execution is accepted. IPC distinguishes control readiness
from execution readiness, which remains false on this startup path.

## Verification

- 65 directed tests passed: startup, registration, selection and state.
- Full Connector regression: 325 passed, two existing skips.
- Nexus R4: 107 passed, including real CLI registration, daemon run loop,
  HTTP/WSS, IPC status/shutdown and a second daemon boot.
- Isolated installed campaign: 109 passed, with package bytes checked against
  wheels and current production sources. Groups overlap.
- The positive control test supplies synthetic Server qualification; the
  negative test uses unchanged production readiness. No provider is qualified.

The first test attempts exposed fixture cleanup errors, an observation race,
the missing Server inventory-producer handoff, and error-response/test transport
issues. Recorded failures and corrected runs are retained. Build initially
failed because the Nexus test venv had no setuptools backend; fresh staging
with the existing global build interpreter succeeded.

Evidence: `evidence/r4-startup-report.json`; coordinated Nexus evidence:
`plans/r4_execution/test_runs_20260930_startup.json`. Core stays at 0.2.28.dev0
with unchanged wheel hash. Older Connector binaries reject schema 6; the full
backup/restore/rollback campaign remains M12. No product gate is closed.
