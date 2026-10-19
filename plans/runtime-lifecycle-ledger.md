# Runtime lifecycle development ledger

Version: **0.0.10**. Branch: **feature/v0.0.10**. Core pin: **0.0.12**.

Canonical plan: Nexus repository, `plans/runtime_lifecycle/plan.md` on
`feature/v0.3.1`. Scope: shared lifecycle recovery, transport fairness and the
local Connector qualification before a separate physical host is available.

## M0 — Baseline validated

Preserve the existing artifact transfer and replay implementation, including
capability negotiation, durable event reference publication and chunk recovery.
Existing installed campaign: 41 Connector cases passed; the three-package
integration campaign totaled 106 cases.

2026-10-16 checkpoint: package and vendored Core metadata agree with the pins.
Diff whitespace checks and credential-pattern scan passed. Published in the
baseline commit on this branch; the canonical ledger records its hash next.

The new Core lifecycle behavior must be tested from an installed wheel and the
vendored Core rebuilt when integrated. Loopback qualification is not evidence
of behavior on another physical machine. No new lifecycle milestone is installed.

## M1 — Consume early process-birth recording (validated)

- Rebuilt and synchronized vendored Core 0.0.12 from Core commit `8520deb`.
  Dependency pin remains 0.0.12. Wheel SHA256:
  `d2799bb6249c28fca5e56d9e17862ca1b3f72b15a59c5f9e2d15d509a0d2da8f`.
- Installed-package execution and daemon regressions: **43 passed** with the new
  Core wheel, using the real Core journal and synthetic native peers.
  Tests: `tests/unit/test_r4_execution.py`, `tests/unit/test_r4_daemon_execution.py`.
  Local JUnit evidence: `build/runtime-lifecycle-m1-installed.xml`.
- Core's opening callback is used through the existing host composition; no
  duplicate process-birth implementation was introduced in Connector.
- This is an isolated installation qualification. The user's running installation
  was not updated, and no second physical host was qualified.
- Breaking changes are authorized for subsequent shared contracts; all three
  components may upgrade together instead of retaining old protocol paths.

Remaining: durable launch intent/fencing (M2), independent readiness (M3),
incremental recovery (M4) and shared local/remote policies (M5).

## M2e — Shared launch recovery (validated checkpoint)

Connector now consumes the Core producer/container recovery proof and publishes
never-started openings through the negotiated extension. Cancelled negotiation
retains native recovery work while revoking the old attempt's authority.
**103 installed tests passed**, including five real disposable Windows crash
boundaries. Vendored Core is synchronized to M2e (`90b4ef6`); pin remains 0.0.12.
See [`runtime-lifecycle-m2e.md`](runtime-lifecycle-m2e.md) for artifacts and scope.
This checkpoint does not complete M2/M5 or qualify another physical host.

## M3c — Shared original-owner containment (validated checkpoint)

Vendored Core 0.0.12 now recovers failed planned launches without requiring an
owner restart. **103 installed consumer tests passed**, with package byte checks
and stable inputs. A scoped-denial test fixture now delays only its intended
session. See [`runtime-lifecycle-m3c.md`](runtime-lifecycle-m3c.md) for the initial
failure, final qualification and remaining renewal-isolation scope.

## M3e — Scoped local lease failure (validated checkpoint)

71 installed cases passed. See [runtime-lifecycle-m3e.md](runtime-lifecycle-m3e.md) for scope,
artifact identities, initial test failures and remaining boundaries.
No global rollout; M3/M5 remain in progress.

## M3h — Shared parallel discovery (validated checkpoint)

35 installed discovery/inventory cases passed with the updated Core wheel,
byte-verified packages and unchanged inputs. See
[runtime-lifecycle-m3h.md](runtime-lifecycle-m3h.md). Global rollout and
per-harness readiness remain pending in the canonical Nexus plan.

## M3i — Partial inventory selection foundation

36 installed cases passed with Core's format-3 contract, byte-verified packages
and unchanged inputs. Publication and exact selection accept the same partial
coverage; the Core vendor is synchronized. See
[runtime-lifecycle-m3i.md](runtime-lifecycle-m3i.md). Independent daemon workers,
onboarding batch ownership, joint execution qualification and global rollout
remain pending. The current live instance was not changed.

## M3i — Independent daemon producer (validated checkpoint)

The actual daemon now separates per-harness observation/publication from the
control loop and consumes accepted batches for onboarding and session open.
The revised source campaign passed 91 cases, including retained shutdown,
connection renewal with Pi discovery held, partial selection, fresh evidence
and refresh frontiers. Installed artifact qualification passed 164 cases,
with unchanged inputs, exact package/source byte verification and valid dependencies.
See [runtime-lifecycle-m3i-producer.md](runtime-lifecycle-m3i-producer.md).
This does not complete M3/M5 or the coordinated real UI gate.

## M8 — Installed subprocess provenance (validated checkpoint)

11 installed cases passed with no skips, unchanged inputs and exact package-byte
verification. Children now use the parent's installed package; a poisoned inherited
PYTHONPATH cannot replace it. The actual CLI starter exits while its daemon keeps
answering authenticated IPC, and a forged client cannot stop it. Windows launcher
PIDs are distinguished from daemon PIDs; cleanup drains the private daemon before
joining starter handles and never erases evidence of a survivor. See
[runtime-lifecycle-m8-process.md](runtime-lifecycle-m8-process.md).
Product wheels and global installation are unchanged; M5/M8 remain incomplete.

## M6 — Supervisor experiment design (implementation pending)

[runtime-lifecycle-m6-supervisor-adr.md](runtime-lifecycle-m6-supervisor-adr.md)
selects the existing daemon/Core host for the isolated experiment. Process
independence is established by the M8 checkpoint, but control replacement still
waits for retained leases and containment. Named-pipe IPC, the actual comparison
and an adoption decision remain pending. No default lifecycle or authority fence
was changed by this design checkpoint.

## M4d3 — Released pre-open capacity dependency qualification

Core vendor synchronized. 70 distinct installed cases qualified: 68 passed in
the initial campaign; two timed-out cases passed unchanged on the focused retry.
Exact artifacts and unchanged inputs verified in both runs. See
[runtime-lifecycle-m4d3.md](runtime-lifecycle-m4d3.md). Connector source and global
installation are unchanged; remaining M4/M5 criteria are not closed.

## M4d4b — Physical registry and retained shared-store lifetime

Shared journal intent is registered through Core before creation. Retained
open/shutdown tasks prevent cancelled observers from abandoning stores;
executor reconnect preserves shared storage, and late compositions reject
closed handles. Installed qualification passed 118 tests with one platform
skip, exact package bytes and unchanged inputs. See
[runtime-lifecycle-m4d4b.md](runtime-lifecycle-m4d4b.md) and its results JSON.
Core vendor is synchronized to the qualified `2afb894` artifact. The first
audit frontier, M4/M5 completion and coordinated global rollout remain pending.

## M5a — Shared refusal dependency qualification

Core vendor synchronized to `19bb8f2`; wheel SHA-256
`dfdfef2edeb9f8db8697e6b850c05a704698e692a3592ed6d9d827887235f816`.
52 installed preopen/launch/execution cases passed with exact package-byte
verification and unchanged inputs. See `runtime-lifecycle-m5a-results.json`.
Nexus additionally qualified four separate-process preopen publication cases
after updating its synthetic discovery callback to the per-adapter port.

The new startup refusal API is not yet integrated into this host. A local
preparation-failure reproduction still closes the shared control connection;
its scoped recovery, canonical closure and capacity-release integration remain
pending. This checkpoint changes the dependency, not Connector execution code,
and does not claim completion of M5 or a production rollout.

## M5b — Scoped preparation failure

54 selected installed cases passed with the current Core/Connector wheels and
unchanged inputs. The remote preparation failure no longer closes the whole
control connection; a separate Connector process demonstrated a failed first
opening and successful subsequent handoff. Nexus accepted the scoped terminal
receipt; the Connector released the original capacity. See
[runtime-lifecycle-m5b.md](runtime-lifecycle-m5b.md) and its results JSON.
This covers post-lease native preparation errors; M5 and UI qualification
remain open.

## M3n — Pi version-probe dependency synchronization

The vendored Core 0.0.12 wheel now has SHA-256
`004a9472140bf2b89c1820281ba7c4a9a658a924adc0fe1e4fcdec749059f8b5`.
It extends only Pi's owned version observation from three to ten seconds.
Three source and three isolated installed Core cases passed; an installed
disposable Nexus validated the real Pi connection, two Meta-harness turns and
an automatic post-restart reply. See the Core M3 Pi probe report and Nexus
`plans/runtime_lifecycle/m3n-pi-probe-ui.md`. Connector execution code is
unchanged; a separate Connector-host UI gate remains pending under M5/UI-B.

## M3o — Pi native-action lease renewal dependency synchronization

The vendored Core 0.0.12 wheel now has SHA-256
`95a6238deae20d1a98ea8c8a44dcb808c24e037eb01e0d38163265fc1dc72193`
from Core commit `f5325af`. It accepts a new Pi native-action lease ID only
with an advancing serial and unchanged authorized subject/grant/boot scope.
Eight focused installed Core cases and one installed bridge case passed; the
disposable UI trace that motivated the change is documented in the Core and
Nexus M3o/M3n reports. Connector execution code is unchanged. Exact-wheel
real UI and the separate Connector-host qualification remain pending.

## M7a — Cold preparation dependency synchronization

The vendored Core 0.0.12 wheel now has SHA-256
`f0d6c9141ce59c04e01c419711b2b1e40f026ff99ba79c88c157f95da36ca457`
from Core commit `680c677`. It defers one duplicate full Pi build hash from
the non-effectful runtime preparation; admitted open still validates the full
identity before capacity reservation and spawn. Core passed 88 source and 11
selected installed cases. Connector execution code is unchanged; Connector
host and UI-B qualification remain pending.

## M7b — Passive Pi identity reader budget

The vendored Core 0.0.12 wheel now has SHA-256
`d9cf577731e123c417eb731c6b34b5e6fa208be7f33d86441e426c8fedadc1e8`
from Core commit `ab87980`. Passive inventory discovery uses two concurrent
file readers; admitted runtime opens retain four. Both still hash the full
dependency closure and return the same build identity. Core passed 100
selected source and 100 isolated installed-wheel tests. Connector execution
code and its Core version pin remain unchanged. Real Connector-host load and
UI-B qualification remain pending.
The Connector wheel was built and installed into a clean test environment
with this exact vendored Core wheel; both packaging tests passed.

## M4d4g — Mixed Core ledger budget

Core commit `9b84aa4` measured 256,573,440 allocated bytes for 100,000
physical registrations paired with terminal capacity records. The default
slot-ledger growth ceiling is now 512 MiB rather than 64 MiB; it does not
preallocate that space. Connector vendors the exact Core 0.0.12 wheel with
SHA-256 `55b58a13f7112e63a44a08efb787ca5e43128f4d090fd639a7a9b307e5e43ecd`.
Core passed 15 source and 15 installed-wheel tests, and both Connector
packaging/installation tests passed with the new wheel. No Connector
execution code changed. Real separate-host load and M4's UI gate remain open.

## M6a — Opt-in Windows named-pipe experiment

An explicit per-logon DACL, remote-client rejection, Proactor overlapped I/O
and the existing IPC token gate now serve the real daemon dispatcher/Core
host over a private named pipe. Eight source and eight installed-wheel tests
passed, including a disposable separate daemon process. Default IPC remains
unchanged. CLI support, real session/reconnect comparison, cost, containment
and the UI gate remain pending. See
[runtime-lifecycle-m6a-named-pipe.md](runtime-lifecycle-m6a-named-pipe.md).

## M5k — Proved initial-refusal capacity release

The initial lease refusal path now releases consumed capacity only with an
exact durable no-effect proof and no native claim or launch. Reconciliation
applies the same rule before the owned-slot fence. Fifteen focused cases
passed. The installed disposable Connector recovered 21 historical operations
on its first new control attempt and stayed `CONTROL_READY`; the original
broadcast handoff then completed through real Nexus UI after its five-second
start deadline was corrected to 300 seconds. See
[runtime-lifecycle-m5k-denied-capacity.md](runtime-lifecycle-m5k-denied-capacity.md).

## M3u — CI consumer alignment

The Connector CI run `39178834200` installed an older vendored Core 0.0.12
wheel. Its journal lacked the pending-claim and reconciliation-retirement
operations already required by this branch, causing repeated daemon recovery
failures. The vendored wheel is now rebuilt from Core `8bde891` with SHA-256
`faea17f68891a6112a6392c5b6078ce06583a5e34e718fb152380c92f30bfa64`.
The registered-adapter fixture now declares a discoverable executable; state
migration expectations use schema 15; cancelled passive discovery may publish
pending inventory but never its cancelled candidate. An isolated Connector
installation using this wheel passed 65 recovery/registration cases and 95
schema-adjacent cases before the cancellation assertion was updated; that
case passed separately. Full CI qualification remains pending.
