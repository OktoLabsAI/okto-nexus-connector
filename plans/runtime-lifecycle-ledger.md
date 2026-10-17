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
