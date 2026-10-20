# M3i — Independent daemon inventory integration

2026-10-17, Connector 0.0.10, branch `feature/v0.0.10`. Installed qualification passed; joint rollout pending.

The connection now owns one Core observation worker per discoverable harness
and an independent publication task. Control readiness and lease renewal do
not await a full discovery round. Partial inventory explicitly preserves
PENDING/FAILED coverage. A refresh delivery is acknowledged only after every
post-claim observation completes successfully and remains fresh.

Onboarding consumes the exact accepted batch, including coverage, for both
physical installation checks. Opening sessions consumes that batch without
rediscovery; the selected adapter must remain fresh under the current owner.
Other harnesses may still be pending. Candidate bytes, workspace identity,
permissions and execution generation checks remain in force. A superseded or
stopping observation owner cannot authorize a late administrative publication.

Before inventory publication uses its authorization, it re-reads the current
connection bootstrap after storage work. A concurrent in-place renewal must
not be mistaken for an expired old authorization. Expired authorization with
no actual renewal still refuses publication; deadlines are never extended
locally. Both cases have deterministic tests.

Shutdown fences workers first and retains discovery, publication and HTTP
ownership until they finish, even when an observer cancels its wait. No old
discovery worker can be detached into a replacement connection. The one-off
metadata publication for a server that has not qualified remote execution
retains its full scan; it cannot run execution or renew an execution lease.

Source qualification history:

- Initial consumer/control campaign: 74 passed, 8 failed. Failures identified
  assertions tied to full-round publication counts, checks before asynchronous
  cleanup completed, and mocks missing the selected adapter/scoped discovery.
- Initial independent cases: 2 passed, 1 failed because the expected Claude
  adapter name was `claude_code` instead of the Core catalog's `claude_stream`.
  The actual held-Pi renewal and refresh-frontier scenarios passed.
- Revised source campaign: **91 passed** in 122.27 seconds. JUnit:
  `build/m3i-independent-regression.xml`. Immutable installed-wheel qualification
  passed: **164 tests** in 68.57 seconds, with `changed_inputs=[]`.
  Core and Connector installed files match the exact wheels, and Connector
  source matches every packaged file. Dependency checks passed (42 packages).
  Reports: `build/runtime-lifecycle-m3i-producer-final-installed/`.
- First installed campaign: **159 passed, 3 failed**, unchanged inputs. Two
  tests observed partial startup before the specific asynchronous result they
  required. The renewal failure prompted the current-bootstrap recheck above.
  Its focused campaign passed both expiry/renewal reproductions; 56 passed
  overall and one test needed an explicit retry barrier to observe FAILED
  independently of control readiness. The final installed campaign includes
  those corrections and the expanded 164-case selection.

New scenarios cover two connection renewals during held Pi discovery, retained
shutdown after waiter cancellation, failed-adapter automatic retry, post-claim
refresh frontiers, stale/missing/failed selected evidence, partial-batch
onboarding and a real Core session open with a synthetic native peer while a
sibling remains pending. These are not real-provider or physical remote-host
tests. Coordinated Server rollout and major UI qualification remain pending.

Core dependency remains the qualified M3i 0.0.12 wheel
`13d72e63d1406a8cd6fcb154ce31bb218c0e042cce0bf946cc8e0350f69a3b9a`.
No global installation has been changed by this checkpoint.

Connector wheel SHA256: `41331e7b9405e830d990141e48e7702d461559a779b66ee39ec8aedc9e52d7c2`.
