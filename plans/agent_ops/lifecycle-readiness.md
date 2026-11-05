# R3 lifecycle increment — local ownership and readiness

The candidate now closes refused-lock descriptors, probes ownership without
creating state, correlates authenticated IPC to canonical state/user identity and
generation, and treats unreachable/draining/old peers as unready. The foreground
module respects `--state-dir`. Canonical shutdown writes a correlated receipt;
CLI outcomes distinguish confirmed drain from absence or uncertainty.

A wider regression exposed double cancellation of an attachment producer during
HTTP cleanup. Transport stop now joins that cleanup without a second cancel;
the focused regression and complete affected runtime suite pass. No production
queue, agent authority or NXL protocol was introduced.

Evidence is maintained in the Nexus repository's fixed R3 ledger and
`plans/agent_ops/evidence/pr02-connector-*.xml`: 46 ownership/readiness/IPC/boundary
tests, 75 affected daemon/runtime regression tests, plus isolated packaging and
installed lifecycle qualification. No personal service or provider was used.

Full SVC-11 remains open: supervisor integration, saved context and launch
serialization are subsequent increments. Do not promote all SVC acceptance
from these narrower checks.
