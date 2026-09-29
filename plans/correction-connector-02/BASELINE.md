# CN2 Baseline — 2026-09-29

| Item | Value |
|---|---|
| Audited connector | 0.1.0.dev0 @ 28d147625c2a47417ad70f72d3b453cd8bd2e984 (= current HEAD lineage; no reset) |
| Core pinned | 0.2.10.dev0 (commit 1560d31; wheel sha256 4cde3b9a…50da0) |
| Reviewer env | Linux, Python 3.13.5; local env: Windows 11, Python 3.13.1 (platform-neutral seeds required) |
| Package | FIX_UPDATE_PLAN/ copied verbatim; `regressoes/` executables NOT delivered — the 20 CN2 cases are reconstructed from §4–11 reproductions (same node ids / causal conditions), as CN1's package instructed |
| Reviewer baseline | suite 144 PASS/9 FAIL/4 SKIP (9 = environmental fixtures); CN1 seeds 14/14 PASS; CN2 4 PASS/16 FAIL |
| Environmental items (not CN2 defects) | 6 synthetic-executable fixtures without POSIX exec bit; 1 Pi-inventory platform expectation; 2 seeds blocked by sandbox `proc_children` during ACTIVE probe — fixed as platform-neutral fixtures + honest NOT_PROBED degradation, no containment disabled |

CN2-00.03 (contract): NXL r3 event.ack carries no generation field — ACK
scope is validated from the authenticated connection identity (envelope
internal), per the plan's explicit guidance.
