# CN3 Baseline — 2026-09-29

| Item | Value |
|---|---|
| Audited connector | 0.2.0.dev0 @ 235439242e4de8edc7ca8940b80a331275be9fb6 (= current HEAD lineage; no reset) |
| Core pinned | 0.2.10.dev0 (1560d31; wheel sha256 4cde3b9a…50da0) |
| Reviewer env | Linux/Python 3.13.5; local: Windows 11/3.13.1 — platform-neutral seeds |
| Package | copied verbatim; `regressoes/` executables again NOT delivered — the 12 CN3 cases are reconstructed from the exact reproductions in report §4–10 (same node ids/causal conditions) |
| Reviewer baseline | suite 164/9/4 (9 = environmental fixtures, unchanged classification); CN2 executor 20 PASS; CN2 adapted 20 PASS; CN3 9 FAIL/3 PASS |
| Defect groups | CN3-01 generations/revisions dropped; CN3-02 foreign reconcile; CN3-03 admission bytes leak; CN3-04 cold-host history empty; CN3-05 duplicate ACK blocks waiter; CN3-06 ephemeral MCP home collision |
| Wiring gaps | G01 availability not in app flow; G02 approval route/tickets/open/callbacks |
