# CN1 Baseline — 2026-09-28

| Item | Value |
|---|---|
| Connector audited | 0.1.0.dev0 @ 8bc50259dc7eb353f349f1eb762be2d33cc393de (already the repo HEAD lineage; no reset performed) |
| Correction HEAD start | this commit (post-0.2.8 alignment lineage; all prior commits preserved) |
| Core pinned (audited) | 0.2.8.dev0 (wheel sha256 6f4823f3…e23a) |
| Core target (validated) | 0.2.10.dev0 — C10 (availability) + C11 (installation identity) APIs confirmed present in the shipped wheel before consumption |
| Audit input package | FIX_UPDATE_PLAN/ (copied verbatim to this directory; regressoes/test_connector_audit.py was NOT delivered — seeds reconstructed from RESULTADOS_RESUMO.json failure messages + report §5, per the package's own instruction that the 34 cases are the contract, not one exact file) |
| Environment | Windows 11 x86_64, Python 3.13.1, pytest 8.x, httpx 0.28.1, websockets 17.x (auditor env: Linux/3.13.5/websockets 16 — platform-neutral seeds required) |
| Suite snapshot (auditor) | 111 PASS / 7 FAIL / 3 SKIP; the 7 = 6 Linux exec-bit fixtures + 1 Pi-inventory expectation (classified §3.1, fixed as fixtures, no preflight disabled) |

No user files removed; the audited commit and the corrected commits stay distinct.
