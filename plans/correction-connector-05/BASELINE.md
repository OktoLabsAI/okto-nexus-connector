# Baseline CN5 — 2026-09-29

- Snapshot auditado: `87b8fd2e3e403cb6a70a1ce265618e29c7a6b86c` (0.4.0.dev0)
  — HEAD local idêntico; trabalho SEM reset.
- Core: `nexus-connector-core==0.2.10.dev0` (`1560d31`), pin inalterado.
- Pacote: 6 arquivos. **Pela QUARTA vez os executáveis (`regressoes/`,
  `executar_verificacao.py`, `evidencias/`) NÃO chegaram**, apesar de
  `00_ENTREGAR_AO_AGENTE.md` afirmar que "nesta rodada eles estão realmente
  incluídos". Sementes reconstruídas das especificações fixas do plano
  (seções 2–3) + node ids da matriz (precedente CN1–CN4).
- CN5: 6 FAIL confirmados (Q01a×2, Q01b, Q02, Q03, Q04) + 14 PASS.
- Suíte do auditor: 196 PASS / 9 FAIL / 4 SKIP — as 9 falhas são ambientais
  POSIX/proc_children/Pi (classificadas por nome, não são os 6 FAIL).
