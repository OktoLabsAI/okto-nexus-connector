# Baseline CN4 — 2026-09-29

- Snapshot auditado: `e9307975a544c584fc64fe54c906ad512ae1b82b` (0.3.0.dev0)
  — HEAD local idêntico ao auditado; trabalho SEM reset.
- Core: `nexus-connector-core==0.2.10.dev0` (pin; `1560d31` no repo irmão).
- Pacote: 6 arquivos (00–04 + RESULTADOS_RESUMO.json). **Pela TERCEIRA vez os
  executáveis (`regressoes/`, `executar_verificacao.py`, `evidencias/`) NÃO
  chegaram** — apesar de `00_ENTREGAR_AO_AGENTE.md` afirmar que o ZIP os
  contém. Sementes reconstruídas das reproduções P01–P05 + node ids
  ACN4-01..14 (precedente CN1/CN2/CN3).
- CN4: 14 casos (11 FAIL + 3 PASS no snapshot); CN3 original adaptado: 12
  PASS (fix de fixture do auditor: `ensure_history_journal` no double — a
  nossa suíte CN3 já usa Core real).
