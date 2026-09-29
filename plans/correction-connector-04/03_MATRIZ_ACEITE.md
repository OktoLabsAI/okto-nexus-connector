# Matriz de aceite CN4

Os 14 primeiros casos foram executados (11 FAIL, três PASS); os demais são critérios complementares **NOT_RUN**, não alegações adicionais de defeito nem testes implementados. Não somar repetição/subconjunto como cobertura nova.

## ACN4-01 — test_p01_managed_approval_translates_after_server_authority[approve-accept]

**Estado nesta auditoria:** FAIL. **Camada:** fault injection / controle; consultar relatório para fronteira exata.

**Preparação/evidência:** evidencias/cn4_initial.xml; installed.xml; repeat.xml

**Resultado exigido:** Preservar a expectativa executável em regressoes/test_cn4_review.py.

## ACN4-02 — test_p01_managed_approval_translates_after_server_authority[deny-decline]

**Estado nesta auditoria:** FAIL. **Camada:** fault injection / controle; consultar relatório para fronteira exata.

**Preparação/evidência:** evidencias/cn4_initial.xml; installed.xml; repeat.xml

**Resultado exigido:** Preservar a expectativa executável em regressoes/test_cn4_review.py.

## ACN4-03 — test_p01b_server_refusal_precedes_any_native_dispatch

**Estado nesta auditoria:** FAIL. **Camada:** fault injection / controle; consultar relatório para fronteira exata.

**Preparação/evidência:** evidencias/cn4_initial.xml; installed.xml; repeat.xml

**Resultado exigido:** Preservar a expectativa executável em regressoes/test_cn4_review.py.

## ACN4-04 — test_p01c_approval_for_b_uses_binding_and_credential_of_b

**Estado nesta auditoria:** FAIL. **Camada:** fault injection / controle; consultar relatório para fronteira exata.

**Preparação/evidência:** evidencias/cn4_initial.xml; installed.xml; repeat.xml

**Resultado exigido:** Preservar a expectativa executável em regressoes/test_cn4_review.py.

## ACN4-05 — test_control_core_approval_accept_contract_is_operational

**Estado nesta auditoria:** PASS. **Camada:** fault injection / controle; consultar relatório para fronteira exata.

**Preparação/evidência:** evidencias/cn4_initial.xml; installed.xml; repeat.xml

**Resultado exigido:** Preservar a expectativa executável em regressoes/test_cn4_review.py.

## ACN4-06 — test_p02_bridge_waits_for_new_batch_not_old_ack

**Estado nesta auditoria:** FAIL. **Camada:** fault injection / controle; consultar relatório para fronteira exata.

**Preparação/evidência:** evidencias/cn4_initial.xml; installed.xml; repeat.xml

**Resultado exigido:** Preservar a expectativa executável em regressoes/test_cn4_review.py.

## ACN4-07 — test_p03_mcp_namespace_keeps_distinct_valid_long_ids[control_short_ids]

**Estado nesta auditoria:** PASS. **Camada:** fault injection / controle; consultar relatório para fronteira exata.

**Preparação/evidência:** evidencias/cn4_initial.xml; installed.xml; repeat.xml

**Resultado exigido:** Preservar a expectativa executável em regressoes/test_cn4_review.py.

## ACN4-08 — test_p03_mcp_namespace_keeps_distinct_valid_long_ids[long_valid_ids]

**Estado nesta auditoria:** FAIL. **Camada:** fault injection / controle; consultar relatório para fronteira exata.

**Preparação/evidência:** evidencias/cn4_initial.xml; installed.xml; repeat.xml

**Resultado exigido:** Preservar a expectativa executável em regressoes/test_cn4_review.py.

## ACN4-09 — test_p04_availability_surface_preserves_real_pi_pair[cli]

**Estado nesta auditoria:** FAIL. **Camada:** fault injection / controle; consultar relatório para fronteira exata.

**Preparação/evidência:** evidencias/cn4_initial.xml; installed.xml; repeat.xml

**Resultado exigido:** Preservar a expectativa executável em regressoes/test_cn4_review.py.

## ACN4-10 — test_p04_availability_surface_preserves_real_pi_pair[ipc]

**Estado nesta auditoria:** FAIL. **Camada:** fault injection / controle; consultar relatório para fronteira exata.

**Preparação/evidência:** evidencias/cn4_initial.xml; installed.xml; repeat.xml

**Resultado exigido:** Preservar a expectativa executável em regressoes/test_cn4_review.py.

## ACN4-11 — test_p04b_inventory_revision_changes_when_build_bytes_change

**Estado nesta auditoria:** FAIL. **Camada:** fault injection / controle; consultar relatório para fronteira exata.

**Preparação/evidência:** evidencias/cn4_initial.xml; installed.xml; repeat.xml

**Resultado exigido:** Preservar a expectativa executável em regressoes/test_cn4_review.py.

## ACN4-12 — test_control_inventory_revision_repeats_without_change

**Estado nesta auditoria:** PASS. **Camada:** fault injection / controle; consultar relatório para fronteira exata.

**Preparação/evidência:** evidencias/cn4_initial.xml; installed.xml; repeat.xml

**Resultado exigido:** Preservar a expectativa executável em regressoes/test_cn4_review.py.

## ACN4-13 — test_p05_reload_rotates_existing_lane_from_persisted_state

**Estado nesta auditoria:** FAIL. **Camada:** fault injection / controle; consultar relatório para fronteira exata.

**Preparação/evidência:** evidencias/cn4_initial.xml; installed.xml; repeat.xml

**Resultado exigido:** Preservar a expectativa executável em regressoes/test_cn4_review.py.

## ACN4-14 — test_p05b_rotated_lane_requests_new_ticket_and_reattaches

**Estado nesta auditoria:** FAIL. **Camada:** fault injection / controle; consultar relatório para fronteira exata.

**Preparação/evidência:** evidencias/cn4_initial.xml; installed.xml; repeat.xml

**Resultado exigido:** Preservar a expectativa executável em regressoes/test_cn4_review.py.

## ACN4-15 — Decisão concorrente

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Dois waiters sobre o mesmo pedido, primeiro retido no Server.

**Resultado exigido:** Apenas uma reserva/aplicação; segundo compartilha resultado ou conflito explícito.

## ACN4-16 — CAS com confirmação perdida

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Server confirma e perde resposta; request local é cancelado.

**Resultado exigido:** Mesma intenção consultável; nenhum efeito permissivo até evidência autorizada.

## ACN4-17 — Write parcial de aprovação

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Server confirma; native write/flush fica incerto.

**Resultado exigido:** Não restaurar pending como convite para segunda aplicação; preservar recibo.

## ACN4-18 — Input nativo

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Pedido de input realmente observado; resposta explícita e grant adequado.

**Resultado exigido:** Ação input correta derivada do contrato; dados não entram em log.

## ACN4-19 — Pedido curto ambíguo

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Mesmo request_id em dois Servers; CLI omite Server.

**Resultado exigido:** Erro de ambiguidade; zero requisições HTTP.

## ACN4-20 — Limpeza por chave de pedido

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Decisão finalizada com segredo distinto da chave do dicionário.

**Resultado exigido:** Somente o pedido resolvido sai de pending; memória/histórico limitados.

## ACN4-21 — ACK aplicado ao Core falha

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** ACK remoto chega, acknowledge_events falha temporariamente.

**Resultado exigido:** Retomada aplica a mesma confirmação, sem loop de reenviar operação do agente.

## ACN4-22 — ACK futuro/cruzado

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Watermark maior que escrito ou de outro namespace/conexão.

**Resultado exigido:** Não avança Core; controles originais preservados.

## ACN4-23 — Lote 129 e ACK parcial

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** 128+1 eventos, confirmação parcial e atrasada.

**Resultado exigido:** Progresso por alvo explícito, sem aceitar watermark obsoleto como novo.

## ACN4-24 — Marker estrangeiro

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Diretório existe com owner divergente.

**Resultado exigido:** Recusa antes de alteração dos arquivos; sem sobrescrever marker.

## ACN4-25 — Namespace com caracteres substituídos

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Dois IDs que a sanitização antiga colapsaria.

**Resultado exigido:** Derivação nova distingue owners ou validação tipada recusa os não suportados.

## ACN4-26 — Migração de HOME ativo

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Uma sessão ativa usa layout anterior.

**Resultado exigido:** Sem mover seus arquivos; nova abertura usa versão nova e cleanup seletivo.

## ACN4-27 — Seleção de duas cópias

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Mesmo build em duas instalações, inventário reordenado.

**Resultado exigido:** B resolve B pela API pública do Core; nenhuma escolha por posição.

## ACN4-28 — Revisão agregada consistente

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Exibir inventário com vários candidatos; selecionar um.

**Resultado exigido:** Binding conserva a revisão exibida, não hash de subconjunto recalculado.

## ACN4-29 — Revisão por qualificação

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Mesmos bytes; estado/razões técnicas ou confiança mudam.

**Resultado exigido:** Revisão muda quando a evidência de elegibilidade mudou.

## ACN4-30 — Snapshot remoto

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Server peer recebe projeção do executor remoto.

**Resultado exigido:** Core/format/executor/revisão/validade íntegros; sem paths/segredos; não é prova de UI real.

## ACN4-31 — No-op reload

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** StateStore não mudou e lane está ready.

**Resultado exigido:** Sem girar epoch nem reanexar desnecessariamente.

## ACN4-32 — Attach antigo chega tarde

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Rotação 1→2 com resposta antiga ainda pendente.

**Resultado exigido:** Resultado da tentativa 1 não abre lane 2.

## ACN4-33 — Ticket expirado

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Clock controlado atinge expiração retornada pelo Server.

**Resultado exigido:** Renovação single-flight ou estado pendente; não estender lease de runtime.

## ACN4-34 — Dois recursos no shutdown

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Um possui resultado incerto, outro está resolvido.

**Resultado exigido:** Política de recuperação/transferência mantém owner e produtores; sem abandono pelo fim do loop.

## ACN4-35 — Provider/Server reais

**Estado nesta auditoria:** NOT_RUN. **Camada:** complementar — teste ainda necessário.

**Preparação/evidência:** Topologia local/remota com credenciais autorizadas em campanha separada.

**Resultado exigido:** Demonstrar ciclo e ferramentas diretas, com mesmos artefatos. Não qualificado por mocks desta revisão.
