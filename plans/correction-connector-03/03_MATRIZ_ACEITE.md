# Matriz de aceite CN3

43 cenários. Os primeiros 12 foram executados contra fonte e wheel: 9 FAIL e 3 PASS. O controle de build/import C28 também passou. C29–C31 estão BLOCKED por falta de campanha qualificada/hosts/providers nesta auditoria; demais complementares estão NOT_RUN. Não são novos bugs alegados nem testes já escritos.

## Campanhas históricas

Os 20 CN2 do executor e os 20 CN2 adaptados passaram em campanhas sobrepostas. A suíte (excluídos dois testes de packaging) teve 164 PASS/9 FAIL/4 SKIP. Ver evidencias/RESULTADOS_RESUMO.json. Esses totais não são adicionados como novos cenários abaixo.

## ACN3-E01 — Operação legítima no mesmo contexto

**Estado:** PASS. **Camada:** codec/receiver/manager/Core/SQLite/peer. **Achados:** CONTROL.

**Preparação:** Sessão Core real: connection_generation=3, owner=1, configuração=1; lane do receiver admitida. Peer não chama provider.

**Ação:** Enviar frame variante valid, pelo codec e receiver. No hash_changed, só o payload é alterado após calcular hash.

**Resultado exigido:** Um send_turn no peer; operação e recibo coerentes.

**Teste:** `regressoes/test_cn3_review.py::test_d01_full_pipeline_preserves_intent_and_grant[valid]`

## ACN3-E02 — Hash adulterado é recusado pelo codec

**Estado:** PASS. **Camada:** codec/receiver/manager/Core/SQLite/peer. **Achados:** CONTROL.

**Preparação:** Sessão Core real: connection_generation=3, owner=1, configuração=1; lane do receiver admitida. Peer não chama provider.

**Ação:** Enviar frame variante hash_changed, pelo codec e receiver. No hash_changed, só o payload é alterado após calcular hash.

**Resultado exigido:** Zero envio; adulteração rejeitada pelo codec antes do dispatcher.

**Teste:** `regressoes/test_cn3_review.py::test_d01_full_pipeline_preserves_intent_and_grant[hash_changed]`

## ACN3-E03 — Configuração do envelope diverge

**Estado:** FAIL. **Camada:** codec/receiver/manager/Core/SQLite/peer. **Achados:** CN3-01.

**Preparação:** Sessão Core real: connection_generation=3, owner=1, configuração=1; lane do receiver admitida. Peer não chama provider.

**Ação:** Enviar frame variante configuration_changed, pelo codec e receiver. No hash_changed, só o payload é alterado após calcular hash.

**Resultado exigido:** Zero envio sob configuração antiga; erro tipado.

**Teste:** `regressoes/test_cn3_review.py::test_d01_full_pipeline_preserves_intent_and_grant[configuration_changed]`

## ACN3-E04 — Owner generation do envelope diverge

**Estado:** FAIL. **Camada:** codec/receiver/manager/Core/SQLite/peer. **Achados:** CN3-01.

**Preparação:** Sessão Core real: connection_generation=3, owner=1, configuração=1; lane do receiver admitida. Peer não chama provider.

**Ação:** Enviar frame variante owner_changed, pelo codec e receiver. No hash_changed, só o payload é alterado após calcular hash.

**Resultado exigido:** Zero envio; owner declarado não é descartado nem autoadotado.

**Teste:** `regressoes/test_cn3_review.py::test_d01_full_pipeline_preserves_intent_and_grant[owner_changed]`

## ACN3-E05 — Conexão avançou mas Core ainda não

**Estado:** FAIL. **Camada:** codec/receiver/manager/Core/SQLite/peer. **Achados:** CN3-01.

**Preparação:** Sessão Core real: connection_generation=3, owner=1, configuração=1; lane do receiver admitida. Peer não chama provider.

**Ação:** Enviar frame variante generation_changed, pelo codec e receiver. No hash_changed, só o payload é alterado após calcular hash.

**Resultado exigido:** Zero envio antes da sincronização/renovação autorizada no Core.

**Teste:** `regressoes/test_cn3_review.py::test_d01_full_pipeline_preserves_intent_and_grant[generation_changed]`

## ACN3-E06 — Reconcile estrangeiro

**Estado:** FAIL. **Camada:** receiver/dispatcher/manager/SQLite. **Achados:** CN3-02.

**Preparação:** Journal A contém op_open; canal configurado B.

**Ação:** Pedir op_open de A por reconcile.request válido no canal B.

**Resultado exigido:** Sem consulta/resposta de dados A para B; erro escopado.

**Teste:** `regressoes/test_cn3_review.py::test_d02_foreign_reconcile_does_not_expose_receipt`

## ACN3-E07 — Reserva de bytes encerrada

**Estado:** FAIL. **Camada:** receiver/scheduler real. **Achados:** CN3-03.

**Preparação:** Quota padrão e 12 frames válidos com workspace binding ID longo.

**Ação:** Concluir cada operação antes da próxima.

**Resultado exigido:** Zero itens/bytes em reserva no final.

**Teste:** `regressoes/test_cn3_review.py::test_d03_admission_bytes_released_exactly`

## ACN3-E08 — Consulta após reinício do host

**Estado:** FAIL. **Camada:** host/manager/SQLite real. **Achados:** CN3-04.

**Preparação:** Persistir receipt, aclose e novo CoreRuntimeHost no mesmo root.

**Ação:** Reconcile público sem runtime/journal aberto em memória.

**Resultado exigido:** Receipt existente retornado; não compor provider.

**Teste:** `regressoes/test_cn3_review.py::test_d04_restart_reads_durable_receipt_without_prior_runtime`

## ACN3-E09 — ACK duplicado após replay

**Estado:** FAIL. **Camada:** sender/codec/ACK real. **Achados:** CN3-05.

**Preparação:** Escrever seq1, receber ACK1 e concluir primeira espera.

**Ação:** Reenviar seq1 e receber ACK1 novamente.

**Resultado exigido:** Nova espera de seq1 conclui com watermark1.

**Teste:** `regressoes/test_cn3_review.py::test_d05_duplicate_valid_ack_satisfies_replay_waiter`

## ACN3-E10 — Revisão de lane muda enquanto aguarda

**Estado:** FAIL. **Camada:** receiver/scheduler real. **Achados:** CN3-01.

**Preparação:** Quatro submits ocupados; quinta operação revisão1 enfileirada.

**Ação:** Mudar autorização da lane para2 e liberar os quatro.

**Resultado exigido:** Quinta não chega ao handler.

**Teste:** `regressoes/test_cn3_review.py::test_d06_lane_revision_change_invalidates_queued_operation`

## ACN3-E11 — Configuração isolada por Server

**Estado:** FAIL. **Camada:** renderers Core/filesystem real. **Achados:** CN3-06.

**Preparação:** Bindings A/B no mesmo executor físico e session_id igual.

**Ação:** Gerar configs diretas com URLs/capability refs distintas.

**Resultado exigido:** Diretórios diferentes; primeiro arquivo permanece igual.

**Teste:** `regressoes/test_cn3_review.py::test_d07_ephemeral_mcp_home_is_scoped_by_server_and_binding`

## ACN3-E12 — Reconcile legítimo

**Estado:** PASS. **Camada:** handler/manager/SQLite real. **Achados:** CONTROL.

**Preparação:** Journal A e canal A; operation_id conhecido.

**Ação:** Pedir o receipt no mesmo namespace.

**Resultado exigido:** Retorna op_open correto.

**Teste:** `regressoes/test_cn3_review.py::test_d08_scope_declined_reconcile_control`

## ACN3-C01 — Nova geração autorizada permanece utilizável

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-01.

**Preparação:** Core em3; contexto completo autorizado para5.

**Ação:** Renovar pelo port público, confirmar e então submeter no canal5.

**Resultado exigido:** Envio único em5; não bloqueio permanente como contorno.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C02 — Rotação de credencial invalida reserva antiga

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-01.

**Preparação:** Operação enfileirada e epoch de credencial antiga.

**Ação:** Trocar credential_epoch sem liberar inicialmente a fila.

**Resultado exigido:** Zero efeito da reserva antiga; outra lane independente funciona.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C03 — Controle não amplia grant

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-01.

**Preparação:** Grant sem runtime.close; outro grant contém controles.

**Ação:** Pedir controle remoto nos dois e negar aprovação no scope correto.

**Resultado exigido:** Nenhuma capability criada pelo booleano containment; controles autorizados seguem sem lease produtiva quando contrato permite.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C04 — Receipt conhecido durante fence

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-01.

**Preparação:** Operação já tem receipt; conexão/revisão muda.

**Ação:** Consultar/repetir mesma operação e tentar intenção nova.

**Resultado exigido:** Consulta idempotente sem efeito; novo efeito incompatível recusado.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C05 — Executor estrangeiro no reconcile

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-02.

**Preparação:** Mesmo Server com executor distinto do canal.

**Ação:** Reconcile de executor alheio.

**Resultado exigido:** Não ler namespace estrangeiro nem remapear ao local.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C06 — Detach/approval de outro namespace

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-02.

**Preparação:** Dois Servers com IDs de binding/pedido iguais.

**Ação:** Entregar frames de escopo estrangeiro ao canal atual.

**Resultado exigido:** Sem remover lane/pedido local nem encaminhar decisão de outro Server.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C07 — Cold restart sem binário

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-04.

**Preparação:** Recibo e eventos persistidos; encerrar host e remover executável.

**Ação:** Criar host novo e recuperar história/stream.

**Resultado exigido:** Recibo e eventos disponíveis sem exigir candidate_for/prepare.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C08 — Inicialização de journal concorrente

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-04.

**Preparação:** Store fechado e duas consultas/startup simultâneas.

**Ação:** Reter abertura e cancelar um waiter.

**Resultado exigido:** Um produtor do store, nenhum recurso órfão, segundo consulta corretamente.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C09 — Bootstrap sem IDs de sessões em memória

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-04.

**Preparação:** Claims/cursores persistidos; novo daemon.

**Ação:** Reconciliação inicial com enumeração paginada.

**Resultado exigido:** Pendências descobertas, não relatório vazio por ausência de sessões RAM.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C10 — Quota total cumulativa maior que limite em voo

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-03.

**Preparação:** Limite de teste pequeno mas maior que um frame.

**Ação:** Muitos frames sequenciais totalizam mais bytes que o limite.

**Resultado exigido:** Nenhuma recusa falsa ao esvaziar; contadores retornam a zero.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C11 — Reserva finalizada após cancelamento/erro

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-03.

**Preparação:** Tarefas em espera e em execução com tokens de reserva.

**Ação:** Cancelar waiter, lançar erro, invalidar lane e repetir finalizador.

**Resultado exigido:** Cada reserva é liberada exatamente uma vez; saldo não negativo.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C12 — Flood de recusa também limitado

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-03.

**Preparação:** Admissão cheia e fila de saída lenta.

**Ação:** Entregar excesso de operações recusáveis.

**Resultado exigido:** Número de tasks de rejeição e bytes permanece limitado; receipts aceitos ficam recuperáveis.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C13 — ACK antigo não completa lote novo

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-05.

**Preparação:** ACK1 válido e novo envio seq2.

**Ação:** Esperar confirmação de2 recebendo só ACK1; depois ACK2.

**Resultado exigido:** Não concluir cedo; concluir após2 sem loop ocupado.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C14 — Falha ao aplicar ACK já validado no Core

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-05.

**Preparação:** Peer já confirmou1; primeira gravação de acknowledge falha.

**Ação:** Restaurar journal sem produzir evento novo.

**Resultado exigido:** Mesma confirmação é aplicada, sem exigir novo trabalho ou nova prova do peer.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C15 — Reconexão sem novo evento

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-04, CN3-05.

**Preparação:** Um stream tem registros não confirmados e depois fica ocioso.

**Ação:** Cair/reabrir socket e reconciliar.

**Resultado exigido:** Reenvio a partir do ACK válido; nenhum turno reiniciado.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C16 — Configuração efêmera em dois bindings do mesmo Server

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-06.

**Preparação:** Mesmo texto de session_id em escopos diferentes suportados.

**Ação:** Preparar/aplicar e encerrar sóA.

**Resultado exigido:** B intacto; ownership inclui chaves efetivas, não só Server.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C17 — Falha na aplicação atômica da configuração

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-06.

**Preparação:** Arquivos anteriores válidos e novo owner/staging.

**Ação:** Falhar antes de replace ou cancelar o waiter.

**Resultado exigido:** Nenhum arquivo misturado; recuperação seletiva e sem remoção de terceiros.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C18 — Retenção de config sob fechamento unknown

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-06.

**Preparação:** Sessão continua possuída, resultado de stop incerto.

**Ação:** Timeout do waiter e outra sessão usa mesma instalação.

**Resultado exigido:** Config da primeira permanece até resolução; segunda usa seu próprio owner.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C19 — Inventário entra pela API da aplicação

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** G01.

**Preparação:** Duas cópias iguais em trusted roots, Core real passivo.

**Ação:** Chamar CLI/IPC/serviço real, publicar snapshot, selecionarB.

**Resultado exigido:** Core fornece refs diferentes; B é resolvido e persistido sem array de runtime duplicado.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C20 — Revisão muda com conteúdo sem mudar versão textual

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** G01.

**Preparação:** Instalação estável; version string igual.

**Ação:** Alterar evidência de build e renovar inventário.

**Resultado exigido:** Revisão antiga rejeitada/atualizada explicitamente; não autorizar por cache antigo.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C21 — Formato remoto desconhecido

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** G01.

**Preparação:** Snapshot com format_version não suportado.

**Ação:** Tentar exibir como elegível e selecionar.

**Resultado exigido:** Incompatível, sem READY implícito nem import arbitrário de módulo.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C22 — Expiração e rotação de ticket em transporte online

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** CN3-01, G02.

**Preparação:** Duas lanes e tickets com deadlines/epochs distintos.

**Ação:** Expirar/rotacionar A e adicionar B mantendo socket.

**Resultado exigido:** Renovação single-flight e isolamento; nenhuma dependência da ordem de listas.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C23 — Ciclo remoto completo com peer de Server

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** G02.

**Preparação:** Contrato acordado e binding sem sessão previamente aberta.

**Ação:** Remote open→submit→event→interrupt→close→reconcile.

**Resultado exigido:** Mesmos IDs/grants; o aplicativo faz o wiring, não teste que chama Core isolado.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C24 — Decisão humana chega à aplicação nativa

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** G02.

**Preparação:** Pedido original observado e autoridade de operador exigida.

**Ação:** CLI/IPC envia decisão ao Server peer e aplica a decisão autorizada.

**Resultado exigido:** Core recebe request/turn/ação corretos; validação falha não consome pedido.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C25 — Bridge Pi/resume pela composição do Connector

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** G02.

**Preparação:** Callbacks e APIs disponíveis em combinações declaradas.

**Ação:** Iniciar pelo aplicativo e acionar callback próprio.

**Resultado exigido:** Backend recebe scope limitado; codec/driver único no Core; ausências explícitas.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C26 — Shutdown unknown com política real de ownership

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** G02.

**Preparação:** Processo de laboratório em SO com containment qualificado.

**Ação:** Interromper cliente durante abertura/close e esgotar prazo.

**Resultado exigido:** Recuperação do mesmo recurso ou transferência comprovada; exit1 não é a evidência.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C27 — Resposta HTTP perdida após mutação

**Estado:** NOT_RUN. **Camada:** complementar a implementar. **Achados:** G02.

**Preparação:** ID estável antes do POST e peer confirma recebimento.

**Ação:** Perder resposta, reiniciar app e consultar.

**Resultado exigido:** Uma intenção/efeito; receipt publicável, não segunda execução com outro ID.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C28 — Build/import de pacote isolado

**Estado:** PASS. **Camada:** packaging. **Achados:** CONTROL.

**Preparação:** Fonte atual, Core pinado, setuptools local.

**Ação:** Construir wheel/sdist e importar com -I fora da fonte.

**Resultado exigido:** Pacote/version/catalogue/availability importáveis; isso não certifica providers.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C29 — Matriz de plataforma sem remover gates

**Estado:** BLOCKED. **Camada:** qualificação externa. **Achados:** QUALIFICATION.

**Preparação:** SO com recursos de processo necessários.

**Ação:** Executar suíte completa e testes de containment/autostart daquele SO.

**Resultado exigido:** Campanha própria; fixtures e ambiente separados de bug. Não generalizar ao sandbox.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C30 — Integração vertical com aplicativos reais

**Estado:** BLOCKED. **Camada:** qualificação externa. **Achados:** QUALIFICATION.

**Preparação:** ServerA e ConnectorB, mesmo Core e autorização de teste.

**Ação:** Binding/UI→open remoto→ferramentas diretas→recovery; repetir Core local no Server.

**Resultado exigido:** G2 somente com evidência dos aplicativos/hosts reais, não smoke de biblioteca.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.

## ACN3-C31 — Provider real e HOME efêmero

**Estado:** BLOCKED. **Camada:** qualificação externa. **Achados:** QUALIFICATION.

**Preparação:** Um harness qualificado com login/referências aprovados.

**Ação:** Iniciar usando config efêmera e MCP HTTP direto.

**Resultado exigido:** URL e auth de ambos provider/Nexus funcionam; nenhum segredo global/proxy.

**Evidência:** construir/mapear cenário causal próprio; build C28 está nos logs de artefato.
