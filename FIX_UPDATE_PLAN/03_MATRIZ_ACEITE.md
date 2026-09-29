# Matriz de aceite CN2


44 cenários: 20 executados (16 FAIL, três controles PASS, uma observação PASS) e 24 complementares NOT_RUN. São condições de aceite, não 44 testes novos já implementados. Nenhum NOT_RUN é alegação de outro bug.


## ACN2-01 — test_b01_saturation_must_not_block_interrupt

**Achado:** N02. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b01_saturation_must_not_block_interrupt`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-02 — test_observation_scheduler_admits_300_waiters

**Achado:** N02. **Estado:** PASS. **Camada:** observação.

**Reprodução:** `test_observation_scheduler_admits_300_waiters`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-03 — test_b02_queued_operation_revalidates_detached_lane

**Achado:** N01. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b02_queued_operation_revalidates_detached_lane`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-04 — test_b03_wire_namespace_matches_resolved_local_session[positive_same_server]

**Achado:** N01. **Estado:** PASS. **Camada:** controle.

**Reprodução:** `test_b03_wire_namespace_matches_resolved_local_session[positive_same_server]`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-05 — test_b03_wire_namespace_matches_resolved_local_session[foreign_server]

**Achado:** N01. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b03_wire_namespace_matches_resolved_local_session[foreign_server]`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-06 — test_b04_sparse_event_batch_returns_without_waiting_for_future_events

**Achado:** N03. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b04_sparse_event_batch_returns_without_waiting_for_future_events`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-07 — test_b05_retry_replays_unacknowledged_batch

**Achado:** N03. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b05_retry_replays_unacknowledged_batch`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-08 — test_b05b_future_ack_does_not_advance_core_watermark

**Achado:** N03. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b05b_future_ack_does_not_advance_core_watermark`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-09 — test_b05c_foreign_ack_not_associated_with_local_stream

**Achado:** N03. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b05c_foreign_ack_not_associated_with_local_stream`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-10 — test_b06_failed_reconciliation_does_not_mark_transport_ready

**Achado:** N04. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b06_failed_reconciliation_does_not_mark_transport_ready`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-11 — test_b07_stop_failure_before_effect_keeps_session_retryable[failed_receipt]

**Achado:** N05. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b07_stop_failure_before_effect_keeps_session_retryable[failed_receipt]`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-12 — test_b07_stop_failure_before_effect_keeps_session_retryable[core_exception]

**Achado:** N05. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b07_stop_failure_before_effect_keeps_session_retryable[core_exception]`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-13 — test_b08_approval_manager_reaches_public_core_api

**Achado:** N06. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b08_approval_manager_reaches_public_core_api`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-14 — test_b09_managed_mcp_start_configures_server_url_not_only_token

**Achado:** N06. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b09_managed_mcp_start_configures_server_url_not_only_token`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-15 — test_b10_reconcile_durable_receipt_does_not_require_current_binary

**Achado:** N04. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b10_reconcile_durable_receipt_does_not_require_current_binary`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-16 — test_b11_bootstrap_ticket_key_matches_selected_binding_agent

**Achado:** N07. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b11_bootstrap_ticket_key_matches_selected_binding_agent`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-17 — test_b11b_lane_added_after_ready_is_actually_attached

**Achado:** N07. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b11b_lane_added_after_ready_is_actually_attached`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-18 — test_b12_plaintext_wss_override_rejected_before_ticket_send

**Achado:** N08. **Estado:** FAIL. **Camada:** fault-injection/contrato.

**Reprodução:** `test_b12_plaintext_wss_override_rejected_before_ticket_send`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-19 — test_control_priority_queue_conserves_two_simultaneous_inputs

**Achado:** CONTROLE. **Estado:** PASS. **Camada:** controle.

**Reprodução:** `test_control_priority_queue_conserves_two_simultaneous_inputs`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-20 — test_control_invalid_agent_is_refused_at_receiver

**Achado:** CONTROLE. **Estado:** PASS. **Camada:** controle.

**Reprodução:** `test_control_invalid_agent_is_refused_at_receiver`

**Resultado:** A asserção do teste executável define a expectativa correta. Consulte relatório e XML para resultado observado.

**Evidência:** evidencias/cn2_final.xml


## ACN2-21 — Sessões com todos os IDs iguais em A/B

**Achado:** N01. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Criar duas sessões reais do Core em dois namespaces; dispatcher recebe ambas as origens.

**Resultado:** Cada frame alcança somente seu recurso, ou recusa anterior; não há ambiguidade resolvida por ordem.

**Evidência:** Não executado nesta auditoria.


## ACN2-22 — Revogação/rotação enquanto operação aguarda

**Achado:** N01. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Bloquear admissão, atualizar epoch/owner/configuração e liberar.

**Resultado:** Zero efeitos antigos; recibo conhecido permanece consultável.

**Evidência:** Não executado nesta auditoria.


## ACN2-23 — Teto explícito de itens e bytes

**Achado:** N02. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Configurar limites pequenos e inundar com payloads de tamanhos distintos.

**Resultado:** Admissão bounded antes de criar tasks; controle tem capacidade própria e rejeições são explícitas.

**Evidência:** Não executado nesta auditoria.


## ACN2-24 — Recibo após queda de socket

**Achado:** N02. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Aceitar operação, perder WSS antes da publicação e reconectar.

**Resultado:** Mesmo ID consultável, sem segundo efeito; resposta enfileirada não desaparece silenciosamente.

**Evidência:** Não executado nesta auditoria.


## ACN2-25 — Limites do lote 0/1/127/128/129

**Achado:** N03. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Preencher journal com cada quantidade e ler pelo adapter do aplicativo.

**Resultado:** Lotes finitos ordenados e sem espera por novos eventos; flush esparso ocorre.

**Evidência:** Não executado nesta auditoria.


## ACN2-26 — Reconexão sem novo evento

**Achado:** N03. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Gerar registros offline, reconectar sem publicar outra notificação.

**Resultado:** Replay do ACK conhecido inicia automaticamente e é limitado.

**Evidência:** Não executado nesta auditoria.


## ACN2-27 — ACK válido, duplicado e parcial

**Achado:** N03. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Enviar lote contíguo, confirmar prefixo válido e repetir confirmação.

**Resultado:** Monotonicidade/idempotência; replay do sufixo sem compactar além da prova.

**Evidência:** Não executado nesta auditoria.


## ACN2-28 — ACK estrangeiro contra journal real

**Achado:** N03. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Ter registros duráveis ainda não enviados, receber ACK de outra conexão/stream.

**Resultado:** Nenhuma atualização/compactação no Core por ACK inválido; validade no banco não substitui prova de envio.

**Evidência:** Não executado nesta auditoria.


## ACN2-29 — Prontidão após queue full/erro

**Achado:** N04. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Falhar projeção ou lotar fila inicial, depois recuperar.

**Resultado:** Não READY produtivo antes da evidência; recuperação real converge sem apagar história.

**Evidência:** Não executado nesta auditoria.


## ACN2-30 — Sessão viva em cache por sessão

**Achado:** N04. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Criar Core por binding+session e pedir snapshot via API de reconcile.

**Resultado:** Localiza dono real; não instancia Core vazio para substituir observação existente.

**Evidência:** Não executado nesta auditoria.


## ACN2-31 — Restart e intents duráveis

**Achado:** N04. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Reiniciar aplicativo após commit sem publicar recibo, com binário movido.

**Resultado:** Claims/recibos continuam consultáveis; ausência de handle não apaga operação nem reexecuta tarefa.

**Evidência:** Não executado nesta auditoria.


## ACN2-32 — POST aceito com resposta perdida

**Achado:** N04. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Peer registra intenção e perde resposta; cliente reinicia consulta.

**Resultado:** Mesmo identificador da intenção, sem duplicação; erro contém referência recuperável.

**Evidência:** Não executado nesta auditoria.


## ACN2-33 — Stop cancelado antes e depois do efeito

**Achado:** N05. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Reter HTTP/Core nos dois pontos, cancelar waiter e consultar novamente.

**Resultado:** Reserva segura liberada somente com prova; unknown mantém produtor e supervisor.

**Evidência:** Não executado nesta auditoria.


## ACN2-34 — Daemon com ownership incerto

**Achado:** N05. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Core retorna unknown e ainda tem recurso/commit ativo.

**Resultado:** Lifecycle público conserva recuperação ou apresenta transferência ao SO comprovada; não só exit code.

**Evidência:** Não executado nesta auditoria.


## ACN2-35 — Configuração MCP por duas sessões

**Achado:** N06. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Partir de homes/configs limpos, abrir duas sessões com URL/capabilities distintas.

**Resultado:** URL e token certos em cada processo; nenhum proxy, segredo global ou contaminação entre sessões.

**Evidência:** Não executado nesta auditoria.


## ACN2-36 — Extensão nativa Pi pelo aplicativo

**Achado:** N06. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Start Pi via Connector e invocar ação estruturada no peer canônico.

**Resultado:** Scope/autorização preservados e biblioteca Core única; teste de helper isolado não basta.

**Evidência:** Não executado nesta auditoria.


## ACN2-37 — Ciclo remoto completo em laboratório

**Achado:** N06. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Server peer manda open, turn, decision, interrupt, close/query sem CLI iniciar sessão.

**Resultado:** Todos os verbos anunciados operam por IDs/contrato; indisponível permanece explícito.

**Evidência:** Não executado nesta auditoria.


## ACN2-38 — Aprovação válida/antiga/negativa

**Achado:** N06. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Pedido real observado, decisão autorizada e mudança de turno/prazo sob espera.

**Resultado:** DTO correto; accept indevido bloqueado, decline permitido só no escopo correto; zero consumo pré-byte.

**Evidência:** Não executado nesta auditoria.


## ACN2-39 — Renovação e rotação por lane

**Achado:** N07. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Duas identidades conectadas; expire/remova/rotacione apenas A.

**Resultado:** Renova single-flight e fecha provas antigas de A sem afetar B; sem root key implícita.

**Evidência:** Não executado nesta auditoria.


## ACN2-40 — Inventário disponibilizado e atualizado

**Achado:** N09. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Criar candidatos conhecidos/unknown/sem contenção e alterar instalação.

**Resultado:** Avaliação Core projetada, revisão muda com evidência; Server aplica permissões sem recalcular política.

**Evidência:** Não executado nesta auditoria.


## ACN2-41 — Duas instalações byte-idênticas

**Achado:** N09. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Discovery real dos dois alvos e resolução por ref após reordenação.

**Resultado:** Escolha A/B inequívoca; versão/TTL/escopo de inventário validados; hashes de build permanecem iguais.

**Evidência:** Não executado nesta auditoria.


## ACN2-42 — WSS seguro e exceção local

**Achado:** N08. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Testar origem WSS aprovada, ws loopback autorizado e overrides/redirects hostis.

**Resultado:** TLS não contornável; nenhuma credencial sai antes de aprovar destino/escopo.

**Evidência:** Não executado nesta auditoria.


## ACN2-43 — Processos e autostart no SO alvo

**Achado:** N05. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Em ambiente qualificado, usar filho inofensivo, protocolo e prova de nascimento.

**Resultado:** Contenção/cleanup por owner; teste de peer sintético não recebe qualificação de SO.

**Evidência:** Não executado nesta auditoria.


## ACN2-44 — Integração local e remota real

**Achado:** N01/N06. **Estado:** NOT_RUN. **Camada:** cenário complementar.

**Preparação/ação:** Server+Core local e Server remoto+Connector com mesmo Core, sob autorização específica.

**Resultado:** Ferramentas diretas, eventos, decisões, partições e recibos comprovados; gate G2 só aqui.

**Evidência:** Não executado nesta auditoria.
