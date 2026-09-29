# Matriz de validação — Connector CN1

## Campanha executada

34 casos; 30 FAIL e 4 PASS (3 controles + 1 observação). As três campanhas adicionais exercitam os mesmos casos, não aumentam a contagem de cenários. Interfaces internas são acessadas somente pelas fixtures de inspeção/fault injection; aplicativos corrigidos devem usar contratos públicos.

| Teste | Baseline | Camada/uso | Achados |
|---|---|---|---|
| `test_01_qualified_candidate_architecture_survives_binding_roundtrip` | FAIL | regressão | A01 |
| `test_02_two_servers_same_binding_id_do_not_share_core_instance` | FAIL | regressão | A03 |
| `test_03_empty_authority_does_not_gain_fallback_permissions` | FAIL | regressão | A02 |
| `test_04_remote_context_never_extends_expired_authorization` | FAIL | regressão | A02 |
| `test_05_inbound_operations_require_exact_live_lane_scope[changes0]` | FAIL | regressão | A02 |
| `test_05_inbound_operations_require_exact_live_lane_scope[changes1]` | FAIL | regressão | A02 |
| `test_05_inbound_operations_require_exact_live_lane_scope[changes2]` | FAIL | regressão | A02 |
| `test_05_inbound_operations_require_exact_live_lane_scope[changes3]` | FAIL | regressão | A02 |
| `test_05_inbound_operations_require_exact_live_lane_scope[changes4]` | FAIL | regressão | A02 |
| `test_06_server_generation_is_adopted_after_welcome` | FAIL | regressão | A02 |
| `test_07_operation_cannot_enter_before_handshake_and_reconcile` | FAIL | regressão | A02 |
| `test_08_priority_queue_conserves_both_items_waking_together` | FAIL | regressão | A04 |
| `test_09_silent_websocket_is_detected_while_tasks_are_pending` | FAIL | regressão | A05 |
| `test_10_http_plaintext_nonloopback_rejected_before_secret_transmission` | FAIL | regressão | A06 |
| `test_11_read_timeout_after_mutating_request_is_not_safe_retry` | FAIL | regressão | A07 |
| `test_12_explicit_mcp_import_uses_exact_origin_not_prefix` | FAIL | regressão | A06 |
| `test_13_failed_stop_can_be_retried_and_not_marked_closed` | FAIL | regressão | A08 |
| `test_14_unknown_close_receipt_keeps_managed_session_recoverable` | FAIL | regressão | A08 |
| `test_15_shutdown_does_not_discard_core_ownership_on_unknown` | FAIL | regressão | A08 |
| `test_16_reconcile_response_is_valid_nxl_not_string_snapshots` | FAIL | regressão | A09 |
| `test_17_reconcile_passes_originating_namespace_not_first_binding` | FAIL | regressão | A09 |
| `test_18_state_store_save_public_method_works` | FAIL | regressão | A15 |
| `test_19_noninteractive_connect_does_not_prompt` | FAIL | regressão | A14 |
| `test_20_launchd_plist_uses_dictionary_and_boolean_types` | FAIL | regressão | A16 |
| `test_21_windows_autostart_preserves_custom_state_directory` | FAIL | regressão | A16 |
| `test_22_websocket_dispatch_does_not_ignore_wire_identity` | FAIL | regressão | A02 |
| `test_observation_offline_event_copy_retains_all_3000_events` | PASS | observação | A13 |
| `test_control_catalog_is_from_core` | PASS | controle positivo | A17 |
| `test_control_http_cross_origin_redirect_never_forwarded` | PASS | controle positivo |  |
| `test_24_select_second_installation_selects_that_exact_path` | FAIL | regressão | A14 |
| `test_25_bind_create_completes_identity_dto_construction` | FAIL | regressão | A14 |
| `test_26_blocked_submit_does_not_block_received_control` | FAIL | regressão | A05 |
| `test_27_start_session_capability_is_referenced_in_effective_environment` | FAIL | regressão | A10 |
| `test_control_core_rejects_ungranted_new_action` | PASS | controle positivo |  |

## Aceites complementares do plano

Os critérios abaixo são exigências a implementar/validar; o revisor não alega que eles já passaram. Cada tarefa pode ser demonstrada por mais de um teste, e um mesmo teste pode servir a mais de uma tarefa. Não somar essa lista como “43 testes novos”.

### CN-00.01 — Fixar entradas e preservar HEAD

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Registro reproduzível de baseline; nenhum arquivo do usuário removido; o commit avaliado e o commit corrigido ficam distintos.

Achados: A01, A17. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-00.02 — Reproduzir as campanhas antes de editar

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

XML, comando, versão e exit code salvos. Controles de autorização do Core, catálogo e redirect continuam passando. Asserções de zero efeito permanecem.

Achados: A01, A02, A04, A08. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-00.03 — Inventariar operações anunciadas e reais

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Cada capacidade anunciada tem rota e teste; cada ausência é BLOCKED/UNSUPPORTED com diagnóstico. Plano original C00–C10 não fica globalmente DONE por conveniência.

Achados: A02, A09, A10, A11. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-00.04 — Definir contrato entre os três agentes

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Documento de handoff com owners, versão exata e dúvidas de contrato resolvidas antes dos respectivos efeitos; sem implementar outro MCP ou outra inbox.

Achados: A17, A11. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-01.01 — Atualizar dependência e testar por wheel

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Mesmo artefato nos consumidores; nenhuma importação privada de factory/provider; instalação e -I imports funcionam fora da fonte; matriz de versões declarada.

Achados: A17. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-01.02 — Preservar candidato completo na persistência

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Teste01 e evidência de qualificação com arquitetura passam. Migração não muda agent_id/chave/histórico e não converte UNKNOWN em READY.

Achados: A01. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-01.03 — Usar projeção técnica do Core e publicar inventário seguro

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Server local usa fatos locais; Server remoto recebe fatos do Connector. Plataforma incompatível/build desconhecido/attach não qualificado não habilitam binding.

Achados: A17. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-01.04 — Resolver a seleção exata e manter aliases de apresentação

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Teste24 seleciona B; reordenar inventário não altera escolha. Referência legada ambígua exige reseleção explícita; não rotacionar chave ou trocar runtime silenciosamente.

Achados: A14, A17. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-01.05 — Coordenar rebind, drift e cache do host

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Atualização de binário aprovada é efetiva; mudança sem aprovação falha. Duas sessões recebem seus próprios dados e um binding de outro Server não reutiliza instância.

Achados: A01, A03, A14. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-02.01 — Introduzir chaves de escopo completas

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Teste02 e variantes com mesmos IDs em A/B passam. Remover/rotacionar/acknowledge uma instalação não toca as demais.

Achados: A03, A09. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-02.02 — Separar socket de autorização para operação

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Testes05/06/07 passam; welcome válido adota a geração que o Server autorizou; contador local de tentativas não vira autoridade; fila cheia não marca attached.

Achados: A02, A12. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-02.03 — Construir ExecutionContext somente de evidência autenticada

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Testes03/04/22 passam; frame errado gera zero send_turn com Core real e peer. Controle da ação originalmente negada continua recusado; accept/input não herdam exceção temporal de interrupt/deny.

Achados: A02. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-02.04 — Renovar e revogar lanes corretamente

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Vários agentes, expiração, revogação seletiva, credencial trocada e fila urgente saturada são exercitados. Nenhuma chave de A solicita ticket de B por ordem da lista.

Achados: A12, A02. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-02.05 — Tratar NXL por operação sem descartar metadados

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Remote close recebe e devolve o mesmo ID; replay idêntico é consulta, não novo efeito; CoreError conhecido vira frame válido e não derruba todo o receiver.

Achados: A02, A11. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-03.01 — Aplicar origem/TLS antes de segredos

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Testes10/12 e redirect control passam; nenhuma requisição autenticada é entregue para HTTP remoto, host por prefixo ou redirect cruzado. Casos de porta/default port/userinfo testados.

Achados: A06. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-03.02 — Corrigir fila sem perda e tratar backpressure

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Teste08 entrega urgent e normal exatamente uma vez. Cancelamento e dois puts simultâneos não perdem dados. Flood normal não bloqueia interrupt/revoke.

Achados: A04. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-03.03 — Desacoplar reader e scheduler de execução

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Teste26 recebe interrupt enquanto submit está preso; ACK/heartbeat continuam processados; resultados fora de ordem mantêm correlação; fechar socket não duplica trabalho.

Achados: A05. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-03.04 — Implementar watchdog e limites de estado de conexão

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Teste09 passa com tasks ainda pendentes e tráfego saudável renova só indicador de atividade. Com socket vivo mas peer aplicativo parado, o estado não permanece READY indefinidamente.

Achados: A05, A12. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-03.05 — Remover cópia ilimitada de eventos e validar ACK

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Carga offline cresce no journal limitado, não proporcionalmente em listas Python. ACK futuro/antigo/cruzado não compacta dados; reconnect drena do cursor; resource controls progridem sob flood.

Achados: A13. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-04.01 — Classificar incerteza por estágio do pedido

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Teste11 preserva unknown após transporte receber POST. Controle connect failure comprovadamente anterior continua seguro; nenhuma orientação de retry com ID novo quando efeito é possível.

Achados: A07. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-04.02 — Unificar autoria da operação local/remota

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Peer de Server conta uma execução por intenção sob sucesso, ACK perdido e retries. Resultado local persiste e é consultável mesmo se HTTP subsequente falhar.

Achados: A07, A11. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-04.03 — Serializar reconciliação com schema real

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Testes16/17 passam; relatórios não vazios, sessão antiga e dois Servers com mesmos IDs geram frames corretos e segregados.

Achados: A09. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-04.04 — Consultar journal na ausência de runtime em memória

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Daemon reinicia após commit local e antes de publicar receipt: responde sobre a mesma operação sem segundo spawn/turn. Vazio é usado somente se realmente não há evidência no namespace solicitado.

Achados: A09. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-04.05 — Reativar pumps e conservar watermarks com limites

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Erro temporário do sender não abandona permanentemente o stream; replay/ACK duplicado preserva sequência e idempotência; remover um Server não cancela pumps de outro.

Achados: A13, A09. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-05.01 — Bloquear novas admissões no início do drain

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Solicitar shutdown e depois start/submit produz recusa anterior ao efeito; abertura já admitida conserva resultado/ownership até resolução.

Achados: A08. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-05.02 — Separar solicitação de stop e resultado físico

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Testes13/14 passam; segundo stop não fica preso em already_closing falso. Timeout depois do efeito não é seguro para repetir a operação com ID novo.

Achados: A08. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-05.03 — Unificar encerramento das duas camadas de host

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Teste15 passa. Com um Core unknown e outro resolvido, relatório conserva ambos corretamente e o segundo não é atrasado indevidamente pelo primeiro.

Achados: A08. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-05.04 — Relatar graceful, forced e pending com fatos

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Não afirmar morte sem observação nem persistência sem commit; CLI e log expõem pendência recuperável; segundo lifecycle público consegue concluir sem imports privados.

Achados: A08. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-05.05 — Inicialização e disposal single-flight

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Dois starts compartilham exatamente o store pretendido, não workers órfãos. Cancelamento do waiter não destrói initialization/commit em voo. Recursos resolvidos são fechados por API pública.

Achados: A08, A03. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-06.01 — Instalar configuração de cliente MCP HTTP por sessão

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Teste27 passa; peer de processo observa URL/credencial de sessão esperada. O tráfego de ferramenta vai diretamente harness→Server; tools-only funciona sem daemon.

Achados: A10. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-06.02 — Resolver credenciais por sessão, inclusive reuso do binding

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Duas sessões sucessivas e concorrentes usam tokens diferentes; nenhum token da primeira aparece na segunda; config/prompt/logs não expõem o segredo global.

Achados: A10, A03. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-06.03 — Compor bridge Pi nativa no daemon

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Teste exercita import da aplicação Connector, start Pi sintético e ação passando pelo backend HTTP controlado. Testar biblioteca Core isolada não encerra essa tarefa. Não há endpoint MCP.

Achados: A10. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-06.04 — Implementar matriz remota declarada

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Server peer controla ciclo inteiro pelo WSS sem CLI iniciar previamente a sessão. Falta de capacidade retorna erro antes do efeito; remote close conserva ID e receipt.

Achados: A11. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-06.05 — Aprovação humana e nativa com estado bifásico

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Decisão inválida não consome pedido. ACK perdido mantém consulta sem segunda resposta. Accept expirado é negado; decline seguro de pedido vigente segue contrato. Outro agente/request_id em outro Server não substitui o pedido.

Achados: A11, A02. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-07.01 — Corrigir bind create e uso de identidade importada

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Teste25 passa e criação usa a identidade selecionada. Não criar usuário ou novo agente para contornar a falha.

Achados: A14. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-07.02 — Respeitar headless e consentimento

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Teste19 passa sem prompt; configuração incompleta retorna código estável; execução aprovada usa mesmos escopos que modo humano.

Achados: A14. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-07.03 — Consolidar writer do estado

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Teste18, falha antes do replace, update concorrente e permissões passam; state corrupto é diagnosticado sem sobrescrever automaticamente.

Achados: A15. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-07.04 — Gerar serviços corretos no root aprovado

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Testes20/21 passam; em cada SO-alvo serviço inicia o mesmo root e cofre. Não marcar boot/logout PASS apenas por gerar texto.

Achados: A16. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-07.05 — Verificar ownership de configuração e remoções

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Dois arquivos MCP existentes e duas identidades: apply/remove/rebind de A preserva B e terceiros. Falha intermediária não apaga histórico ou chave.

Achados: A03, A10, A14. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-08.01 — Executar regressões e testes derivados dos achados

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Cada achado tem test node/camada/resultado; 34 testes recebidos não substituem cenários ausentes; nenhum PASS por xfail, permissões inventadas ou desativação de preflight.

Achados: A01, A02, A03, A04, A05, A06, A07, A08, A09, A10, A11, A12, A13, A14, A15, A16, A17. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-08.02 — Qualificar pacote fora da árvore

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Hash e versão exatos registrados; Core e Connector não dependem da árvore-fonte nem pacote Server. Versões incompatíveis falham cedo com diagnóstico.

Achados: A17, A16. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-08.03 — Executar um caminho integrado real antes de ampliar matriz

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

IDs e recibos preservados; Server não valida paths remotos localmente; ferramentas MCP HTTP diretas; nenhum segundo efeito depois de resposta perdida. Isso é gate integrado, não teste do Core sozinho.

Achados: A01, A02, A09, A10, A11. Evidência futura: commit, node, camada, comando, saída e artefato.

### CN-08.04 — Revisar status e entregar responsabilidades remanescentes

Estado nesta auditoria: **NOT_RUN no escopo completo desse aceite**.

Nenhuma declaração global DONE sobre funções sem wiring. Pendência externa de Server distingue-se de defeito interno do Connector. Desenvolvimento paralelo continua com contrato fixado; liberação limitada ao escopo realmente comprovado.

Achados: A17. Evidência futura: commit, node, camada, comando, saída e artefato.

