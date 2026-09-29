# Reavaliação do Nexus Connector — CN2

## Decisão

A implementação evoluiu, mas CN1 **não está integralmente concluído**. O pacote é utilizável como base de desenvolvimento e integração experimental com pin exato; não recomendo declarar G1 (Connector corrigido no escopo de laboratório), nem execução remota governada pronta. Confirmamos oito grupos de falhas de execução/contrato em 16 casos de teste e uma lacuna funcional de integração do inventário. Não há recomendação de reescrever Core/Connector nem de mudar o transporte aprovado.

**Connector examinado:** `28d147625c2a47417ad70f72d3b453cd8bd2e984`, versão `0.1.0.dev0`. ZIP: `okto-nexus-connector-main(2).zip`.

**Dependência declarada e utilizada:** `nexus-connector-core==0.2.10.dev0`, código de pacote do commit `1560d314ed2b478515dcbbe533436d7d0b027b09`. Python 3.13.5, Linux. Bibliotecas e caminhos efetivos constam em `evidencias/environment.json`.

O Core conserva os adaptadores e a autoridade canônica continua no Server. MCP é HTTP direto harness→Server. Nenhum achado autoriza introduzir MCP stdio, proxy, outra inbox, usuário Nexus ou cópia de adaptadores.

## 1. Método, proveniência e limites

O ZIP foi extraído e seus 118 arquivos originais foram inventariados antes da execução. Foi comparado com o snapshot anterior `8bc5025` e com o plano CN1. Executei código do Connector tanto instalado fora da árvore-fonte quanto na árvore atual, mantendo a mesma versão exata do Core.

O wheel `0.2.10.dev0` produzido pelo executor não estava anexado. Para não testar outro Core, obtive pelo conector GitHub o commit exato e seu delta em relação ao Core `0.2.9` já fornecido. As seis mudanças de código/empacotamento (`__init__.py`, `models.py`, `discovery.py`, `availability.py`, `installation.py`, `pyproject.toml`) foram materializadas e verificadas pelos hashes Git dos blobs. Os demais arquivos do pacote são os do predecessor, inalterados naquele commit. Construí e instalei localmente esse código. **Isto não significa que testei o mesmo hash binário do wheel do executor**, nem que auditei integralmente a nova versão do Core.

`rfc8785==0.1.4` é a implementação oficial completa, verificada por hash de blob; não foi substituída por serializador aproximado. Nenhum gate de contenção do produto foi removido. Os testes que injetam uma factory nativa usam a porta pública de testes para criar peers controlados, não qualificam providers.

Não foram executados providers reais, um Nexus Server real, rede multi-host, autostart Windows/macOS, reboot ou campanhas WSL2. Não foram usados segredos reais. Os testes WSS usam frames do schema real e estados de transporte de laboratório; não demonstram uma exploração remota anônima. O teste de cruzamento de Server atravessa codec, receiver, dispatcher, manager, kernel/journal/Core reais, tendo como fronteira final um peer nativo.

Os testes de empacotamento originais, que dependem de clone irmão/downloads, foram excluídos da suíte principal. Construção e instalação foram avaliadas em campanha separada, sem afirmar instalação online em máquina limpa.

## 2. Resultados

| Campanha | PASS | FAIL | SKIP/observação |
|---|---:|---:|---|
| Suíte entregue, exceto `tests/e2e/test_packaging.py` | 144 | 9 | 4 SKIP |
| Subconjunto de sementes originais CN1, sem alteração | 14 | 0 | 20 deselected |
| CN2, pacote instalado | 4 | 16 | Dos 4 PASS, três controles e uma observação de carga |
| CN2 repetido com a fonte atual | 4 | 16 | Mesmo resultado |
| Build wheel/sdist | PASS | — | Construção local, sem publicação |
| Importação isolada e catálogo do Core | PASS | — | Core 0.2.10, catálogo v1 e disponibilidade v2 |

Os subconjuntos não devem ser somados à suíte completa. As 16 falhas são parametrizações de oito grupos, não 16 causas independentes. A observação de 300 tarefas não impõe um limite artificial de produção: mede o comportamento atual; a ausência de admissão limitada é comprovada também pela inspeção do receiver.

### Falhas de ambiente e fixtures

Seis falhas da suíte entregue são os cenários CLI/daemon que usam executáveis sintéticos sem permissão de execução no Linux. Outra exige inventário Pi vazio em plataforma na qual o resolvedor passivo agora retorna a instalação da fixture. Duas sementes reconstruídas de seleção/bind acionam probe ativo e esbarram na ausência de `proc_children` neste sandbox. Não classifiquei esses nove resultados como os defeitos CN2.

Os testes independentes abaixo não dependem de desligar a contenção. Suas falhas repetidas são distintas dessas limitações.

## 3. Delta em relação a A01–A17 / CN1

| Item anterior | Situação nesta revisão |
|---|---|
| A01, arquitetura do candidato | Corrigido no round-trip selecionado: arquitetura e evidências agora persistem. |
| A02, autoridade | Parcial: há validação de lane no receiver e não há fallback de ações vazias; escopo ainda se perde no serviço e não é revalidado após fila. N01. |
| A03, namespaces | Parcial: cache por Server e registro por SessionKey melhoraram; o despacho ainda resolve pelo ID textual. N01/N04. |
| A04, perda na fila dupla | Cenário original corrigido. O getter preserva as duas mensagens. Limites/admissão de trabalho são N02. |
| A05, responsividade | Watchdog independente entregue; um submit não bloqueia diretamente o reader. Quatro submits ocupam todos os slots inclusive de controles. N02. |
| A06, origem/TLS | HTTPS remoto e importação por origem exata corrigidos nos testes. Override WSS ainda admite `ws://` remoto. N08. |
| A07, incerteza HTTP | ReadTimeout de mutação corretamente sinaliza possível efeito no controle original. Persistência prévia de intenção/recovery da resposta perdida continua exigindo integração com Server, não demonstrada por esse teste. |
| A08, lifecycle | `OUTCOME_UNKNOWN` fica gerenciado e host conserva Core desconhecido. `FAILED` e `CoreError` ainda tratados incorretamente. N05. |
| A09, reconcile | Projeção tipada e passagem de namespace foram corrigidas em parte. Leitura histórica depende indevidamente do binário atual; falha de reconcile é convertida em READY. N04. |
| A10, ferramentas | Token por sessão chega ao ambiente; URL/configuração MCP não chega ao lançamento limpo. Bridge Pi não está composta no caminho gerenciado. N06. |
| A11, verbos remotos | Steer/turno esperado e close com ID remoto avançaram. Remote open continua explicitamente indisponível; decisão nativa contém TypeError e DTO invertido. N06. |
| A12, lanes | Estado pending/attaching/ready entregue. Adição durante conexão, seleção de identidade do ticket e renovação completa ainda pendentes. N07. |
| A13, eventos | RAM já não acumula toda cópia de eventos. Leitura finita, replay e ACK estão incorretos. N03. |
| A14, CLI | Uso do DTO e seleção por executável corrigidos em código; casos de probe nesta plataforma ficam bloqueados, não comprovam defeito adicional. |
| A15, save | Corrigido; semente original de round-trip passou. |
| A16, serviços | Correções de tipos do plist e diretório de estado passaram nos testes de artefato. Boot/logout real não foi qualificado. |
| A17, evolução do Core | Pin 0.2.10 e identidade da instalação presentes. Avaliação pública e publicação do inventário não estão ligadas no Connector. N09. |

## 4. N01 — Escopo autenticado não acompanha a operação até a sessão

**P1 — bloqueador de isolamento.** Vinculado a A02/A03 e CN-02.01/03/05.

**Âncoras:** `daemon/app.py:314–369`; `services/runtime_service.py:572–690`; `transport/wss_client.py:382–448,558–592`.

O receiver valida Server/executor/lane/agente/generation. Depois, o dispatcher transforma o frame em argumentos que não incluem esse namespace. `submit_remote`, `steer_remote`, `interrupt_remote` e `close_remote` chamam `session(session_id)` e comparam apenas `binding_id`. O manager reconstrói contexto usando a sessão que encontrou.

**Reprodução principal:** uma sessão do Core A já existe. Um transporte configurado como negociado para B recebe um frame sintaticamente válido, com lane e agente B, ID de binding igual e ID textual da sessão A. A validação do receiver para B passa. O serviço encontra A, fabrica um contexto de A e o Core A corretamente aceita esse contexto — que não deveria ter sido usado para o frame B. O peer registra `send_turn(op_wire)`. O controle do mesmo Server também executa, como esperado.

Isto não é falha da verificação de identidade do Core: o aplicativo entregou a ele a identidade errada como se fosse autoridade do host. Não houve transferência de credencial real ou execução de provider.

**Segunda reprodução:** quatro operações ficam executando. A quinta passa pela validação e espera semáforo. A lane é detached antes de ela adquirir capacidade. Ao liberar as quatro, a quinta chega ao handler. `_run_operation_guarded` existe, mas o receiver não o utiliza; mesmo uma validação antes de entrar no semáforo seria cedo demais.

**Correção:** conservar uma operação validada com origem da conexão, SessionKey, BindingKey, gerações, revisões, grant e intenção. Resolver somente no namespace autenticado e comparar a sessão efetiva. Revalidar a reserva de autoridade após cada espera de admissão, imediatamente antes da chamada pública do Core. Nunca remapear para outra sessão por unicidade incidental do texto. Separar a exceção temporal de contenção de concessão de novas capabilities; `_authorized_context` não deve ampliar `allowed_actions` por um booleano.

**Testes:** `test_b02_queued_operation_revalidates_detached_lane`; `test_b03_wire_namespace_matches_resolved_local_session[foreign_server]`; controle `[positive_same_server]`.

## 5. N02 — Semáforo único bloqueia interrupt sob saturação

**P1 — controle operacional.** A05, CN-03.02/03/04.

**Âncora:** `transport/wss_client.py:382–448`.

Todas as `operation.submit`, inclusive `turn.interrupt` e `runtime.close`, executam em `_op_semaphore` de quatro slots. O reader não espera mais pelo handler, mas o controle continua esperando pelo mesmo recurso das operações que precisa interromper.

**Reprodução:** quatro submits ocupam os quatro slots e mantêm barreira fechada. Um interrupt válido chega ao reader. O contador de frames avança, mas o handler do interrupt não inicia até liberar os submits. A margem do teste é apenas uma janela de observação de laboratório, não SLO proposto.

**Limite adicional medido:** 300 frames válidos criaram 300 tasks em `_inflight`. O semáforo limita os corpos executando, não a quantidade de operações aceitas/retidas. Não houve OOM; a inspeção demonstra ausência de checagem de capacidade antes de `create_task`.

**Correção:** classes de trabalho e capacidade reservada de controle, com filas/admissão limitadas por itens e bytes, fairness por lane e resposta explícita de backpressure. Não aumentar o semáforo nem criar outra tarefa ilimitada por controle. Preservar o getter de fila já corrigido. Retorno `False` de enfileiramento de recibo precisa manter uma obrigação recuperável, não desaparecer.

**Teste:** `test_b01_saturation_must_not_block_interrupt`. Observação: `test_observation_scheduler_admits_300_waiters`.

## 6. N03 — Fluxo de eventos não é um replay confiável

**P1 — evidência durável e observabilidade.** A13, CN-03.05/CN-04.05.

**Âncoras:** `daemon/app.py:42–125,435–459`; `transport/wss_client.py:245–270,477–516`. Core 0.2.10: `RuntimeCore.events` é um iterador de acompanhamento, não uma página finita.

### N03.1 — Lote esparso espera o futuro

`_journal_batch` consome `runtime.events(cursor)` até acumular 128 eventos ou o iterador terminar. O iterador permanece acompanhando o stream. Com um evento já persistido no SQLite real, a função não retorna esse evento enquanto aguarda os demais. Ao fim de um lote cheio, a próxima tentativa sem eventos também pode permanecer pendurada.

**Correção:** ler uma página finita do port público de journal do Core, ou manter consumidor contínuo proprietário que faça flush limitado por tamanho/latência. Não implementar SQL paralelo ou outra inbox. A página deve poder devolver um único evento, e zero deve significar fim do snapshot, não esperar um novo evento para liberar o caller.

### N03.2 — Cursor SENT usado como ACKED

Após `send_events` aceitar o lote, `_sent_through` avança. Quando não chega ACK, a leitura seguinte inicia depois de `_sent_through`, não do ACK durável. No teste, as leituras foram `[0,1]`, mas a sequência 1 foi enviada apenas uma vez e nunca confirmada. O journal não foi apagado; a rota de publicação deixou de reapresentar o registro na retomada.

`send_events` atualmente confirma entrada na fila, não necessariamente escrita no socket. Enqueued, escrito na conexão e ACK durável precisam de fatos separados. A reconexão também deve despertar streams pendentes sem depender de nova atividade do harness.

### N03.3 — ACK sem limite ou namespace válido

O EventBridge encaminhou watermark 999 ao callback do Core embora só tivesse publicado a sequência 1. Separadamente, um `event.ack` com Server/executor estrangeiros, válido pelo schema NXL real, foi associado ao stream local por `(session_id, epoch)`.

Não demonstrei exclusão real de registros nesse teste: o callback mostra a aceitação indevida na fronteira do host. O Core trata o host como responsável por só confirmar ingresso durável real. Não transferir essa responsabilidade aos testes de SQLite.

**Correção conjunta:** chave completa de stream; active connection/generation como contexto de transporte (o frame r3 não permite campo extra de geração); watermark monotônico contíguo, limitado ao que foi realmente escrito para aquele stream; entrada desconhecida/futura/estrangeira não avança Core ACK. Retransmitir eventos não confirmados é permitido e deduplicado pelo Server; não significa reenviar a tarefa do agente.

**Testes:** `test_b04_sparse_event_batch_returns_without_waiting_for_future_events`, `test_b05_retry_replays_unacknowledged_batch`, `test_b05b_future_ack_does_not_advance_core_watermark`, `test_b05c_foreign_ack_not_associated_with_local_stream`.

## 7. N04 — Reconciliação pode falhar e ainda liberar READY

**P1 — admissão/recovery.** A09, CN-02.02/CN-04.03/04.

**Âncoras:** `wss_client.py:517–541,593–616`; `runtime_service.py:692–738`; `core_host.py:262–315`.

`_send_initial_reconcile` captura erro do callback, deixa um relatório vazio e continua. `_complete_handshake` marca `_reconciled=True`/READY logo após enfileirar, sem garantir a entrega do relatório. A reprodução injeta uma falha de journal no callback e observa `online=True`.

Há outra falha concreta: o recovery sem runtime em memória chama `host.build`, que exige um executável atual válido. Persisti um recibo no SQLite, removi somente o executável e consultei a operação pelo namespace correto: recebi `BINARY_NOT_FOUND`, sem acesso ao recibo que continua durável.

Além disso, a construção agora cria runtimes por sessão, mas `reconcile` procura o cache por binding. Essa divergência merece o cenário complementar de sessão viva versus consulta histórica: nunca usar um runtime novo sem handle para negar a existência da sessão ainda gerenciada. O startup envia listas vazias; inventariar pendências/claims requer um fluxo próprio e limitado, não supor que consulta vazia enumera tudo.

**Correção:** separar dados históricos de aptidão para iniciar processos. Journal disponível deve responder a recibos/fences mesmo se o binário foi removido. Ausência de handle significa ownership desconhecido, não operação inexistente. A reconciliação precisa de estado `failed/pending/complete` e confirmação no ponto acordado do protocolo; erro não vira snapshot vazio com sucesso. Receipts, snapshots e requests devem conservar namespaces em todas as rotas.

**Testes:** `test_b06_failed_reconciliation_does_not_mark_transport_ready`, `test_b10_reconcile_durable_receipt_does_not_require_current_binary`.

## 8. N05 — Stop só diferencia OUTCOME_UNKNOWN de todo o resto

**P1 para lifecycle declarado.** A08, CN-05.

**Âncoras:** `runtime_service.py:434–525,620–650`; `daemon/app.py:155–210`.

A melhoria conserva `OUTCOME_UNKNOWN`. Entretanto, qualquer outro `receipt.stage` segue para limpeza. Um recibo `FAILED`, com `possible_effect=False` e `retry_safe=True`, removeu a sessão do gerenciamento apesar de representar falha antes do efeito de fechamento. Não se demonstrou processo real órfão: foi uma reprodução da máquina de estados com resposta tipada do contrato.

O segundo teste fez `Core.close` lançar `CoreError(JOURNAL_FULL, retry_safe=True)` antes de qualquer efeito. O bloco captura somente ConnectorError, deixando StopAttempt em `closing`. A próxima chamada interpreta isso como encerramento ainda em andamento, embora não exista produtor correspondente.

**Correção:** classificar resultado tipado de Core/HTTP/recibo e estado físico separadamente. Recusa sem efeito libera só a reserva de stop correspondente; unknown conserva produtor e supervisor; sucesso de uma operação só libera gerenciamento conforme o contrato físico/durável do Core. Tratar CoreError e cancelamento sem inferir que todo Exception prova nada executado. A mesma regra vale para close remoto.

**Pendência estática correlata:** o daemon fecha IPC/transporte e retorna mesmo quando conservou instâncias unknown; um exit code não mantém o loop Python vivo. CN1 permite saída somente se existir transferência comprovada ao backend/guardian e recovery. Isso precisa ser explicitado e qualificado, não suposto pela presença de uma referência que morre junto ao processo.

**Testes:** `test_b07_stop_failure_before_effect_keeps_session_retryable[failed_receipt]` e `[core_exception]`.

## 9. N06 — Tokens e métodos isolados não completam ferramentas/controles

**P1 funcional para o produto gerenciado; não evidência de escalada.** A10/A11, CN-06.

### N06.1 — A URL MCP não chega ao lançamento limpo

O `start` agora solicita capability, constrói um template e usa `child_environment`. O teste confirmou que o token sintético aparece no ambiente: essa parte foi corrigida. Porém, a URL MCP/configuração não é aplicada ao argv, a um arquivo de configuração do harness ou ao ambiente apropriado. O template é usado para resolver o bearer, mas não é renderizado/instalado no caminho gerenciado. O helper de configuração persistente existe separadamente.

Percorri start e o preparador/ambiente reais do Core com um peer na abertura. O comando final tinha apenas `tool app-server`; o ambiente tinha PATH e a variável do token. Nenhuma URL `https://nexus.example/mcp` chegou ao lançamento. Cenário: projeto e sessão limpos, sem configuração manual herdada. Não houve tentativa contra serviço MCP real.

**Correção:** consumir o template completo no mecanismo realmente suportado pelo harness. Preferir configuração efêmera por sessão; caso arquivo persistente seja necessário, diff/backup/CAS e ownership. Token em variável sem cliente apontando à URL não atende. Não criar proxy MCP para suprir a configuração.

### N06.2 — Método de aprovação não alcança o Core

`RuntimeManager.decide_native_approval` chama `_authorized_context(session)` sem o argumento `action`, produzindo TypeError. O teste passou todos os argumentos públicos, inclusive `response=None`. Há também ordem incorreta dos campos de `NativeApprovalOperation`: request e decision estão trocados na construção posicional. Corrigir o primeiro erro sem o segundo apenas desloca a falha.

Usar argumentos nomeados, derivar ação/decisão validada e obter grant do mecanismo autorizado do Server. Exceção temporal de decline não elimina identidade/capability/correlação. Conectar a rota inteira de decisão nativa, não só um método que o daemon não chama.

### N06.3 — Escopo explicitamente pendente

Remote `runtime.open` continua retornando CAPABILITY_UNSUPPORTED de forma deliberada. Isso é mais honesto que um stub de sucesso, mas continua sendo funcionalidade central não entregue. A indisponibilidade do Server real pode bloquear a qualificação vertical, não prova que o lado Connector está implementado.

`CoreRuntimeHost.build` também não fornece `pi_native_action`, `codex_resume` ou habilitação/callback de approvals à composição. Os modos que dependem dessas capacidades precisam de wiring público ou permanecer declaradamente pendentes. Não habilitar attach não qualificado.

**Testes:** `test_b08_approval_manager_reaches_public_core_api`; `test_b09_managed_mcp_start_configures_server_url_not_only_token`.

## 10. N07 — Ciclo das lanes não acompanha o estado local

**P2 — disponibilidade/isolamento de credenciais.** A12, CN-02.04.

**Âncoras:** `daemon/app.py:242–311`; `wss_client.py:212–227,616–650`.

Duas reproduções:

1. A primeira identidade importada é A, mas a primeira lane pertence a B. O bootstrap escolhe identidade e binding separadamente, pedindo o ticket de B com a chave A. O peer HTTP de laboratório registra `('key_a','bind_a_de_B')`. O Server correto deve negar; não foi comprovado acesso indevido ou vazamento de segredo para outro Server.
2. Adicionar lane a um transporte já READY somente a coloca no dicionário. Nenhum attach é agendado. `reload_state` chama essa mesma entrada, sem completar o attach. A nova lane depende de reconectar o socket inteiro.

Expiração retornada pelo endpoint de ticket é descartada, `expires_at` não participa de um renovador completo, e o reload de binding já existente não aplica epoch/revisão atualizadas. São pendências estáticas do mesmo ciclo, não contadas como testes de expiração real executados.

**Correção:** escolher primeiro um binding e resolver exatamente sua identidade; uma lane tem ticket/epoch/revisão/expiração e attach em voo próprios. Reload deve aplicar delta de adição, remoção e rotação; renovação single-flight escopada não pode ampliar autoridade ou matar lanes independentes.

**Testes:** `test_b11_bootstrap_ticket_key_matches_selected_binding_agent`; `test_b11b_lane_added_after_ready_is_actually_attached`.

## 11. N08 — Exceção de URL do link contorna a exigência WSS

**P1 — proteção do transporte autenticado.** A06, CN-03.01.

**Âncoras:** `daemon/app.py:217–230`; `wss_client.py:274–290`.

A origem de gerenciamento agora exige HTTPS remoto, mas `link_url_override` vai diretamente para NXLTransport, que encaminha a URL e Authorization ao cliente WebSocket sem validar WSS. O teste forneceu `ws://remote.example/link` e registrou o bearer sintético entregue ao construtor de conexão. A sentinela abortou antes de rede: nenhum segredo real foi transmitido.

**Correção:** validar scheme/host/porta/userinfo/origem aprovada antes até da obtenção do ticket. WSS fora de loopback; exceção local somente documentada/aprovada. Se controle WSS tiver origem distinta de HTTPS por desenho, ela precisa de aprovação explícita com escopo/audience, não de qualquer override textual. TLS e redirect devem ser testados com a versão de websockets utilizada.

**Teste:** `test_b12_plaintext_wss_override_rejected_before_ticket_send`.

## 12. N09 — Core disponibiliza avaliação, Connector ainda não a utiliza

**P2 funcional — ligação do seletor remoto.** A17, CN-01.03 e evolução C9–C11 do Core.

O Core 0.2.10 efetivamente exporta catálogo, avaliação e resolução de instalação; o wheel instalado foi consultado. A referência de instalação e arquitetura agora são conservadas pelo Connector, o que deve ser preservado.

Entretanto, a árvore `src/okto_nexus_connector` não chama `evaluate_runtime_availability`, não constrói/consome `AvailabilityReport`, e não publica uma projeção técnica versionada por executor. `resolve_installation` é importado, mas não usado. `inventory_revision=(state.schema_version<<8)|1` não representa a evolução do conteúdo do inventário.

Não fabrico um teste exigindo outro nome de API: essa é a ausência de uma chamada/fluxo já especificado. O documento do executor afirma que a API de disponibilidade foi consumida, mas o caminho não existe no código analisado.

**Correção:** discovery → avaliação pública Core → snapshot seguro do executor → Server aplica políticas e oferece opções. Resolver a seleção pela referência do inventário correto com revisão/TTL reais, sem caminhos desnecessários no wire. Duas cópias iguais continuam duas instalações. Catálogo não é prontidão; prontidão técnica não é autorização. Aplicativos não devem copiar os qualificadores nem a lista de runtimes.

## 13. Evidência do executor e definição de pronto

`plans/correction-connector-01/evidence/cn1-correction-2026-09-28.md` declara todos os achados P1/P2 corrigidos. Essa conclusão é incompatível com os resultados CN2. O mesmo arquivo afirma que o teste de managed MCP foi ignorado por limitação do peer, mas que a integração cobre o fluxo; o novo teste demonstra que o token é transmitido sem a URL de configuração.

As diferenças de evidência importam: um teste com um submit não cobre saturação dos quatro slots; resolver um ID único não cobre o mesmo texto em dois Servers; retirar a cópia em RAM não prova replay correto; capturar ConnectorError não trata CoreError; API do Core disponível não significa API consumida pelo daemon.

Corrigir status por requisito, preservando XML histórico. Uma falha ambiental não se transforma em bug, e um SKIP não encerra implementação. Remote open pode ficar BLOCKED por contrato externo, mas o projeto não deve receber G1 para esse ciclo anunciado sem implementação e provas pertinentes.

## 14. Recomendação prática

Conservar o trabalho válido e executar CN2. Prioridade: escopo fim a fim, controle sob saturação, eventos/recovery e TLS do link. Em paralelo: stop, ferramentas, lanes e inventário. A integração com Server/Core continua como desenvolvimento, sem contornos locais que copiem adaptadores ou criem outro MCP.

O aceite de laboratório precisa atravessar o aplicativo inteiro com peers que imponham o schema e contem efeitos. Depois, com autorização específica, executar um adaptador real: Server remoto sem providers/projetos, Connector com o mesmo Core, abertura remota, tarefa, stream, decisão, interrupt, close e reconexão. Essa campanha não foi realizada nesta auditoria.

**Não apliquei correções ao produto, não fiz push nem publicação.** Os arquivos originais foram preservados e verificados por hash. O pacote contém relatório, plano, testes e evidências; as expectativas que falham não devem ser invertidas para declarar aprovação.
