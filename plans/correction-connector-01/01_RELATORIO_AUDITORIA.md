# Auditoria do Okto Nexus Connector — snapshot 8bc5025

**Connector:** `0.1.0.dev0` — commit `8bc50259dc7eb353f349f1eb762be2d33cc393de` (comentário do ZIP).  
**Dependência declarada:** `nexus-connector-core==0.2.8.dev0`.  
**Comparação adicional:** Core `0.2.9.dev0` (`6909435...`), último código do Core disponível nesta conversa. C11 é requisito/plano pendente, não uma API presumida implementada.  
**Entrada:** `okto-nexus-connector-main.zip`, SHA-256 `62ebe945eccfc3f762cadb837501dd12a46cb1d5080ac41a99c72a189a593a59`.  
**Revisor:** análise estática e execuções descritas neste pacote; nenhum arquivo original foi alterado.

## 1. Conclusão executiva

A separação entre aplicação e biblioteca existe e deve ser preservada. Há CLI, daemon, IPC autenticado, importação de identidade de agente, cofre, cliente WSS, helpers de configuração e composição por `create_runtime`. Não encontrei servidor/proxy MCP nem codecs nativos copiados dentro do Connector.

**A implementação ainda não pode ser considerada completa em relação ao Plano 2 R3, nem pronta para operação distribuída governada.** Os principais bloqueios são do Connector: perda de metadados do candidato, reconstrução indevida do contexto de frames, falhas de filas/concorrência, lifecycle de encerramento e recuperação, e caminhos managed/remote ainda não conectados. Trocar a dependência não resolve esses problemas.

Foram organizados **17 achados/grupos de trabalho**, sem tratar cada parametrização como um bug independente. A campanha adicional tem **34 casos: 30 falham, três controles positivos passam e uma observação de buffer passa**. Há achados estáticos sem teste adicional específico; estes são identificados como tal. Não se deve inferir uma exploração remota anônima, um provider executado ou um teste multi-host a partir de uma fixture.

**Decisão:** continuar Server e Connector em paralelo é adequado. Antes da campanha integrada real, fechar os bloqueios de contexto, seleção, controle e recuperação; até lá, não apresentar o daemon como executor remoto operacional completo. Não reabrir a arquitetura do Core nem implementar compensações dos seus problemas nos hosts.

## 2. Referências normativas e escopo

Base: `02_PLANO_NEXUS_CONNECTOR.md`, revisão 3, 25/09/2026 (plano original de três projetos e MCP exclusivamente HTTP direto); alterações de Core C1–C10 e o contrato de identidade de instalação proposto em C11. O arquivo de plano também está no ZIP do Connector. Referências C00–C11 neste relatório são fases do Plano 2; os nomes A01–A17 identificam achados desta auditoria.

O Nexus mantém identidade canônica, autorização, inbox, outbox e handoffs. O Connector representa agentes já existentes, não usuários Nexus. O Core fornece runtimes, políticas técnicas e mecanismos duráveis. O harness MCP deve falar HTTP diretamente com o Server; a aplicação pode configurar esse cliente, mas não termina sessões MCP nem se torna relay.

Não alterei repositórios, permissões ou credenciais. Não executei providers reais, Nexus Server real, dois hosts remotos, Windows/macOS, nem instalação de serviços de SO. A revisão não valida alegações sobre endpoints atualmente implementados pelo Server: esse repositório não foi auditado neste trabalho.

## 3. Execuções verificadas

| Campanha | Resultado | Interpretação |
|---|---|---|
| Suíte do Connector, Core fixado 0.2.8, exceto dois testes de packaging | 111 PASS / 7 FAIL / 3 SKIP | 121 casos; fixtures/fakes incluídos |
| Mesma suíte com Core 0.2.9, experimento sem alterar pyproject | 111 PASS / 7 FAIL / 3 SKIP | Não aprova release com pin divergente |
| Auditoria adicional, fonte com Core 0.2.8 | 30 FAIL / 4 PASS | Três controles + uma observação entre os PASS |
| Auditoria adicional, fonte com Core 0.2.9 | 30 FAIL / 4 PASS | Mesmas causas; atualização não substitui correção do host |
| Auditoria adicional, wheel instalado + Core 0.2.8, `python -I` | 30 FAIL / 4 PASS | Sem importar a árvore-fonte do Connector |
| Wheel e sdist por `setuptools.build_meta`, em cópia | PASS | Build sem depender de download |
| CLI `--version`, imports e catálogo por wheel | PASS | Sem pacote Nexus Server ou servidor MCP carregado |
| Providers/SO remoto/Server real/E2 | NOT_RUN | Exige campanha posterior autorizada |

Subconjuntos e repetições não se somam como cobertura independente. Os dois testes originais de packaging exigem `build`, downloads e um clone irmão em caminho fixo; não executei esse fluxo. Fiz build por backend, instalação local sem resolução de dependências e probe isolado, com resultados registrados. Isso não equivale a uma instalação online em máquina limpa.

### 3.1. As sete falhas da suíte fornecida

Seis fluxos CLI/daemon usam executáveis sintéticos `.exe` criados sem bit executável no Linux, e são recusados em seleção/candidate. O sétimo teste espera inventário Pi vazio fora de Windows, mas o resolvedor passivo do Core retorna um candidato válido para a fixture. São diferenças de fixture/plataforma e expectativa que precisam ser corrigidas, não sete provas adicionais de bugs do produto. Não desliguei preflight nem executei os arquivos falsos.

Os bugs A01–A17 não foram deduzidos dessa contagem. As regressões independentes usam fixtures executáveis como dados quando necessário, peers controlados, código real nas fronteiras de interesse e teardowns que liberam barreiras.

### 3.2. Ambiente e dependências

Python 3.13.5/Linux; pytest 9.0.2; pytest-asyncio 1.3.0; httpx 0.28.1; websockets 16.0; jsonschema 4.26.0. O ambiente é uma venv de auditoria com bibliotecas disponíveis reaproveitadas. O módulo rfc8785 0.1.4 foi colocado no ambiente a partir do código oficial, sem stub de canonicalização: `_impl.py` confere com blob Git `3137d3326b98938affadb1be711ee411eb2ab86e`. O pacote não foi baixado por pip; a limitação de rede é registrada. Nenhum código dessa dependência foi modificado para passar testes.

## 4. Aderência ao plano original

| Fase | Avaliação da entrega |
|---|---|
| C00 — base independente | Estrutura/empacotamento presentes; pin e compatibilidade precisam atualizar. |
| C01 — identidade/cofre | Modelo agent-centric correto; origem e lifecycle de rotação ainda incompletos. |
| C02 — daemon/IPC | Singleton/readiness existem; shutdown, isolamento auxiliar e recovery precisam corrigir. |
| C03 — onboarding/binding | Fluxo implementado parcialmente; arquitetura perdida, seleção errada e CLI quebrada. |
| C04 — HTTPS/WSS | Transporte de contrato existe; falta aplicar handshake/escopo/geração/renovação com rigor e corrigir filas. |
| C05 — runtime via Core | Composição pública correta; contexts/lifecycle/remote verbs incompletos. |
| C06 — ferramentas managed | Helpers existem, mas configuração managed e bridge Pi não estão integradas ao caminho produtivo. |
| C07 — uso diário | CLI extensa, porém há NameError, prompt indevido e erro de state.save. |
| C08 — recuperação/limites | Recibos e journal do Core reutilizados; reconciliação/replay/filas da aplicação não fecham o requisito. |
| C09 — serviços/upgrade | Geradores existem; plist e root precisam corrigir, qualificação real de SO não ocorreu aqui. |
| C10 — artefato/evidências | Wheel construído; testes do executor não bastam para declarar todas as fases funcionais DONE. |
| C11 — integração real | Não executada nesta revisão; não é substituída por fakes. |

O status entregue marca C00–C10 DONE e C11 BLOCKED. Recomendo reabrir somente as tarefas atingidas por esta evidência e separar estado de implementação de qualificação. Não atribuo ao agente intenção de enganar; testes de um helper não demonstram automaticamente o wiring da aplicação que o deveria chamar.

## 5. Achados e correções explícitas

Todos os caminhos abaixo são relativos a `src/okto_nexus_connector/`, salvo `tests/`, `pyproject.toml` e referências ao Core. Números de linha são do snapshot; procurar pelos símbolos ao aplicar em HEAD posterior.

### A01 — Arquitetura do candidato perdida entre discovery e runtime

**Prioridade:** P1. **Evidência:** Bug reproduzido.  
**Referência:** R3 C03.3 / C05.1–2; Core C9–C11.  
**Âncoras:** `services/connect_service.py:146–158`; `storage/state_store.py:BindingRecord`; `services/core_host.py:120–172`.

**O que foi observado.** Um InstallationCandidate selecionado com architecture=x86_64 é passado ao serviço create_binding real, persistido e reconstruído por candidate_for. O resultado contém architecture=None. O modelo BindingRecord sequer preserva esse campo. Fiz também uma comparação pura usando uma tupla genuína da qualificação do Core 0.2.8: com x86_64 a qualificação devolve True; retirando somente a arquitetura, False.

**Consequência.** A composição real perde evidência indispensável à qualificação antes de chamar a factory. As fixtures que injetam uma factory sintética não percorrem esse gate. Não é um requisito novo do Core 0.2.9: a versão fixada já exige a arquitetura.

**Correção exigida.** Preservar o candidato observado como dados versionados, incluindo arquitetura, versão, build, fingerprint local e alvos necessários. Revalidar usando os mecanismos públicos do Core. Para registros antigos, marcar NEEDS_REDISCOVERY e recuperar evidência local por procedimento aprovado; nunca preencher a arquitetura com a do host por suposição. Não remover a qualificação nem introduzir uma allowlist no Connector. Migrar o schema local sem recriar agente ou apagar histórico.

**Aceite.** Round-trip completo mantém todos os campos; registro legado sem evidência não anuncia READY; mesma instalação qualificada continua qualificável depois de persistir/reabrir o estado.

**Limites da prova.** Nenhum provider real foi executado. A perda de campo e sua consequência na função de qualificação foram demonstradas separadamente; não se trata das permissões de execução das fixtures Linux da suíte original.

**Testes associados:** `test_01_qualified_candidate_architecture_survives_binding_roundtrip`.

### A02 — Envelope remoto descartado e contexto autorizado reconstruído indevidamente

**Prioridade:** P1. **Evidência:** Bugs reproduzidos e lacunas de validação.  
**Referência:** R3 C04.2–4 / C05.2 / C08.2; contratos de ExecutionContext e fences do Core.  
**Âncoras:** `transport/wss_client.py:239–248,349–421`; `daemon/app.py:278–308`; `services/runtime_service.py:258–312,502–535`.

**O que foi observado.** O transporte não confronta server_id/executor_id/agente/binding/geração do frame com a conexão e a lane ativa. Também aceita operação antes de welcome/reconciliação. A geração oferecida pelo Server é comparada, mas não adotada; um contador local é incrementado por conexão. Depois, o dispatcher extrai texto/IDs e o RuntimeManager substitui o restante por _remote_context da sessão. Na reprodução completa, um frame NXL válido com agente errado e geração antiga resultou em SUBMITTED e send_turn no peer, passando pelo kernel/journal reais. A sessão original estava autorizada a turn.submit, de modo que o teste não depende de burlar o controle interno de allowed_actions do Core. Separadamente, _context transforma allowed_actions vazio em permissões default; _remote_context fabrica um prazo mínimo de 30 segundos e uma lista fixa de ações.

**Consequência.** O Core valida o contexto entregue pelo host, não o envelope que o host descartou. Assim ele não consegue detectar um frame antigo que o Connector reescreveu como contexto atual. O problema é autenticação/escopo da operação no adaptador de transporte, não uma demonstração de invasão anônima pela Internet.

**Correção exigida.** Criar estado de conexão/lane com scope exato e geração negociada. Validar envelope e grant autenticado antes de produzir ExecutionContext. Persistir ou manter registro autorizado por sessão com ações/revisões/deadline legítimos; uma mensagem não pode inventar permissões nem renovar prazo. Adotar a geração negociada conforme NXL; não inferi-la do número de reconnects. Recusar dados obrigatórios ausentes em produção; remover fallback de testes do caminho produtivo. Controles de contenção usam a semântica explícita do Core, não permissões genéricas fabricadas.

**Aceite.** Agente, Server, executor, binding, workspace, owner/connection generation e revisões incorretos resultam em zero efeitos; welcome/reconcile/lane pendentes não admitem trabalho; o mesmo frame correto continua funcionando. Receipt de operação conhecida mantém recuperação autorizada. O hash continua sendo verificado, mas não é usado como credencial.

**Limites da prova.** O controle adicional mostrou que o Core recusa turn.submit quando a ação não existe no contexto original da sessão. A fabricação de prazo no helper, isoladamente, não comprova execução depois do deadline original: o Core mantém verificações próprias. Não atribuo essa suposta falha ao Core.

**Testes associados:** `test_03_empty_authority_does_not_gain_fallback_permissions`, `test_04_remote_context_never_extends_expired_authorization`, `test_05_inbound_operations_require_exact_live_lane_scope`, `test_06_server_generation_is_adopted_after_welcome`, `test_07_operation_cannot_enter_before_handshake_and_reconcile`, `test_22_websocket_dispatch_does_not_ignore_wire_identity`.

### A03 — Caches e tarefas não estão integralmente separados por Server

**Prioridade:** P1. **Evidência:** Bug reproduzido e inspeção.  
**Referência:** R3 C02.3 / C10.2 / TC-06, TC-41.  
**Âncoras:** `services/core_host.py:186–202`; `services/runtime_service.py:83–96,238–244,490–500,624–645`; `daemon/app.py:318–326`; `cli/commands/bind.py:remove`.

**O que foi observado.** CoreRuntimeHost armazena runtimes somente por binding_id. Dois bindings de Servers diferentes com o mesmo ID recebem a mesma instância: o segundo build retorna antes de validar o candidato. _pumps e _lease_tasks são indexados apenas por session_id, e aprovações apenas por request_id. Há remoções de bindings que filtram só binding_id. A tabela principal de ManagedSession usa server_id/session_id, mas os registros auxiliares não acompanham esse namespace.

**Consequência.** Colisões válidas entre instalações de Server podem reutilizar o runtime errado, cancelar a task de outro contexto ou remover objetos indevidos. Não se deve pressupor unicidade global de IDs emitidos por servidores independentes.

**Correção exigida.** Adotar chaves tipadas ServerExecutorKey/BindingKey/SessionKey em TODOS os registros auxiliares, rotas, preferências e ownership de configuração. APIs de CLI podem aceitar alias, mas devem resolvê-lo univocamente antes da mutação. Ambiguidade retorna erro e exige qualificação de Server, nunca primeiro item. Considerar workspace binding e revisão na reutilização de runtime.

**Aceite.** Dois Servers com mesmos IDs textuais de binding/sessão/request operam isolados. Stop/remove/ACK/rotate/reconcile de A não modifica B. Testar também caminhos de erro e tasks de lease/eventos.

**Limites da prova.** A reutilização incorreta da instância foi reproduzida. Não alego que o Core aceitou automaticamente uma identidade diferente: suas validações podem recusar posteriormente; a escolha errada do recurso pelo Connector já é defeito.

**Testes associados:** `test_02_two_servers_same_binding_id_do_not_share_core_instance`.

### A04 — Fila de prioridades perde uma mensagem quando dois consumidores despertam juntos

**Prioridade:** P1. **Evidência:** Bug reproduzido.  
**Referência:** R3 C04.5 / C08.3 / TC-19.  
**Âncoras:** `transport/wss_client.py:77–113`.

**O que foi observado.** get() inicia dois queue.get(), um para urgent e outro para normal. Se ambos terminam, os dois itens já foram retirados das filas, mas o código devolve somente o primeiro task do conjunto done. Na reprodução, urgent foi entregue e normal desapareceu; a próxima leitura não o recuperou. Os dois put ocorreram antes de o waiter voltar a executar.

**Consequência.** Mensagens de controle, recibos ou dados podem ser perdidos sem erro ou mecanismo de retry específico. O problema independe de saturação do limite da fila.

**Correção exigida.** Usar um único seletor de filas com Condition/Event e retirada atômica, ou conservar explicitamente todo resultado já retirado. Definir fairness e backpressure por item/byte. Quando put retorna False, o chamador precisa tratar a recusa, sobretudo attach e recibos; não declarar envio/attach bem-sucedido pelo simples enqueue tentado.

**Aceite.** Inserção simultânea nas duas prioridades entrega ambos exatamente uma vez. Corridas, cancelamento do waiter e fila cheia não descartam itens já aceitos. Saturação normal não impede controle urgente.

**Limites da prova.** Teste determinístico de filas em memória; não afirma perda observada em um Server real.

**Testes associados:** `test_08_priority_queue_conserves_both_items_waking_together`.

### A05 — Recepção bloqueada por execução e watchdog sem caminho periódico efetivo

**Prioridade:** P1. **Evidência:** Dois bugs reproduzidos.  
**Referência:** R3 C04.3–5 / C08.3.  
**Âncoras:** `transport/wss_client.py:269–344`.

**O que foi observado.** _receiver aguarda _handle e este aguarda a operação nativa inteira. Uma operação retida impediu a leitura do interrupt seguinte no mesmo socket, embora o Core permita controle independente. Em _session, a checagem de ausência só acontece depois de FIRST_COMPLETED entre três tasks de duração indefinida; não existe timeout no wait e last_receive não é atualizado. O teste encurtou a constante para 0,03 s e manteve as tasks pendentes: o prazo não foi consultado.

**Consequência.** O canal é full duplex no transporte, mas a aplicação serializa recepção e execução. Uma tarefa lenta pode impedir controles/ACKs e a detecção de ausência da aplicação. Ping/pong da biblioteca pode encerrar algumas conexões: não afirmo que todo socket perdido ficará aberto para sempre.

**Correção exigida.** Manter reader pequeno: decode, validar scope, admitir/encaminhar a um scheduler limitado. Separar execução produtiva e controles com quotas, sem task ilimitada por frame. Implementar watchdog próprio com relógio monotônico e sinal de atividade de entrada; integrá-lo à política de lease, sem renovação autoconcedida. Um sender encerrado normalmente também deve encerrar a sessão de transporte de forma definida.

**Aceite.** Com submit preso, interrupt/revoke/ACK ainda são recebidos e roteados. Watchdog vence com tasks ainda vivas; tráfego recebido válido o atualiza. Loop não entra em busy-spin quando uma task termina.

**Limites da prova.** Peers controlados; não foi medida latência de provider real nem estabelecido um SLO universal de milissegundos.

**Testes associados:** `test_09_silent_websocket_is_detected_while_tasks_are_pending`, `test_26_blocked_submit_does_not_block_received_control`.

### A06 — HTTP sem TLS remoto é aceito e a importação MCP compara origem por prefixo

**Prioridade:** P1. **Evidência:** Bugs reproduzidos.  
**Referência:** R3 C01 / C04.1 / TC-05.  
**Âncoras:** `transport/https_client.py:65–75,106–119,140–164`; `identity/import_flow.py:65–118`.

**O que foi observado.** NexusHTTPClient recusa verify=False fora de loopback, mas aceita http://remote.example com verify=True e monta Authorization: Bearer. MockTransport observou essa requisição. Na importação explícita de uma entrada MCP, startswith aceita https://nexus.example.attacker.invalid como se fosse a origem https://nexus.example. A rotina de redirecionamento contém ainda str(response.url).join(location), que não é junção de URLs.

**Consequência.** Uma configuração de endereço inadequada pode transmitir a chave por HTTP não protegido; o teste usou apenas segredo sintético e nenhuma rede externa. A checagem de origem da importação não garante o host aprovado. O redirect cross-origin testado foi recusado, e não há evidência de encaminhamento automático de chave nesse caminho.

**Correção exigida.** Validar scheme/host/porta antes de acessar cofre ou preparar cabeçalhos. Fora de loopback exigir HTTPS/WSS; exceção de laboratório explícita e restrita. Canonicalizar e comparar a tupla de origem, sem prefixo textual; rejeitar userinfo e formatos ambíguos conforme contrato. Usar urljoin só para interpretar Location e não seguir redirecionamento autenticado para outra origem. Revalidar alteração de origem de um Server já conhecido com consentimento explícito.

**Aceite.** HTTP não loopback produz zero requisições autenticadas. Subdomínio malicioso, porta diferente, downgrade, URL com userinfo e redirect divergente recusados. Loopback de testes permitido na política documentada.

**Limites da prova.** Não houve exfiltração de credencial real nem tentativa em servidores de terceiros. O risco é comprovado pela construção/entrega da requisição ao transporte de laboratório.

**Testes associados:** `test_10_http_plaintext_nonloopback_rejected_before_secret_transmission`, `test_12_explicit_mcp_import_uses_exact_origin_not_prefix`.

### A07 — Erro de leitura após POST é classificado como ausência de efeito e retry seguro

**Prioridade:** P1. **Evidência:** Bug reproduzido.  
**Referência:** R3 C03.4 / C04.4 / C08.1.  
**Âncoras:** `transport/https_client.py:140–179,209–330,362–378`; `services/runtime_service.py:314–347`; `services/connect_service.py:create_binding`.

**O que foi observado.** Após o transporte receber um POST, injetei ReadTimeout na resposta. _request converteu o erro para retry_safe=True, possible_effect=False e recomendou repetir porque nenhum efeito seria possível. A mesma classe genérica captura outros erros de transporte. Há ainda um POST de operação posterior a core.submit: uma falha de rede pode ocultar do usuário um recibo local de efeito já produzido.

**Consequência.** O usuário ou uma camada de retry pode emitir nova intenção e duplicar setup/trabalho remoto quando o primeiro pedido foi aceito. A auditoria não executou duplicação no Server real: demonstrou a classificação incorreta justamente na fronteira que permitiria a decisão errada.

**Correção exigida.** Classificar por método, estágio e prova, não por exceção HTTPError genérica. Gerar ID de intenção antes de mutações remotas, conservá-lo em registro local e consultar a mesma operação após perda de resposta. Alinhar endpoint de resolução e endpoint de execução para não executar local e remotamente a mesma intenção. Um recibo local conhecido sobrevive à falha de comunicação posterior e fica consultável. GET e recusa comprovadamente anterior à entrega podem ter política distinta.

**Aceite.** Server de laboratório comita e perde ACK: a operação fica unknown/queryable com o mesmo ID; não cria um segundo ID nem faz segundo efeito. Falha de conexão comprovadamente pré-envio continua diagnosticável. Retry idêntico recupera o recibo.

**Limites da prova.** A escolha de nomes de rotas definitivos depende da integração com o Server; não foi verificado um Server vivo. O contrato de idempotência deve ser acordado, não implementado unilateralmente por suposição.

**Testes associados:** `test_11_read_timeout_after_mutating_request_is_not_safe_retry`.

### A08 — Stop e shutdown confundem pedido, resultado e encerramento do supervisor

**Prioridade:** P1. **Evidência:** Bugs reproduzidos e lacunas de lifecycle.  
**Referência:** R3 C02.5 / C05.4–5 / C08; correções C1–C10 do Core.  
**Âncoras:** `services/runtime_service.py:416–468,629–645`; `services/core_host.py:209–224`; `daemon/app.py:158–174,362–438`.

**O que foi observado.** stop marca closing antes de resolver autorização HTTP; quando essa resolução falha, o estado continua closing e nova tentativa não repete o controle. Depois de um CloseReceipt OUTCOME_UNKNOWN, o manager remove sessão e pumps da gestão. shutdown rotula stop como graceful sem examinar o desfecho e rotula erro como forced apenas por possible_effect. O helper CoreRuntimeHost.shutdown_all apaga a instância e fecha stores mesmo com unknown. O caminho principal do daemon é diferente: chama RuntimeManager.shutdown, fecha IPC/transporte e retorna zero; ele não usa esse helper, mas também não preserva um estado público de conclusão pendente. dispatch não impede runtime.start/submit enquanto _draining está ativo.

**Consequência.** A aplicação pode perder a rota normal de consulta/recuperação e apresentar um encerramento mais forte do que comprovou. Não demonstrei um processo órfão vivo no SO: o Core possui mecanismos próprios de contenção. O defeito observado é de lifecycle, semântica e referências do host.

**Correção exigida.** Definir máquina de estado de StopAttempt com ID, receipt e produtor possuído. Erro pré-efeito libera só a tentativa para retomada; unknown conserva sessão técnica e controle. Fechar admissões na borda do daemon antes de drenar, usar orçamento total explícito e não multiplicar 45 s por runtime silenciosamente. Colher ShutdownReport real do Core; não inferir forced/graceful de exceção. Só dispor Core/stores quando suas obrigações estiverem resolvidas ou transferidas por contrato explícito a guardian/recovery qualificado. Saída não zero/relatório pending quando apropriado, sem prometer parada não observada.

**Aceite.** HTTP indisponível não torna stop permanentemente irrecuperável. Unknown permanece consultável e o segundo shutdown alcança o mesmo recurso. Drain rejeita novos starts. Pending durable e physical stop são estados distintos; timeout do host não cancela produtor durável do Core.

**Limites da prova.** A falha de shutdown_all foi testada nesse helper isolado e não é apresentada como execução do caminho principal do daemon. A proposta deve unificar os dois, evitando implementações paralelas divergentes.

**Testes associados:** `test_13_failed_stop_can_be_retried_and_not_marked_closed`, `test_14_unknown_close_receipt_keeps_managed_session_recoverable`, `test_15_shutdown_does_not_discard_core_ownership_on_unknown`.

### A09 — Reconciliação usa namespace implícito, snapshots inválidos e depende de runtimes em memória

**Prioridade:** P1. **Evidência:** Bugs reproduzidos e gap de restart.  
**Referência:** R3 C04.4 / C08.1 / TC-17, TC-31.  
**Âncoras:** `daemon/app.py:330–343`; `services/runtime_service.py:470–486`; `daemon/app.py:129–153,178–204`.

**O que foi observado.** O handler descarta server_id/executor_id do reconcile.request. O manager escolhe primeiro runtime e, se necessário, primeiro binding. Snapshots são serializados como str(s); um frame de relatório com snapshot real foi rejeitado pelo codec NXL real. Sem _runtimes em memória, reconcile devolve listas vazias; não há caminho de startup que primeiro consulte claims/recibos persistidos e produza a evidência histórica antes de novas admissões.

**Consequência.** Reconnect pode falhar na serialização ou associar evidência à instalação errada. Depois de restart, vazio pode esconder operações duráveis ainda desconhecidas. A perda efetiva ou o reenvio de uma tarefa depois de crash não foi reproduzido; a lacuna de acesso à evidência está no código.

**Correção exigida.** Exigir namespace autenticado completo e usar acesso público ao journal/RuntimeCore apropriado, inclusive quando não há handle vivo. Projetar DTOs conforme os schemas reais de receipt e snapshot; validar encode/decode antes do envio. Separar histórico durável de observação física e manter unknown sem tentar reabrir pipes por PID. Reconciliação é estado obrigatório antes de admitir trabalho na nova conexão.

**Aceite.** Relatório não vazio passa no codec; Server B nunca consulta dados de A. Reiniciar daemon com operação persistida retorna receipt/unknown correspondente sem spawn e sem execução repetida. Testar intents conhecidos com sessão evictada.

**Limites da prova.** Não confundir o fake WSS peer com uma comprovação de reconciliação com Nexus Server real. A integração externa continua não executada.

**Testes associados:** `test_16_reconcile_response_is_valid_nxl_not_string_snapshots`, `test_17_reconcile_passes_originating_namespace_not_first_binding`.

### A10 — Fluxo managed não instala a configuração MCP HTTP nem compõe a bridge Pi

**Prioridade:** P1. **Evidência:** Bug de wiring reproduzido + funcionalidade ausente.  
**Referência:** R3 C06.1–5; ausência de proxy MCP preservada.  
**Âncoras:** `services/runtime_service.py:191–236`; `services/core_host.py:54–75,191–197`; `services/mcp_config_service.py`; `tests/contract/test_native_bridge.py`.

**O que foi observado.** start pede capability de sessão e a coloca no resolver, mas LaunchOverlay.http_templates e secret_bindings permanecem vazios. O teste executou o serviço de start e a construção real do ambiente do Core: o token sintético não apareceu na referência de ambiente necessária, e nenhuma URL/template foi acrescentada ao lançamento. Helpers de configuração persistente existem, mas não estão ligados a esse fluxo automático. create_runtime não recebe pi_native_action; a bridge Pi está demonstrada em testes que importam o Core diretamente e usam um backend fake, sem compor a aplicação Connector. O callback de resume e habilitação de native approvals também não são fornecidos.

**Consequência.** É possível ter uma sessão nativa criada sem acesso governado às ferramentas de colaboração que deveria utilizar. Autoconfiguração manual separada não cumpre o primeiro uso sem atrito. Testar somente a biblioteca da bridge não demonstra que o Connector hospeda a bridge.

**Correção exigida.** Montar configuração efêmera do cliente MCP HTTP no processo do harness, apontando diretamente ao Server e usando capability de sessão mínima. Ligar templates/env/argv ou arquivo temporário apropriado ao formato realmente suportado, sem proxy. Guardar dados por sessão, não em closure da primeira sessão reutilizada por todo binding. Instanciar callback Pi nativo não MCP com backend limitado das APIs canônicas; validar scope/lease/ID no host, sem criar outra inbox. Operador de native approvals deve integrar o método público do Core e a autoridade do Server.

**Aceite.** Duas sessões do mesmo binding recebem capabilities distintas e somente as suas; URL/env/config efetivos são observados no processo de laboratório. Pi invoca ação estruturada passando por código do Connector e backend HTTP controlado. Tools-only continua independente do daemon e nunca existe listener MCP.

**Limites da prova.** Não encontrei servidor/proxy MCP reintroduzido. O problema é a ausência da configuração/bridge no caminho produtivo, não a existência de stdio nativo.

**Testes associados:** `test_27_start_session_capability_is_referenced_in_effective_environment`.

### A11 — Operações remotas e aprovações estão incompletas, e erros do Core não viram recibos consistentes

**Prioridade:** P1. **Evidência:** Gap funcional confirmado por inspeção.  
**Referência:** R3 C05.1–5 / C07.4; painel e CLI sobre a mesma execução.  
**Âncoras:** `daemon/app.py:278–343,440–472`; `services/runtime_service.py:258–288`; `transport/wss_client.py:349–393`.

**O que foi observado.** O dispatcher remoto implementa somente turn.submit, turn.interrupt e runtime.close. Não há caminho remoto de open, steer, input/provide ou decisão nativa via Core. runtime.close ignora o operation_id recebido, chama stop local que faz outra resolução HTTP e não devolve receipt. interrupt descarta expected_turn_id. Não existe chamada produtiva a RuntimeCore.decide_native_approval. CoreError não é ConnectorError e pode escapar do handler para encerrar o receiver, em vez de produzir a recusa/recibo. A aprovação de UI é removida antes de validar decisão e antes do HTTP; uma falha pode consumi-la sem confirmação.

**Consequência.** O requisito de iniciar e administrar runtimes pelo Server ainda não está completo. Um teste de enviar texto por WSS não demonstra o controle remoto integral. Campos descartados podem invalidar o contrato de idempotência e de correlação de turno.

**Correção exigida.** Criar matriz de verbos NXL versus API pública do Core e implementar cada operação anunciada. Capacidades ainda não conectadas devem permanecer indisponíveis. Manter IDs, correlação e contexto; close retorna receipt da mesma intenção. Separar pedido HITL do Server de resposta de aprovação/input do harness; cada um possui autoridade e estado próprios. Reservar decisão e consumir só após prova adequada. Traduzir CoreError preservando code/stage/possible_effect/retry_safe/operation_id e validar frame resultante.

**Aceite.** Remote open→turn→interrupt→close usa o mesmo ID de cada intenção. Approval pendente termina no writer correto do Core; decisão negativa segura e erro pós-write mantêm sua semântica. Recusa do Core não derruba canal nem se transforma em sucesso. Nenhuma capacidade é anunciada apenas para satisfazer UI.

**Limites da prova.** Ausência de rota é gap de implementação, não falha dinâmica de um provider. Não inventei um endpoint real de Server nem comprovei autoaprovação indevida no Server.

**Testes associados:** inspeção estática; criar cenários de integração correspondentes, sem considerar como já executados.

### A12 — Estado online, attach, tickets e rotação não refletem o ciclo de autorização

**Prioridade:** P2. **Evidência:** Inspeção estática do fluxo.  
**Referência:** R3 C01 / C04.2–3 / TC-16.  
**Âncoras:** `transport/wss_client.py:239–248,395–421`; `daemon/app.py:178–274`.

**O que foi observado.** online é ativado ao abrir o socket, antes do handshake NXL. attached=True é gravado ao tentar enfileirar binding.attach, sem conferir o retorno de put. Tickets são buscados no attach, mas a expiração é descartada e não há renovação periódica por lane. reload_state acrescenta lanes sem chamar attach numa conexão já online e não atualiza epoch/revisões das existentes. O bootstrap escolhe a primeira identidade e o primeiro binding independentemente, podendo pedir um ticket com credencial de outro agente.

**Consequência.** Uma UI pode informar disponível para uma lane ainda não admitida. Mudanças locais exigem reconnect para efetivar parte do estado, e múltiplas identidades podem impedir autenticação corretamente no Server. Não há prova de obtenção de ticket proibido: espera-se que o Server o recuse.

**Correção exigida.** Estados separados SOCKET_OPEN, NEGOTIATING, RECONCILING, LANE_READY; derivar elegibilidade de binding dessa máquina e do grant vigente. Renovação single-flight escopada, com jitter e expiração real. Diff de reload resolve adição/remoção/rotação e fecha concessões antes de trocar credenciais. Bootstrap escolhe binding e sua identidade correspondente. Backpressure nunca marca attach como concluído.

**Aceite.** Adicionar lane online funciona sem reiniciar daemon; ticket expira/renova sem intervenção; remover agente A não muda B; fila urgente cheia não produz falso attached; rotação revoga a epoch anterior e não reaproveita metadados antigos.

**Limites da prova.** Esses cenários complementares precisam de testes após implementação; não foram contados como execuções que passaram ou falharam nesta auditoria.

**Testes associados:** inspeção estática; criar cenários de integração correspondentes, sem considerar como já executados.

### A13 — Buffer de eventos copia payloads indefinidamente fora do journal

**Prioridade:** P2. **Evidência:** Observação executada + gap de limites/ACK.  
**Referência:** R3 C04.4–5 / C08.3 / TC-33.  
**Âncoras:** `daemon/app.py:42–101`; `transport/wss_client.py:373–379`; `services/runtime_service.py:557–575`.

**O que foi observado.** EventBridge._pending é lista sem limite por stream. Ao publicar 3.000 eventos de laboratório sem transporte online, todos os 3.000 payloads permaneceram em memória. É uma observação de crescimento, não um teste contra um limite arbitrário de 1.024. Os ACKs do transporte não são confrontados com geração/namespace, maior sequência enviada ou monotonicidade. O pump encerra em erro, e o restart não hidrata seus cursores duráveis.

**Consequência.** A limitação da fila WSS não limita a cópia anterior do stream; um período offline longo pode crescer com todo o histórico. Um ACK inconsistente pode interromper a ponte ou produzir decisões incorretas de avanço, dependendo da validação posterior do Core.

**Correção exigida.** Usar journal como fonte de replay, manter em RAM somente cursor/notificação e lotes limitados por bytes/itens. Backpressure controla leitura do journal sem perder eventos aceitos. Confirmar apenas prefixo contíguo durável no namespace/epoch/generation autorizado, nunca aceitar watermark futuro ou regressivo. Reconectar retoma cursores; compactação segue confirmação real. Distinguir gap comunicado de descarte silencioso.

**Aceite.** Carga offline supera repetidamente o orçamento configurado sem crescimento proporcional em RAM; volta da conexão drena eventos via journal. ACK duplicado, antigo, futuro ou de outro Server não compacta dados incorretos. Saturação de um agente não impede controles de outro.

**Limites da prova.** Não foi executado OOM, crash com perda de evento ou servidor malicioso de ACK. Esses riscos são delimitados como consequência potencial de código e cobertura faltantes.

**Testes associados:** `test_observation_offline_event_copy_retains_all_3000_events`.

### A14 — Falhas concretas no fluxo de seleção e no uso headless

**Prioridade:** P2. **Evidência:** Três bugs reproduzidos.  
**Referência:** R3 C03 / C07.1–2; contrato de instalação C11.  
**Âncoras:** `cli/commands/connect.py:116–119,183–230`; `cli/commands/bind.py:28–65`; `services/connect_service.py:103–133`.

**O que foi observado.** run_connect passa non_interactive=False na confirmação agregada mesmo quando o usuário solicitou modo não interativo. Selecionar a segunda instalação de uma mesma família devolve apenas adapter_id; o código então usa matches[0], perdendo a escolha física. run_bind(create) define uma classe local com identity=identity; o NameError foi reproduzido antes da chamada de criação. Além disso, create_binding reutiliza o registro antigo pelo mesmo alias/família/projeto mesmo quando foi passado outro candidato, tornando o conselho de rebind para atualizar o executável insuficiente.

**Consequência.** Automação pode aguardar prompt indevido; a seleção humana pode apontar para outro executável; o comando avançado bind create falha. A unicidade de referência do Core C11 precisa ser consumida, mas corrigir somente o Core não corrige matches[0] da UI/CLI.

**Correção exigida.** Passar o candidato ou ref inequívoca como valor de seleção; resolver publicamente no mesmo inventário/revisão. Usar ImportResult verdadeiro em vez de classe improvisada. --non-interactive nunca pede input: exige aprovação/flags explícitas ou retorna APPROVAL_REQUIRED. Rebind faz diff e atualização CAS do alvo autorizado, preservando histórico, ou recusa mudança claramente; não aparentar sucesso com seleção antiga.

**Aceite.** Escolher B realmente usa B após reordenação; modo headless completo não chama input; bind create chega ao serviço com a identidade importada correta; mudança de binário exige aprovação e altera somente o binding pretendido.

**Limites da prova.** A reprodução da CLI substituiu login/probe/rede por resultados controlados, mantendo o código real de escolha/DTO/confirm. Nenhum executável de provider foi rodado.

**Testes associados:** `test_19_noninteractive_connect_does_not_prompt`, `test_24_select_second_installation_selects_that_exact_path`, `test_25_bind_create_completes_identity_dto_construction`.

### A15 — StateStore.save chama um método inexistente

**Prioridade:** P2. **Evidência:** Bug reproduzido.  
**Referência:** R3 C00 / C03.4 / armazenamento local.  
**Âncoras:** `storage/state_store.py:128–130,218–243`.

**O que foi observado.** save chama self._write_locked, que não existe. A operação pública simples StateStore(...).save(ConnectorState(...)) lança AttributeError. O writer efetivo é outra função; update usa um caminho diferente.

**Consequência.** Qualquer consumidor desse método falha em salvar estado. A maioria dos fluxos atuais utiliza update, por isso esse resultado não significa que todas as gravações do Connector estejam quebradas.

**Correção exigida.** Consolidar save/update no writer atômico sob o mesmo lock e revisão. Definir claramente se save substitui snapshot ou exige CAS; não usar um alias sem preservar bloqueio/mode/rename. Testar concorrência e erro entre arquivo temporário e replace.

**Aceite.** save/load round-trip passa; update continua preservando campos concorrentes; permissões locais e atomicidade verificadas.

**Limites da prova.** Defeito localizado, sem necessidade de outro banco nem mudança de arquitetura.

**Testes associados:** `test_18_state_store_save_public_method_works`.

### A16 — Artefatos de autostart inválidos ou sem o diretório de estado

**Prioridade:** P2. **Evidência:** Geração de artefatos testada; SO alvo não executado.  
**Referência:** R3 C09 / TC-36.  
**Âncoras:** `platform/service_install.py:39–128,183–204`.

**O que foi observado.** O plist gerado contém EnvironmentVariables como string vazia e RunAtLoad/KeepAlive como strings, não valores plist do tipo apropriado. O root é ignorado. O comando Windows também ignora o root recebido, podendo iniciar outro diretório de estado. O teste decodificou o plist real com plistlib e inspecionou o comando gerado. Na parte Linux, loginctl recebe $USER literalmente sem shell, e a unidade concatena paths sem escaping adequado.

**Consequência.** Uma instalação customizada pode subir sem seus bindings/credenciais ou o serviço pode rejeitar o artefato. As flags genéricas de sobrevivência a boot/logout são mais fortes que a evidência de geradores de texto.

**Correção exigida.** Gerar plist com plistlib e tipos corretos; transportar explicitamente --state-dir ou env aprovado em cada SO. Construir ações Windows com quoting correto para caminho com espaços e contrato de working directory/ACL. Usar usuário real na consulta de linger. Qualificar start/status/restart/logoff/boot separadamente; autostart no login não é processo contínuo desde o boot.

**Aceite.** Parser/plataforma aceita artefato; daemon iniciado pelo serviço responde no root escolhido e reutiliza mesmas identidades. Testes por SO comprovam os limites declarados, incluindo paths com espaços e caracteres XML.

**Limites da prova.** Não executei launchd, schtasks ou reboot. Os defeitos de formato e omissão de argumentos foram reproduzidos; comportamento do gerenciador real permanece requisito de qualificação.

**Testes associados:** `test_20_launchd_plist_uses_dictionary_and_boolean_types`, `test_21_windows_autostart_preserves_custom_state_directory`.

### A17 — Alinhamento com Core público ainda parcial: versão, disponibilidade e identidade da instalação

**Prioridade:** P2. **Evidência:** Gap de integração e compatibilidade.  
**Referência:** R3 C00.4/C05; Core C9/C10/C11.  
**Âncoras:** `pyproject.toml:dependencies`; `services/discovery_service.py`; `services/core_host.py:120–172`; `cli/commands/connect.py:_choose_harness`.

**O que foi observado.** O catálogo de tipos é corretamente obtido via get_runtime_catalog. Entretanto, a dependência continua ==0.2.8.dev0, anterior à CandidateAvailability entregue em 0.2.9. O Connector não utiliza evaluate_runtime_availability; reconstrói políticas locais de discovery/probe/build e consulta containment_preflight via native.process. Não publica snapshot de inventário/disponibilidade ao Server pelo fluxo WSS. Existem tabelas fixas de particularidades por adaptador, mas o controle positivo confirmou que a enumeração principal vem do catálogo; não a classifiquei como outra lista autoritativa independente.

**Consequência.** A atualização do Core não basta para alterar o binding se o aplicativo descarta metadados, publica caminhos em vez de projeção segura ou escolhe pelo primeiro ID. A qualificação deve continuar na biblioteca, enquanto conectividade/TTL/autorização pertencem aos aplicativos.

**Correção exigida.** Escolher e fixar um wheel exato após validar as correções vigentes; usar catálogo, disponibilidade e resolução pública da instalação. C11 foi proposto no último plano do Core e não está implementado no último ZIP fornecido: consumir a API real quando entregue, sem inventar nomes ou duplicar hashes. Versionar a projeção, fazer migração somente inequívoca e expor dados do host remoto. Manter arrays apenas de apresentação não autoritativos; mover conhecimentos de packaging nativo ao Core quando faltarem portas públicas.

**Aceite.** Adicionar adaptador ao Core atualiza tipos do Connector/UI sem nova lista; duas instalações iguais continuam distinguíveis; build não qualificado não vira pronto; Server Linux exibe disponibilidade remota Windows da fonte remota. Mesmo wheel nas integrações reais.

**Limites da prova.** Trocar experimentalmente 0.2.8 por 0.2.9 não corrigiu os 30 testes de defeitos do Connector. O experimento não é aprovação dessa combinação para release, pois contraria o pin declarado. C11 não foi presumido concluído.

**Testes associados:** `test_control_catalog_is_from_core`.

## 6. Riscos adicionais a verificar, não promovidos a bugs reproduzidos

**Compatibilidade de websockets:** o requisito permite versão 13, enquanto o código usa o alias de topo `websockets.connect` com `additional_headers`. As APIs nova e legacy da série 13 têm argumentos diferentes (`additional_headers` versus `extra_headers`). Usar import explícito da implementação qualificada e testar o mínimo/máximo da faixa ou restringi-la. Nesta auditoria executei websockets 16, não a versão 13. Fontes primárias: documentação `websockets 13.1`, seções Client new asyncio / Client legacy asyncio; este é um item de compatibilidade estático, fora dos 30 casos com falha.

**Concorrência de stores:** `ensure_journal()` faz check seguido de await sem inicialização single-flight. `ledger` e algumas operações StateStore continuam síncronas em rotas de loop. Revisar com barreiras antes de alegar responsividade ou unicidade sob dois starts concorrentes. Não foi reproduzido um travamento dessa rotina nesta rodada.

**Configuração persistente:** o ownership de entradas precisa considerar arquivo, adapter, binding e Server. Preferências globais e chaves usadas para remover nem sempre coincidem com as usadas para aplicar. Validar apply/backup/remoção sobre duas configurações reais, sem apagar ajustes de terceiros. Nenhuma remoção real de configuração de usuário foi executada.

**Provider home:** a opção de confiança escolhe diretórios próprios do provider e os passa como `provider_home`. Confirmar se o Core/harness espera HOME do usuário ou diretório específico do provider; não generalizar `.codex`, `.claude` e `.pi` como o mesmo contrato. Credenciais, plugins e hooks só dentro do escopo aprovado. Não registrei esse ponto como falha comprovada.

**Erros de redirecionamento e status:** substituir `str.join` por resolução de URL apropriada não implica seguir redirects automaticamente. GOAWAY, perda de ticket e erro fatal/transiente devem ter decisões explícitas de reconnect; não inferir política pela presença do socket.

## 7. Atualizações necessárias nos consumidores do Core

O catálogo está corretamente centralizado. Preserve esse avanço: não substituir `get_runtime_catalog` por arrays no Server ou na CLI. As tabelas locais de comandos/formatos ainda exigem revisão para que a inclusão de novos adaptadores não demande mudanças indevidas no host. Capacidades condicionadas à integração efetiva (MCP direto, bridge Pi, approvals) não podem ser inferidas somente de `managed_supported`.

Core 0.2.8 é anterior à disponibilidade pública e às correções de release durable verificadas em 0.2.9. O Connector deve consumir uma versão exata que incorpore essas entregas e a referência inequívoca de instalação do C11 quando implementada/validada. Até lá, uma seleção ambígua precisa ser recusada ou usar o próprio objeto local selecionado, nunca inventar um protocolo remoto permanente baseado no índice do array.

Contratos a entregar ao Server: catálogo seguro, avaliação por candidato, executor e revisão, candidato escolhido, estado de conexão/reconciliação, autoridade da lane, receipt/query e evento/cursor. A API do Server e sua UI são projetores dessas informações, não outra allowlist. As novas projeções devem usar versão de aplicativo acordada ou revisão NXL apropriada, respeitando schemas que proíbem campos adicionais.

## 8. Prioridade e conclusão

Primeiro corrigir fronteiras de confiança e causalidade (A02/A03/A06/A07), seleção executável (A01), controle/recovery (A04/A05/A08/A09). Em paralelo, fechar o contrato com Core (A17) e ligar o fluxo managed/remote (A10/A11). Depois concluir lane lifecycle, limites, CLI, state e serviços. O plano executor detalha dependências e gates, não exige esperar todas as plataformas para validar uma combinação delimitada.

Um teste vertical mínimo deve criar binding e iniciar um runtime pelo **Server remoto**, no host Connector, receber eventos, interromper/parar, e recuperar um recibo depois de reconectar sem reenviar a tarefa. O mesmo wheel do Core deve operar localmente no Server sem instalar Connector. Candidatos/credenciais/projetos do host remoto não existem no Server. Esse teste não foi executado aqui.

**Recomendação final:** manter a base, corrigir o Connector e iniciar integração em ambiente controlado; não considerá-lo ainda executor distribuído completo. Não há justificativa para outro proxy MCP, usuário Nexus, fork de runtime ou nova inbox. As falhas devem ser resolvidas onde ocorreram, com testes do caminho de aplicação e não apenas das bibliotecas subjacentes.

## 9. Proveniência de fontes e evidências

Fontes primárias locais: ZIP recebido; plano R3; planos/reports do Core fornecidos na conversa; wheels correspondentes. Referências remotas de código foram consultadas no commit fixo, nunca usadas para substituir o ZIP por HEAD atual. `evidencias/TRECHOS_CODIGO.md` contém as linhas numeradas; `RESULTADOS_RESUMO.json` e XMLs têm contagens verificáveis; `achados.json` relaciona causas, provas e ações.

Referências técnicas consultadas: documentação oficial Python asyncio Task/timeout (cancelamento de espera não é término de produtor físico); documentação versionada websockets 13.1 new/legacy. Referências do produto não foram inferidas dessas páginas: cada achado acima aponta ao snapshot ou a uma execução registrada.
