# Plano CN3 — correções comprovadas e fechamento do wiring CN2

**Destino:** agente executor do `okto-nexus-connector`. **Baseline auditado:** `235439242e4de8edc7ca8940b80a331275be9fb6`, `0.2.0.dev0`; Core `0.2.10.dev0`.

**Escopo:** 8 fases, 28 tarefas. Seis grupos de falhas reproduzidas e duas categorias de pendências anteriores inspecionadas. Trabalhar no HEAD atual, sem reset. Este plano complementa CN2/CN1/R3; não reduz silenciosamente o escopo original.

## Mandato

Implemente e comprove, não responda somente com outro planejamento. Preserve namespace de operação já corrigido, codec e hash verificados, controle separado, reader finito, ACK contra futuro/namespace, stop seguro nos cenários CN2, Core único e templates MCP HTTP. Não criar proxy/servidor/stdio MCP; o harness fala diretamente com Nexus Server por HTTP. Identidade e autoridade são do agente no Server. O Connector fornece host e transporte, não outra inbox nem codecs próprios.

As 12 sementes novas começam com 9 FAIL e 3 PASS no snapshot. Não são 12 bugs; CN3-01 tem quatro casos. G01/G02 são lacunas de ligação de funcionalidade anterior, não nove reproduções adicionais. Novos casos da matriz complementar começam NOT_RUN.

## Fronteiras e transições obrigatórias

| Objeto | Regra |
|---|---|
| Autoridade | Conexão autentica origem; grant autoriza ação; frame declara expectativas que devem coincidir. Resolver a sessão correta não permite descartar geração. |
| Fila | Reserva tem bytes/itens fixos e finalização única; recepção não significa admissão durável nem execução. |
| Reconcile | Persistência não aberta é estado a recuperar, não consulta bem-sucedida vazia. Namespace é do canal autenticado. |
| ACK | Enfileirado, escrito, confirmado no Server e aplicado no Core são quatro fatos. A espera testa watermark-alvo. |
| Configuração | O diretório pertence ao namespace e à execução, não ao session_id textual global. |
| Inventário | Core fornece fatos; Connector publica seu host/revisão; Server autoriza; UI apresenta. Helper sem caller não encerra fluxo. |
| Shutdown | Fim do waiter/socket não destrói produtor, journal ou controle físico em andamento. Unknown tem política de recuperação explícita. |

## Ordem de execução

CN3-00 primeiro. CN3-01 é a prioridade de isolamento e autoridade. CN3-02 pode avançar em paralelo; a fase CN3-03 usa ambas. CN3-04 e CN3-05 podem avançar depois de delimitar chaves/contratos. CN3-06 conclui ou registra bloqueios externos reais. CN3-07 não promove teste sintético a qualificação de outro host.

Os outros dois agentes podem continuar trabalhando em paralelo com pin e contrato exatos. Nenhum consumidor compensa gaps copiando adaptadores ou qualificação. Uma task já corrigida no HEAD fecha com evidência em vez de retrabalho.


## CN3-00 — Baseline e conservação do que passou

**Dependências:** nenhuma.


### CN3-00.01 — Identificar fonte, artefatos e contrato

**Achados:** CN3-01, CN3-02, CN3-03, CN3-04, CN3-05, CN3-06. **Estado inicial:** PENDING.

**Implementar:** Registrar HEAD, dirty state, versão, Core efetivo, hash do wheel e revisão NXL. Trabalhar no HEAD atual sem reset. Confrontar as âncoras do snapshot com o código existente e classificar cada achado como reproduzido, já corrigido com prova ou fixture a adaptar. Preservar o catálogo, identidade de instalação, configuração HTTP e proteções do Core.

**Não resolver assim:** Não substituir fonte pela versão do relatório nem afirmar que uma nova versão do Core corrige automaticamente o host. Não publicar, fazer push, rotacionar chaves ou executar providers com credenciais reais por efeito deste plano.

**Aceite:** Baseline e inventário dos arquivos salvos; dependências e limitações identificadas. O artefato testado é o mesmo que será entregue aos consumidores.


### CN3-00.02 — Executar sementes e preservar sua causalidade

**Achados:** CN3-01, CN3-02, CN3-03, CN3-04, CN3-05, CN3-06. **Estado inicial:** PENDING.

**Implementar:** Executar o runner deste pacote: 12 casos novos (9 FAIL/3 PASS no baseline) e 20 CN2 adaptados (20 PASS). Comparar também os 20 CN2 do executor. Ler o diff de fixtures antes de reutilizar. Barreiras devem permanecer fechadas até a asserção; operação começa válida e perde revisão durante a espera quando esse for o cenário. O hash adulterado é um controle de recusa do codec, não um bug a corrigir.

**Não resolver assim:** Não xfail/skip, trocar zero efeito por qualquer erro, instalar stub de canonicalização, preencher arquitetura por suposição ou desligar contenção. Não exigir que o nome antigo de método exista para considerar o comportamento correto.

**Aceite:** Logs/XML e estados por camada; adaptações documentadas sem relaxar expectativas. Sementes CN2 passam junto com os controles legítimos.


## CN3-01 — Uma autoridade coerente em toda mensagem e espera

**Dependências:** CN3-00.


### CN3-01.01 — Preservar o envelope e separar expectativas do grant

**Achados:** CN3-01. **Estado inicial:** PENDING.

**Implementar:** Completar ValidatedOperation com todos os campos do contrato relevantes, incluindo session_owner_generation e workspace binding/revisões pertinentes. O objeto deve conservar a origem autenticada e um token de reserva, não somente dados autodeclarados do frame. Comparar o envelope com o snapshot completo de autorização da sessão antes de construir ExecutionContext. Reutilizar os DTOs públicos do Core; adaptar apenas o envelope do aplicativo. Geração nova da conexão não significa Core já renovado.

**Não resolver assim:** Não substituir campos divergentes pelos valores da sessão para tornar a chamada aceitável. Não adotar allowed_actions, prazo ou owner a partir de um frame sem a autorização host correspondente. Não exigir nova implementação de hash: o codec real já o valida.

**Aceite:** As três variantes D01 de configuração/owner/conexão produzem zero send_turn; variante válida produz exatamente um; hash alterado continua recusado antes do efeito.


### CN3-01.02 — Revalidar a mesma reserva após adquirir capacidade

**Achados:** CN3-01. **Estado inicial:** PENDING.

**Implementar:** Na admissão capture identidade da lane, serial da conexão, ticket/credential epochs, revisão de autorização/configuração e o grant esperado. Depois da fila, confira igualdade/compatibilidade com a reserva capturada e com o contexto autorizado do Core. Rotação, detach, shutdown e nova revisão invalidam novos efeitos antes do despacho. Mantenha lookup autorizado de receipt conhecido independente da permissão para novo efeito. As transições mutáveis precisam de snapshot coerente e seção de memória curta.

**Não resolver assim:** Não comparar somente lane.state e ticket_epoch. Não ler o token corrente ao finalizar uma tentativa antiga e tratá-lo como token da reserva original. Não segurar lock global durante SQLite ou provider.

**Aceite:** D06 recusa operação da revisão 1 após a lane avançar para 2. Cobrir credential_epoch, owner e conexão. Operação já conhecida não é reenviada; controles legítimos continuam responsivos.


### CN3-01.03 — Vincular todo frame à origem autenticada antes dos handlers

**Achados:** CN3-02. **Estado inicial:** PENDING.

**Implementar:** Aplicar validação comum de namespace também a reconcile.request, approvals e detach, além de ACK e operações. Separar validação de origem da máquina de estados do protocolo: reconcile/handshake podem ser necessários antes de READY, mas jamais escolhem outro Server pelo payload. Passar envelope interno autenticado ao callback; ele recebe namespace confiável e IDs solicitados. Responder no escopo do canal. Validar session/request/binding quando o tipo de mensagem exigir.

**Não resolver assim:** Não usar str(frame.server_id) como autoridade da consulta, primeiro binding, _default_namespace de rede ou aprovação de outro Server com mesmo request_id. Não bloquear o próprio recovery exigindo READY global para toda mensagem.

**Aceite:** D02 não chama leitura do namespace A a partir de B nem devolve seu receipt. D08 próprio Server continua funcionando; erros não transportam dados estrangeiros.


### CN3-01.04 — Sincronizar renovação, rotação e gerações com o Core

**Achados:** CN3-01, G02. **Estado inicial:** PENDING.

**Implementar:** Guardar o ExecutionContext autorizado completo na sessão. Adotar uma renovação apenas depois de a operação pública do Core confirmar ou de reconciliação autorizada concluir a tentativa correta; trocar snapshot atomicamente e preservar unknown. Não atualizar só o deadline ou allowed_actions. Conservar expires_at, credential_epoch e revisions retornados para tickets/lanes; atualizar lanes existentes por diff e renovar single-flight antes da expiração conforme contrato do Server. Enquanto não sincronizado, novo trabalho fica indisponível com diagnóstico.

**Não resolver assim:** Não ampliar ações de controle via allowed.update para responder a pedidos remotos sem grant; exceção temporal do Core não concede novas capabilities. Não converter recebimento de mensagem em mais 90 segundos de autorização. Não inferir autoridade de linha durável incompleta.

**Aceite:** Reconexão genuína avança contexto no Core antes de novo envio; rotação de A não altera B; ticket expirado não fica silenciosamente READY. Casos de sucesso, ACK perdido e falha de renovação são distintos.


### CN3-01.05 — Uniformizar chaves, remoções e respostas públicas

**Achados:** CN3-01, CN3-02, CN3-06. **Estado inicial:** PENDING.

**Implementar:** Usar Server/executor/binding/sessão/epoch conforme o domínio de cada registro, task, diretório, approval e ACK. A API humana pode aceitar ID curto apenas após resolução inequívoca; a rede não usa esse atalho. Remover concatenação ambígua de binding#session onde houver chave pública tipada. Documentar quais campos são namespace e quais são revisão. Preservar a compatibilidade persistida por migração aditiva quando preciso.

**Não resolver assim:** Não pressupor unicidade global porque IDs normalmente são UUIDs. Não criar nova identidade de agente para identificar instalação ou sessão. Não mudar hashes e IDs de operações antigas.

**Aceite:** Mesmos IDs em A/B permitem consultar/controlar cada recurso exato; erro de contexto não é corrigido automaticamente. Remoção de A preserva B e seu histórico.


## CN3-02 — Contabilização exata e liberação única de admissão

**Dependências:** CN3-00.


### CN3-02.01 — Materializar a reserva com custo calculado uma vez

**Achados:** CN3-03. **Estado inicial:** PENDING.

**Implementar:** Fazer try_reserve produzir uma reserva identificada com classe de trabalho, bytes cobrados e item cobrado, ou uma recusa. Calcular o custo com uma política única documentada antes da task; conservar esse número até finalizar. release recebe a reserva, não outro frame reconstruído. Finalizar uma única vez e detectar inconsistência em vez de escondê-la com clamp do contador.

**Não resolver assim:** Não subtrair len(json.dumps(_as_error_frame_payload(...))). Não aumentar limites para compensar o contador residual. Não trocar o custo pelo tamanho do payload apenas sem aplicar a mesma política no ingresso.

**Aceite:** D03 termina com waiting_items=0 e waiting_bytes=0. Frames válidos de comprimentos diferentes e campos opcionais não geram resíduo nem saldo negativo.


### CN3-02.02 — Cobrir todos os caminhos de finalização e de recusa

**Achados:** CN3-03. **Estado inicial:** PENDING.

**Implementar:** Usar ownership da reserva desde a criação até finally do trabalho. Se create_task falhar, invalidar a lane ou cancelar antes de adquirir semáforo, liberar somente a reserva daquele trabalho. Recusas por capacidade não devem gerar uma população ilimitada de tasks de erro. Resultado já admitido permanece consultável no journal quando a fila de saída está cheia; enfileirar não significa escrever.

**Não resolver assim:** Não liberar duas vezes quando watcher e handler finalizam simultaneamente. Não fazer task por erro como substituto de backpressure nem apagar receipt quando enqueue recusa.

**Aceite:** Testes de exceção, cancelamento, invalidação pós-fila e cheia preservam contadores exatos; quatro submits bloqueados continuam sem impedir interrupt.


### CN3-02.03 — Demonstrar capacidade sob carga finita sustentada

**Achados:** CN3-03. **Estado inicial:** PENDING.

**Implementar:** Configurar quotas pequenas em teste, repetir ciclos de frames longos/curtos até exceder o total cumulativo do limite, mas não o volume em voo. Verificar que ocioso recupera toda capacidade; em carga concorrente nunca ultrapassa itens/bytes projetados. Medir unidades admitidas e efeitos, não só tamanho de _inflight. Manter versão pública dos parâmetros se já existir.

**Não resolver assim:** Não declarar memória limitada porque um semáforo possui quatro slots. Não usar sleeps sem barreira quando o teste exige estado em voo.

**Aceite:** Operações sequenciais não sofrem recusa falsa por quota histórica; concorrentes acima do limite são recusadas antes do efeito. Controles e lossless queue CN1 permanecem verdes.


## CN3-03 — Recuperação durável e ACK como condição monotônica

**Dependências:** CN3-01, CN3-02.


### CN3-03.01 — Abrir o journal para história independentemente do runtime

**Achados:** CN3-04. **Estado inicial:** PENDING.

**Implementar:** Adicionar/usar uma entrada pública assíncrona do host para obter o journal histórico correto, single-flight e fora do loop bloqueante. Startup deve recuperar esse estado antes de declarar reconciliação bem-sucedida. RuntimeManager.reconcile e _journal_page não podem tratar journal ainda não aberto como evidência de ausência. Consultar recibos/claims sem candidate_for, prepare ou factory nativa. Validar origem primeiro.

**Não resolver assim:** Não exigir executar um harness para abrir o banco; não usar SQL paralelo na aplicação; não devolver vazio por erro de inicialização. Não confundir criar banco novo em diretório errado com ausência de histórico.

**Aceite:** D04 retorna op_durable num host novo depois de aclose do anterior, sem compose/start. Semente CN2 de executável ausente também passa. Falha de read mantém READY produtivo pendente.


### CN3-03.02 — Recuperar obrigações e streams mesmo sem memória de sessões

**Achados:** CN3-04, G02. **Estado inicial:** PENDING.

**Implementar:** Ler claims e cursores por páginas públicas na inicialização/reconexão. Construir histórico/unknown e obrigações de receipt/eventos sem assumir processo vivo ou morto. Separar consultas com IDs explícitos de enumeração inicial limitada; o handshake não pode sempre retornar relatório vazio porque enviou listas vazias. Não depender de um evento novo para acordar registros antigos. Agregar por namespace e não pelo primeiro runtime.

**Não resolver assim:** Não reabrir pipes, fazer attach por PID ou repetir último prompt. Não apagar registro porque seu binding mudou; seu acesso exige autorização apropriada ao histórico.

**Aceite:** Crash depois de commit e antes de publicação mantém receipt/stream recuperável; restart sem binário nem novo evento drena o que ficou pendente, sem novo efeito de agente.


### CN3-03.03 — Fazer espera de ACK depender do watermark-alvo

**Achados:** CN3-05. **Estado inicial:** PENDING.

**Implementar:** Conservar acked_through validado por conexão/stream. A espera recebe ou deriva um through_sequence explícito; antes e depois de qualquer espera verifica o predicado acked_through>=target. Replay não apaga a prova que já foi recebida. ACK igual pode despertar waiters sem avançar cursor; ACK regressivo não retrocede. Fechar a corrida entre observar estado e registrar Event/Condition, preservando namespaces e serial da conexão.

**Não resolver assim:** Não resolver apenas com waiter.set indiscriminado. Um ACK1 não conclui a espera de lote até2. Não aceitar ACK futuro nem transportar confirmação da conexão substituída sem a reconciliação definida.

**Aceite:** D05 retorna ACK1 para o replay de seq1. Controle novo seq2 permanece esperando até ACK2; duplicatas válidas são idempotentes e não disparam efeitos adicionais.


### CN3-03.04 — Separar ACK remoto e aplicação durável no Core

**Achados:** CN3-05, CN3-04. **Estado inicial:** PENDING.

**Implementar:** Registrar obrigação de aplicar ao Core um watermark já validado. Se acknowledge_events falhar, manter a prova e retentar com backoff/coalescência limitada; não exigir um ACK novo para o mesmo fato. Se sender ou journal reader falhar, pump supervisionado deve poder retomar. Tratar escrita de socket, fila aceita e ACK como fatos diferentes. Confirmar progresso ao redor de falha após commit e confirmação perdida.

**Não resolver assim:** Não avançar cursor do Core pelo último evento enfileirado nem suprimir o stream até outra mensagem do agente. Não usar nova tarefa de agente para gerar wake-up. Não criar inbox paralela.

**Aceite:** Falha transitória no Core ACK recupera com peer que só repete o mesmo watermark; journal não perde seq1, nenhum replay de trabalho, tarefas limitadas e erros observados.


### CN3-03.05 — Fixar intenção e publicação sem segunda mutação de trabalho

**Achados:** G02. **Estado inicial:** PENDING.

**Implementar:** Concluir o acordo CN2 com Server: identificador estável antes do primeiro POST mutável, autorização/resolução distinta da execução e publicação de receipt distinta de novo runtime/operation. Conservar obrigação de publicar o recibo produzido pelo Core. Erro de resposta traz ID consultável e possível efeito coerente. A ausência de endpoint acordado fica explicitamente bloqueada antes de alegar idempotência completa.

**Não resolver assim:** Não mandar só text_length a um endpoint de execução dizendo que é receipt. Não criar outro operation_id depois de ReadTimeout para descobrir o resultado. Não implementar contrato unilateral em mocks e apresentar como Server real.

**Aceite:** Peer conta uma intenção e no máximo um efeito sob ACK perdido/restart; consulta histórica encontra o mesmo receipt. Handoff registra schema, owners e resultado que ainda depende do Server.


## CN3-04 — Configuração efêmera com ownership por namespace

**Dependências:** CN3-01.


### CN3-04.01 — Delimitar diretório pelo dono completo da execução

**Achados:** CN3-06. **Estado inicial:** PENDING.

**Implementar:** Derivar uma chave local opaca de Server/executor/binding/sessão e epoch/tentativa quando necessário. Validar segmentos e confinar a criação ao root aprovado. A mesma execução pode reutilizar seu próprio diretório; outra execução/namespace jamais o sobrescreve. Usar recibo de ownership local ou metadados mínimos para identificar quem pode atualizar/remover a configuração.

**Não resolver assim:** Não usar session_id isolado, índice de lista ou display_name. Não reutilizar token de outra sessão por possuir mesma instalação. Não adicionar identidade de usuário Nexus.

**Aceite:** D07 mantém diretórios e config separados para A/B com session_id igual. Leituras no primeiro permanecem idênticas depois de criar o segundo; paths externos não são alcançados.


### CN3-04.02 — Aplicar e limpar configuração de modo atômico e seletivo

**Achados:** CN3-06. **Estado inicial:** PENDING.

**Implementar:** Escrever o conjunto de arquivos em staging sob o dono correto e aplicar atomically/CAS quando houver edição concorrente. Definir cleanup apenas após o recurso correspondente resolver suas obrigações, preservando unknown. Tokens de sessão permanecem no mecanismo qualificado do Core, sem segredo global no arquivo. Repetir preparação deve ser idempotente para o mesmo owner e detectar drift.

**Não resolver assim:** Não remover diretório inteiro do parent para limpar uma sessão; não sobrescrever arquivos de terceiros nem trocar URL/token da execução em andamento silenciosamente. Não voltar ao MCP proxy.

**Aceite:** Falha entre criar e aplicar não deixa configuração misturada; cancelamento/restart/retry preserva A e permite completar B. Cópias legítimas de mesma família têm seleção e config inequívocas.


### CN3-04.03 — Qualificar cliente direto e autenticação do provider no contexto efêmero

**Achados:** CN3-06, G02. **Estado inicial:** PENDING.

**Implementar:** Preservar o renderer público do Core. Demonstrar no adapter/harness qualificado que URL e referência do bearer são consumidas junto das credenciais de provider aprovadas. A mudança de HOME pode alterar descoberta de login; mapear isso por provider sem copiar home inteiro, hooks não aprovados ou todos os segredos. Se depender de cofre do SO, testar o modo daemon do SO-alvo separadamente.

**Não resolver assim:** Não assumir que variável de token configura cliente completo nem que arquivos gerados provam login real. Não usar --dangerously-skip-permissions como correção de onboarding.

**Aceite:** Teste de processo/peers verifica arquivos/ambiente; campanha real separada confirma MCP HTTP harness→Server sem tráfego pelo Connector, com tokens diferentes por sessão.


## CN3-05 — Ligar o inventário único ao fluxo de seleção

**Dependências:** CN3-00, CN3-01.


### CN3-05.01 — Compor snapshot técnico no executor e publicá-lo explicitamente

**Achados:** G01. **Estado inicial:** PENDING.

**Implementar:** Usar catalogue/discovery/evaluate_runtime_availability do Core no serviço efetivamente chamado por CLI/daemon. Publicar uma projeção acordada e versionada na API do aplicativo ou no transporte com schema adequado. Preservar core_version, formato, executor, referência/estado/razões e revisão de evidência/TTL. Evitar uma segunda allowlist. Listagem passiva não abre provider nem concede autorização.

**Não resolver assim:** Não considerar availability_snapshot concluído se só testes o chamam. Não inserir campos novos num frame NXL que proíbe additionalProperties. Não usar o SO do Server para avaliar o host remoto.

**Aceite:** Chamada pelo endpoint/IPC real do Connector devolve snapshot Core; Server peer recebe o mesmo executor/versionamento. Catálogo segue sem array autoritativo duplicado.


### CN3-05.02 — Resolver o candidato que o usuário realmente selecionou

**Achados:** G01. **Estado inicial:** PENDING.

**Implementar:** Carregar candidate_ref e revisão desde a opção apresentada até o executor original. Invocar resolve_installation público sobre o mesmo inventário autorizado e passar o candidato completo ao Core/BindingRecord. Ref legada ambígua exige reseleção; nenhuma lista manual de runtimes nos aplicativos. Rótulos são apenas apresentação.

**Não resolver assim:** Não limitar a entrega a importar resolve_selection ou testar seu wrapper. Não usar matches[0], candidato por nome ou índice persistido. Não substituir fingerprint de conteúdo por ref de instalação.

**Aceite:** Fluxo CLI/serviço seleciona B entre duas cópias iguais, persiste B e prepara B após reordenar inventário. Migração v1 ambígua recusa; outra versão/SO não vira READY por omissão.


### CN3-05.03 — Unificar revisão do inventário e invalidar evidência antiga

**Achados:** G01. **Estado inicial:** PENDING.

**Implementar:** Remover uso da versão de schema do StateStore como revisão do conteúdo do inventário. Derivar revisão canônica dos fatos relevantes (refs, conteúdo/build/arquitetura, qualificação/contenção/configuração) ou manter revisão monotônica local por mudança observada. Escolher um único owner. Atualização de bytes sem mudar version string ainda invalida evidência anterior. TTL/online/revisão entram na elegibilidade do host; revalidar prepare/open continua necessário.

**Não resolver assim:** Não criar uma revisão no helper e outra no binding sem relação. Não autorizar start só por cache de READY nem alterar identidade do agente quando um binário muda.

**Aceite:** Fixture de snapshot→binding usa a mesma revisão; mutação do build entre seleção e prepare recusa ou exige aprovação atual. UI real permanece gate do Server, não prova fictícia do Core.


## CN3-06 — Fechar ou delimitar os verbos e o lifecycle ainda pendentes

**Dependências:** CN3-01, CN3-03, CN3-04.


### CN3-06.01 — Implementar abertura remota pelo mesmo contrato autorizado

**Achados:** G02. **Estado inicial:** PENDING.

**Implementar:** Acordar com Server o comando/runtime.open, grant e receipt usando o bundle vigente ou evolução versionada. Reutilizar o caminho local de resolução/preparação por instalação sem round-trip que crie outra intenção. Abertura remota deve funcionar sem CLI pré-iniciar a sessão. Se contrato externo ainda indisponível, manter erro tipado e tarefa BLOCKED; não declarar executor remoto completo.

**Não resolver assim:** Não anunciar open porque turn.submit funciona com sessão criada pela fixture. Não usar um processo novo para tentar descobrir o resultado de open anterior incerto.

**Aceite:** Peer de contrato executa binding→open→submit→events→interrupt→close com IDs preservados; Server real/multi-host tem campanha separada.


### CN3-06.02 — Ligar decisão autorizada ao Core e preservar pedido bifásico

**Achados:** G02, CN3-02. **Estado inicial:** PENDING.

**Implementar:** Atribuir chave completa à solicitação de aprovação, validar antes de reservar e não fazer pop antes de confirmação. Separar transporte da decisão humana ao Server de aplicação da decisão autorizada no runtime. Chamar RuntimeManager/Core.decide_native_approval por rota final explícita com ação/request/turn corretos; prazo produtivo não isenta accept. Recusa pré-byte pode liberar sua reserva; escrita incerta não permite segunda resposta.

**Não resolver assim:** Não inferir permissão de operador da chave do próprio agente; não achar binding por texto sem Server; não declarar wiring porque o método funciona só no teste unitário.

**Aceite:** CLI/IPC→Server peer→decisão autorizada→Core real chega ao adapter de laboratório. Decisão inválida não consome pedido; accept expirado não envia; negativa correta segue contrato.


### CN3-06.03 — Compor capacidades opcionais reais sem duplicar adaptadores

**Achados:** G02. **Estado inicial:** PENDING.

**Implementar:** Ligar callbacks públicos de Pi native action, Codex resume e approvals opt-in somente nas combinações qualificadas. Preservar referência de credencial por sessão e contexto do Server. O driver/protocolo permanece no Core. O inventário deve refletir capacidade indisponível quando backend/callback necessário não está configurado. Manter attach não qualificado indisponível.

**Não resolver assim:** Não copiar bridge JS, parsers ou codecs no Connector. Não usar texto livre para claim/complete nem MCP stdio/proxy como alternativa. Não habilitar capability apenas para o teste passar.

**Aceite:** Teste atravessa import/composição do aplicativo, adapter de laboratório e backend HTTP autorizado. Campanha real de cada provider separada; pendência é informada ao seletor/diagnóstico.


### CN3-06.04 — Dar saída explícita ao daemon com recursos e produtores incertos

**Achados:** G02. **Estado inicial:** PENDING.

**Implementar:** Escolher uma política operacional e comprová-la: manter modo drain com IPC/recovery limitado até resolver, ou transferir containment/ownership para mecanismo de SO realmente qualificado. Integração do shutdown não cancela indiscriminadamente produtores do Core ou trabalhos aceitos pelo transporte. Reports de unknown e release pendente conservam chaves e next action; dois recursos têm budgets independentes. Exit code não substitui prova.

**Não resolver assim:** Não retornar 1 e alegar que a próxima chamada ocorrerá num loop Python que já saiu. Não fechar journal/ledger antes do produtor que ainda pode comitar. Não kill por PID isolado nem abandonar Future por timeout.

**Aceite:** Peer/real backend conforme camada demonstra retorno bounded e recuperação do mesmo recurso; nenhuma operação é reexecutada por reconexão. Política de logout/boot/guardian é testada ou BLOCKED.


## CN3-07 — Evidência proporcional e artefato para integração

**Dependências:** CN3-01, CN3-02, CN3-03, CN3-04, CN3-05, CN3-06.


### CN3-07.01 — Executar matriz e comparar as fronteiras efetivas

**Achados:** CN3-01, CN3-02, CN3-03, CN3-04, CN3-05, CN3-06, G01, G02. **Estado inicial:** PENDING.

**Implementar:** Executar as sementes novas duas vezes, os 20 CN2 adaptados, CN2 do executor e a suíte fornecida no ambiente disponível. Corrigir fixtures POSIX/probes em camada unitária sem alterar gates do produto. Para cada task declarar: código ligado, prova sintética, backend do SO, provider e consumidores reais. Mapear ponto de entrada, espera, evento contado e resultado.

**Não resolver assim:** Não somar repetição/subconjunto, nem converter skip em PASS. Não elevar testes de wrapper a evidência da aplicação inteira. Não declarar retorno desconhecido como graceful/forced sem fatos.

**Aceite:** Nove falhas passam, três controles permanecem verdes, condições antigas preservadas. Cada G recebe prova de wiring ou owner/BLOCKED explícito, não DONE global.


### CN3-07.02 — Construir wheel e testar consumo externo com pin exato

**Achados:** CN3-01, CN3-02, CN3-03, CN3-04, CN3-05, CN3-06. **Estado inicial:** PENDING.

**Implementar:** Construir wheel/sdist, fixar dependências testadas e hashes, instalar fora da fonte. Rodar CLI, catalogue/availability/seleção, frio de journal, tipos/erros e testes novos contra o import instalado. Quando versão da API/DTO muda, entregar handoff aos agentes Server/Core sem exigir imports privados. Usar artefato do Core escolhido de forma explícita, não último wheel de pasta irmão.

**Não resolver assim:** Não modificar a fonte original durante auditoria nem publicar no PyPI/push por autorização implícita. Não afirmar identidade do wheel de outro agente sem conferir seu hash.

**Aceite:** Import python -I e runner instalados passam; envelope/NXL/projeção compatíveis ou revisados explicitamente. Migrações conservam agentes/chaves/histórico/unknown.


### CN3-07.03 — Emitir decisão por escopo e fechar status documental

**Achados:** G01, G02. **Estado inicial:** PENDING.

**Implementar:** Revisar afirmações da evidência CN2: envelope completo, N09 published, history recovered e callbacks aplicados precisam corresponder às rotas do código. Preservar histórico e registrar o delta desta rodada. G0 permite desenvolvimento; G1 exige fechar falhas de laboratório no escopo anunciado; G2 exige uma integração vertical real local/remota com mesmo Core; G3 exige o restante da matriz original. Não confundir maturidade do Core com maturidade do Connector.

**Não resolver assim:** Não reabrir a arquitetura inteira nem inventar novo achado de estilo. Não excluir pendência externa para declarar produto integralmente pronto.

**Aceite:** Relatório começa pelo gate/escopo alcançado, lista commits/campanhas/migrações/limites e informa de maneira operacional o que o próximo agente pode integrar.


## Evidência exigida por tarefa

Guardar task_id, commit, campos/API alterados, command, exit code, test nodes, SO/Python, Core/wheel/hash, XML/log e pendências. Atualizar backlog e matriz separadamente. Nenhuma autorização para publicação, uso de credenciais reais ou mudanças nos outros repositórios acompanha este plano.
