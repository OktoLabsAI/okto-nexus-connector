# CN4 — correções localizadas do Connector 0.3.0.dev0

**Baseline:** `e9307975a544c584fc64fe54c906ad512ae1b82b`. **Core:** `0.2.10.dev0`. **Destinatário:** agente executor do `okto-nexus-connector`.

## Missão

Implementar os cinco grupos P01–P05 sobre o HEAD real, preservando CN3/CN2/CN1. Não responder apenas com outro plano. O ZIP inclui reproduções; usar os arquivos, não reconstruí-los a partir da narrativa. Não reescrever a biblioteca de runtimes, duplicar catálogo/qualificação nos consumidores ou criar servidor/proxy/stdio MCP. Harnesses MCP continuam diretamente conectados ao Server por HTTP.

Não há demonstração de bypass nativo completo nesta revisão: o teste de ordem usa sentinela e a integração real atual recusa as strings de decisão. Corrigir os dois aspectos conjuntamente; a recusa acidental por vocabulário não é um mecanismo de autorização. O namespace de aprovação foi testado separadamente e encaminhou a proposta ao Server errado. Os outros achados são de integridade de configuração, evidência e progresso operacional.

## Invariantes de aceite

| Objeto | Invariante |
|---|---|
| Aprovação | Proposta local não é decisão autorizada; confirmação canônica precede efeito permissivo; escopo completo até binding/sessão/pedido. |
| Recuperação | Timeout encerra a espera, não prova rollback nem encerra o produtor; idempotência por intenção. |
| Evento | ACK1 pode satisfazer replay1, não lote2. Confirmado no Server e aplicado no Core são fatos distintos. |
| Configuração | IDs completos distintos não colidem após a derivação do diretório; marker é conferido antes de escrever. |
| Inventário | O candidato avaliado é o mesmo descoberto/selecionado; revisão identifica conteúdo/evidência, não só nome/versão/estado. |
| Lane | Reload aplica rotação real, reanexa sob nova prova e invalida permissões antigas sem reabrir contexto por suposição. |

## Estados mínimos a preservar

**Decisão:** PENDING → SERVER_PENDING → SERVER_CONFIRMED → NATIVE_PENDING → APPLIED. RESULT_UNKNOWN é separado de recusa anterior ao efeito. Negativa e aprovação possuem ações/correlações próprias. Uma nova tentativa de UI não cria automaticamente nova intenção.

**Stream:** JOURNALED → ENQUEUED → WRITTEN → REMOTE_ACKED → CORE_ACK_APPLIED. O alvo da espera pertence ao batch e não muda por ACK antigo.

**Lane:** READY antigo → FENCED/PENDING → ATTACHING sob token atual → READY novo somente com prova. Falha pode permanecer recuperável, nunca restaurar implicitamente o epoch antigo.

## Execução por fases

CN4-00 antecede tudo. Aprovações e suas chaves são prioridade. ACK, configuração e inventário podem avançar paralelamente; lanes usam o mesmo modelo de scope. A última fase exige evidências de todas as anteriores. Novas APIs dos aplicativos devem ser acordadas; uma dependência externa real é BLOCKED, não motivo para fabricar sucesso local.

## CN4-00 — Baseline e conservação

**Dependências:** nenhuma.

### CN4-00.01 — Fixar o snapshot e as evidências

**Implementar:** Registre HEAD, branch, dirty state, Python, SO, pacote do Core, hashes de fonte e wheel. O baseline desta revisão é e930797/0.3.0.dev0; não é uma instrução para retornar a ele. Copie este pacote inteiro, inclusive regressoes e runner. Compare os cinco grupos ao HEAD e classifique correção posterior com prova, sem refazer trabalho válido. Preserve dados de agentes, segredos referenciados, bindings e journal.

**Não resolver assim:** Não reconstruir os testes a partir só do Markdown nem fazer reset/descarte de alterações posteriores.

**Aceite obrigatório:** Baseline identificável e comparação de arquivos guardada. As 139 entradas originais são referência histórica, não número obrigatório para o novo HEAD.

### CN4-00.02 — Executar reproduções e controles antes de editar

**Implementar:** Rode executar_verificacao.py com o Core fixado. O conjunto CN4 tem 14 casos: 11 FAIL e três controles PASS neste snapshot; CN3 original adaptado tem 12 PASS. A única adaptação anterior foi acrescentar ensure_history_journal ao double que devolve o mesmo journal. Conserve a barreira de ACK2 ausente, IDs longos completos, dois Servers com binding textual igual e o par real Node/CLI. Separe testes de roteamento com sentinela dos que atravessam o Core.

**Não resolver assim:** Não inverter expectativas, truncar IDs na fixture, atribuir fingerprints artificiais distintos ou liberar a barreira para obter PASS.

**Aceite obrigatório:** Logs/XML e comandos antes/depois; o controle de accept direto no Core continua operacional e a operação legítima CN3 permanece verde.

## CN4-01 — Aprovação autorizada, escopada e recuperável

**Dependências:** CN4-00.

### CN4-01.01 — Resolver uma chave completa até o último recurso

**Implementar:** Introduza/complete ApprovalKey imutável com Server/executor/request e identidade do binding/sessão conforme o contrato. Preserve a origem autenticada recebida do transporte. Ao receber uma escolha curta de CLI, encontre exatamente um pedido ou devolva ambiguidade com alternativas; não use o primeiro. Busque binding por Server + executor + binding_id e compare agente/workspace. Passe SessionKey ao manager para não voltar a resolver session_id isolado. Use nomes distintos approval_key e agent_secret_handle/agent_key; o segredo nunca pode substituir a chave do dicionário.

**Não resolver assim:** Não derivar Server com split(binding_id), escolher first binding nem usar dados da sessão encontrada para remapear o pedido.

**Aceite obrigatório:** P01c envia exclusivamente à origem B com identidade B. A/B com mesmos request_id/binding_id/session_id permanecem isolados; remover A não remove B.

### CN4-01.02 — Separar proposta do operador de decisão confirmada

**Implementar:** A primeira operação externa do fluxo permissivo deve ser a decisão canônica no Server. Conserve a proposta local como intenção, não autorização. Após confirmação, valide o scope do resultado, pedido/hash/turno e decisão permitida; só então aplique no Core. Um resultado aplicado=false, conflito, recusa, revisão incompatível ou resposta indeterminada não autoriza callback nativo. Se o contrato do Server ainda não fornece evidência suficiente, deixe essa rota tipadamente bloqueada e documente o handoff; não simule autorização no Connector.

**Não resolver assim:** Não corrigir somente approve→accept deixando native antes do Server. Não confundir a autenticação do agente que transporta a mensagem com a autoridade humana exigida para a aprovação.

**Aceite obrigatório:** P01b tem zero chamadas de aplicação nativa quando Server recusa. Controle autorizado mostra Server confirmado → Core → peer, nessa ordem, com contexto e operação identificáveis.

### CN4-01.03 — Traduzir decisões pelo contrato público do Core

**Implementar:** Mapeie explicitamente a decisão confirmada: approve→accept e negativa→decline/cancel somente de acordo com a semântica do pedido. Construa NativeApprovalOperation com argumentos nomeados e request originalmente observado. Não derive input.provide por decision.startswith: input é determinado pelo tipo do pedido e pelo contrato. O contexto vem do grant completo já validado, sem acrescentar capabilities. Respostas negativas podem ter dispensa da lease produtiva prevista no Core, nunca dispensa de alvo/identidade/correlação. Não duplique codecs nativos no aplicativo.

**Não resolver assim:** Não enviar approve/deny diretamente ao Core; não converter decisão desconhecida em accept; não tratar todo CoreError(VALIDATION_ERROR) como sessão externa.

**Aceite obrigatório:** P01 approve/deny passam contra Core e journal reais; negativo não vira positivo, input incompatível é recusado antes dos bytes. Controle público do Core permanece verde.

### CN4-01.04 — Dar ownership e finalização única à tentativa

**Implementar:** Mantenha DecisionAttempt com token, chave do pedido, intenção local estável, resultado canônico, operação Core, Future produtor e recibo. Reserve sob seção curta; um segundo pedido em deciding compartilha consulta ou recebe conflito, sem nova aplicação. Estado proposto: PENDING → SERVER_PENDING → SERVER_CONFIRMED → NATIVE_PENDING → APPLIED; recusa comprovada libera a própria reserva, resultado incerto permanece consultável. Não fazer pop por segredo nem apagar todas as evidências ao completar; histórico/tombstone limitado pode preservar idempotência.

**Não resolver assim:** Não devolver pending após qualquer exceção; não reutilizar só op_appr_request_id sem scope/identidade da decisão; não executar duas chamadas porque o waiter anterior expirou.

**Aceite obrigatório:** Dois operadores concorrentes não geram duas aplicações. Pedido terminal é retirado da lista pendente por sua chave correta e repetição consulta o mesmo resultado.

### CN4-01.05 — Conservar causalidade entre dois resultados duráveis

**Implementar:** Distinga confirmação perdida no Server de write possível no Core. Antes do primeiro POST mutável, registre a intenção estável pelo mecanismo acordado com o Server; não cunhar outra intenção a cada retry. Se o Server confirmou e a aplicação falhou antes do byte, retome somente a aplicação autorizada ainda válida; se houve write possível, consulte o recibo Core. Cancelar IPC encerra a espera, não a obrigação. Guarde pending/reconciliation por scope com quotas; nenhuma credencial ou resposta de operador sensível entra em diagnóstico bruto.

**Não resolver assim:** Não desfazer uma confirmação canônica por erro local; não repetir mensagem de aprovação para descobrir se o runtime aceitou; não apresentar unknown como não executado.

**Aceite obrigatório:** Falhas antes/depois do POST, antes/depois do write e na publicação de recibo têm resultado explícito. No máximo uma aplicação nativa por intenção, ou UNKNOWN com consulta, sem duplicação cega.

### CN4-01.06 — Testar o percurso real e documentar suporte

**Implementar:** Exercite a entrada IPC/CLI efetiva, o peer Server controlado, o manager, o Core e o writer/peer nativo apropriado. Inclua proposta nativa válida e proposta administrativa sem sessão gerenciada: são rotas distintas. Habilite callbacks de produção somente quando compostos e qualificados; se ainda ausentes, declarar a disponibilidade real. Atualize a evidência que hoje afirma G02 wired para apontar às entradas executadas, e não a testes que nunca tinham uma sessão viva.

**Não resolver assim:** Não promover a sentinela que aceita qualquer decisão a prova do provider ou a aprovação completa de segurança.

**Aceite obrigatório:** Matriz relaciona cada cenário a camada e rota. Falha do Server garante zero efeito nativo; controles de contexto/gerações do CN3 continuam passando.

## CN4-02 — ACK do lote correto no publicador de eventos

**Dependências:** CN4-00.

### CN4-02.01 — Propagar o watermark-alvo do lote

**Implementar:** No EventBridge, fixe target a partir do lote efetivamente aceito para envio (normalmente batch[-1].sequence) e passe-o a wait_event_ack. O predicate não deve ser current>0 em uma espera por lote novo. Se o produto admitir ACK parcial, defina um limiar de progresso e mantenha o restante pendente sem laço de ACK antigo. Preserve escopo da conexão/stream e a distinção queued/written/remote-acked/Core-applied. O alvo é imutável para aquela espera, não derivado novamente de cursor que outra chamada pode alterar.

**Não resolver assim:** Não apagar ACK1 para resolver batch2; isso reabre o defeito CN3 de replay. Não tratar igualdade com watermark antigo como progresso do lote novo.

**Aceite obrigatório:** P02 faz somente uma tentativa de batch2 enquanto ACK2 está ausente. Replay1 com ACK1 já conhecido continua imediato; ACK1 não libera batch2.

### CN4-02.02 — Recuperar sem reenvio apertado nem perder obrigação

**Implementar:** Depois de timeout, fila cheia ou ACK aplicado ao Core com falha, mantenha uma obrigação por stream com cursor remoto confirmado e cursor durável aplicado. Use wake/backoff/coalescência com limites; não releia/reenvie batch2 repetidamente no mesmo loop por ACK1. Reconexão acorda streams mesmo sem novos eventos. Cancelamento de follower não descarta a obrigação. Nenhuma recuperação de evento repete a tarefa do agente.

**Não resolver assim:** Não criar outra inbox ou armazenar payloads ilimitados na RAM; não repetir acknowledge_events sem progresso em laço ocupado.

**Aceite obrigatório:** Um erro transitório de Core ACK converge na retomada; dois publishers/ACKs concorrentes mantêm ordem e bounds; nenhum novo send_turn.

### CN4-02.03 — Ampliar a prova do caller, não só do helper

**Implementar:** Execute controles de 0/1/128/129 eventos, replay, ACK atrasado/duplicado/parcial, troca de conexão e erro ao aplicar confirmação. Use o EventBridge real e NXLTransport real ao menos nos testes de predicate; observe número de leituras e batches, não apenas return do waiter. O teste adicional desta auditoria contém barreira para limitar o flood do baseline; mantenha a condição ACK2 ausente.

**Não resolver assim:** Não chamar somente wait_event_ack(target=2) num teste e declarar seu caller corrigido.

**Aceite obrigatório:** CN3 D05 continua verde; P02 passa duas vezes na fonte e no wheel, sem erros de task não observados.

## CN4-03 — Diretório de configuração por ownership inequívoco

**Dependências:** CN4-00.

### CN4-03.01 — Substituir sanitização destrutiva por chave versionada

**Implementar:** Derive diretório limitado a partir da tupla completa Server/executor/binding/session e da tentativa de abertura quando exigido pelo lifecycle. Use serialização canônica sem separador ambíguo e digest forte com prefixo de versão. Prefixo legível é opcional, nunca a chave sem o digest completo pertinente. O algoritmo deve distinguir IDs válidos longos e caracteres que a sanitização hoje substitui. Caminhos continuam locais e não viram identidade de agente ou qualificação do build.

**Não resolver assim:** Não apenas aumentar 80 para outro limite; não truncar campos autorizados; não remapear colisão para primeira pasta encontrada.

**Aceite obrigatório:** P03 IDs de 85 caracteres gera diretórios distintos e A permanece intocado. Controle de nomes curtos continua funcionando; códigos e paths não são usados como autorização.

### CN4-03.02 — Validar marker e escrever de modo transacional

**Implementar:** Antes de reutilizar a árvore, leia marker e compare o proprietário completo. Marker divergente ou sem evidência suficiente deve recusar/reconciliar, nunca ser sobrescrito antes da validação. Faça criação concorrente e marker/config com staging/replace e permissões locais adequadas, observando links e escapes conforme o modelo de confiança. Remoção alcança somente arquivos comprovadamente próprios. Mantenha secrets fora do marker e dos logs.

**Não resolver assim:** Não confiar no comentário validated on reuse enquanto o código regrava o marker incondicionalmente; não apagar árvore de outro binding por prefixo.

**Aceite obrigatório:** Duas gerações concorrentes não misturam config; marker estrangeiro recusa antes de escrever; cleanup A preserva B e configurações externas.

### CN4-03.03 — Migrar sem alterar processos ativos

**Implementar:** Versione o layout das árvores novas. Para sessões antigas, conserve a referência exata ao diretório em uso até resolução; não mover HOME/config durante execução e não inferir owner por nome truncado. Uma árvore ambígua exige encerramento seguro/reseleção em vez de migração automática. Migração preserva agentes/chaves e journal. Declare tratamento de caminhos longos Windows separadamente da prova unitária portátil.

**Não resolver assim:** Não renomear diretório ativo para aparentar que o novo algoritmo estava em vigor; não publicar serviço MCP para evitar arquivos.

**Aceite obrigatório:** Sessão existente encerra pelo owner original; nova sessão usa layout novo; erro durante apply não destrói a configuração anterior. MCP segue HTTP direto.

## CN4-04 — Inventário fiel, revisão única e seleção pública

**Dependências:** CN4-00.

### CN4-04.01 — Preservar o candidato inteiro até avaliação

**Implementar:** Reutilize InstallationCandidate produzido pelo Core no pipeline de descoberta/seleção. Se mantiver InventoryEntry, defina conversão pública completa e sem inferências: executable, launch_script, fingerprint, build_identity, version, architecture, trust/source e installation_ref. CLI e IPC devem usar a mesma função de serviço. Não recriar o candidato somente por executable, nem trocar trust por explicit=True implicitamente. Probe passivo inexistente continua NOT_PROBED, mas observação já existente não pode ser descartada.

**Não resolver assim:** Não mascarar a perda de versão com um default; não avaliar Node sozinho quando a instalação é o par Node/CLI.

**Aceite obrigatório:** P04 CLI e IPC preservam reference/build/version do candidato Pi real. Duas CLIs Pi usando o mesmo Node permanecem instalações distintas. Nenhum processo é iniciado para listar.

### CN4-04.02 — Derivar revisão de todas as evidências relevantes

**Implementar:** Use uma representação canônica e ordenada do snapshot que inclua identidade de instalação, fingerprint/build, versão/arquitetura, confiança e dimensões/razões de prontidão, além de versões de Core/formato pertinentes. Diferencie conteúdo/evidência da hora de publicação/TTL: atualizar timestamp sem mudança de evidência não deve girar a revisão por acidente. Não use apenas ref+version+state, pois atualizações mantêm esses campos. Compare o conjunto completo de candidatos na revisão, não a ordem do discovery.

**Não resolver assim:** Não usar schema_version do StateStore, version string de provider nem um novo contador não persistente como prova de conteúdo; não fazer duas revisões para a mesma evidência.

**Aceite obrigatório:** P04b muda revisão quando bytes da CLI mudam mantendo path/version/state. Controle sem mudanças e com reordenação é estável; mudanças de qualificação/contenção pertinentes invalidam a evidência.

### CN4-04.03 — Conectar seleção à revisão realmente apresentada

**Implementar:** Faça a resposta da UI/CLI carregar executor, adapter_id, candidate_ref e revisão do snapshot exibido. No host produtor, confira revisão/TTL/escopo e chame resolve_installation/resolve_selection público sobre esse mesmo inventário. Persistir o candidato resolvido completo; não recalcular a revisão de [candidate] e chamá-la de revisão do inventário agregado. Revalidar prepare/open continua obrigatório. Referência legada ambígua exige reseleção.

**Não resolver assim:** Não usar índice, display_name ou primeiro candidato; não replicar o algoritmo de referência do Core; não engolir falha de snapshot deixando inventory_revision=None como aceite silencioso.

**Aceite obrigatório:** Fluxo de duas cópias byte-idênticas seleciona B, mantém B após reordenação e recusa revisão velha após update. Resolução passa pela API pública do Core.

### CN4-04.04 — Publicar contrato de projeção por executor

**Implementar:** A projeção usada pelos aplicativos deve levar core_version, versão do formato, identidade/revisão do executor, observação e validade. O Connector produz fatos do seu host; o Server acrescenta autorização e conectividade. IPC local disponível não é prova de snapshot publicado remotamente: escolha a API de aplicativo versionada acordada ou revisão NXL deliberada, sem inserir extras em schemas restritivos. Mantenha caminhos e segredos locais; não carregar classes recebidas pela rede.

**Não resolver assim:** Não reimplementar qualified_build, catálogo ou preflight no Server/UI. Não afirmar UI ou Server remoto testado só por chamar dispatch diretamente.

**Aceite obrigatório:** Peer do Server recebe a projeção correta e versionada; tipos/formato desconhecidos são incompatíveis, não READY. Documentar owner/BLOCKED quando ainda faltar o contrato remoto.

## CN4-05 — Rotação e reanexação de lanes existentes

**Dependências:** CN4-00, CN4-01.

### CN4-05.01 — Aplicar diff real no reload

**Implementar:** Compare o estado persistido com cada lane existente, não somente adições/remoções. Mudança validada de credencial, agente, autorização ou configuração deve invalidar a prova antiga em memória antes de I/O e atualizar as referências corretas. No-op real é idempotente e não gira ticket_epoch. Não atualizar o contexto do runtime apenas porque o StateStore mudou; a nova autorização passa pelo Server e pelas operações de renovação/reconciliação do Core.

**Não resolver assim:** Não assumir que um teste que atribui lane.authorization_revision diretamente prova o reload. Não ampliar grant copiando revisions sem confirmação.

**Aceite obrigatório:** P05 reload coloca a lane na revisão/epoch atuais e retira a prontidão antiga. Mensagens enfileiradas antes da rotação não produzem efeito; outras lanes permanecem estáveis.

### CN4-05.02 — Atualizar provider e completar reattach

**Implementar:** A atualização de lane deve trocar ticket_provider/identidade autorizada e agendar attach no canal ativo pelo mesmo coordenador existente. O token da tentativa identifica o epoch/revisão capturados; resultado de attach antigo não marca o novo epoch como ready. Enquanto novo attach não completar, o estado continua pending. Reutilize uma task em voo ou finalize-a conforme sua propriedade, sem múltiplos anexos concorrentes.

**Não resolver assim:** Não retornar depois de ready→pending sem próximo passo; não reutilizar closure de credencial antiga; não forçar restart de todos os Servers para ativar uma lane.

**Aceite obrigatório:** P05b consulta o novo provider e despacha attach. Confirmação velha não abre lane nova; falha transitória é recuperável e não bloqueia lanes alheias.

### CN4-05.03 — Conservar expiração do ticket e provar renovação

**Implementar:** Passe a expiração obtida no contrato HTTPS para o registro de lane em vez de descartá-la. Defina prazo monotônico derivado da observação local, política single-flight/backoff e diferença entre ticket expirado, credencial revogada e socket vivo. A renovação de ticket não estende por si a lease de uma sessão. Correlacione detach/revoke ao namespace e faça inventory/readiness refletirem o estado atual. Qualifique a campanha remota separadamente.

**Não resolver assim:** Não anunciar renovação automática só porque expires_at foi adicionado ao dataclass; não usar polling agressivo nem reassociar identidade pelo primeiro registro.

**Aceite obrigatório:** Fake clock + peer demonstram expiração, renovação e rotação sem ticket cruzado; expiração não deixa READY indefinido; ausência de contrato tem estado BLOCKED explícito.

## CN4-06 — Aceite, pacote e handoff proporcional

**Dependências:** CN4-01, CN4-02, CN4-03, CN4-04, CN4-05.

### CN4-06.01 — Executar regressões por camada e por caminho

**Implementar:** Execute novas sementes duas vezes, CN3 original adaptado, CN3 do executor, regressões anteriores e suíte no SO efetivo. Em testes de estados unitários, isolar preflight é permitido quando não se afirma qualificação; nunca retirar o gate do produto para obter verde. Mostre contadores de chamadas/bytes/owner/watermark realmente observados. Investigue warnings e tasks sem dono; não agregue repetições como novos testes.

**Não resolver assim:** Não apresentar 14 testes reconstruídos como substituto das sementes recebidas; não reduzir o teste de duas superfícies a um helper.

**Aceite obrigatório:** Cada achado tem prova antes/depois; controles continuam passando. Nove falhas ambientais conhecidas são separadas de novas falhas, e qualquer diferença de fixture vem com diff.

### CN4-06.02 — Construir artefato único e testar fora da fonte

**Implementar:** Construa wheel e sdist sobre a fonte corrigida, registre hashes, fixe Core e instale em diretório/ambiente externo à árvore. Execute os testes de catálogo, pipeline de candidatos, decisões, ACK e rotação contra o import instalado. Teste migração de state/inventory/layout sem rotacionar identidade ou apagar operações incertas. Entregue aos agentes do Server/Core somente mudanças públicas reais, com versão e compatibilidade.

**Não resolver assim:** Não escolher o último wheel de clone irmão, substituir dependência por código diferente com mesmo número, afirmar identidade do wheel do executor sem hash ou publicar release sem autorização.

**Aceite obrigatório:** Runner instalado e -I import funcionam; fontes/artefatos/versões são identificados; rollback e pendências duráveis têm procedimento seguro.

### CN4-06.03 — Fechar somente o escopo comprovado

**Implementar:** Atualize status por requisito: implementado e ligado, demonstrado com peer, qualificado por SO/provider, integrado. Mantenha a lista externa separada: runtime.open remoto, callbacks opcionais, publicação estável de intenções/recibos e transferência/recuperação ao sair com unknown. Se a opção escolhida for manter drain/IPC disponível, implemente-a; exit code 1 não prova que o supervisor continua vivo. Não reabrir o Core para corrigir orquestração do Connector.

**Não resolver assim:** Não alegar G1/G2 integral enquanto aprovações/snapshot/lanes prometidos estiverem parciais; não tratar falha funcional como só ausência do Server.

**Aceite obrigatório:** Relatório começa pelo escopo liberado, lista bloqueios reais e owners. Desenvolvimento em paralelo continua; liberação operacional é limitada às rotas demonstradas.

## Entrega ao concluir

Relatório por achado, commits/diff, API/formatos alterados, teste/node/camada, comandos e exit codes, XML/log, hashes dos wheels, migração e riscos residuais. Nenhum push, publicação, uso de credencial real ou alteração de permissão está autorizado por este plano. Uma correção já presente no HEAD fecha com prova, sem retrabalho.
