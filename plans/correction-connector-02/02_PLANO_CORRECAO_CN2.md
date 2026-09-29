# Plano CN2 — completar as fronteiras corrigidas parcialmente no CN1

**Destino:** agente do `okto-nexus-connector`. **Baseline:** `28d147625c2a47417ad70f72d3b453cd8bd2e984`, Connector `0.1.0.dev0`, Core `0.2.10.dev0`. Trabalhar no HEAD real, sem reset ou perda de mudanças.

## Mandato

Implementar, não produzir outro plano em substituição à execução. O CN2 complementa o CN1 e R3: preserva a fila lossless, arquitetura persistida, origem HTTPS correta, StateStore, APIs e avanços de candidatos. Os adaptadores continuam exclusivamente no Core. Somente MCP HTTP direto harness→Server; nenhum proxy/stdio MCP. A identidade permanece no agente. As correções não devem trocar o canal remoto ou criar outra inbox.

Este plano contém **9 fases e 32 tarefas**. A matriz separa 20 verificações executadas (16 FAIL, três controles PASS e uma observação PASS) de 24 cenários complementares ainda não executados. Subconjuntos e repetições não são cobertura nova. N01–N08 têm reprodução; N09 é lacuna funcional inspecionada, não falha de um teste que impõe um nome inexistente.

## Sequência

Executar CN2-00. Escopo e scheduler formam a base das correções de eventos/recuperação. Lifecycle e managed wiring usam os mesmos contextos. Lanes/TLS/inventário podem avançar em paralelo nas partes independentes, respeitando o contrato de CN2-00.03. Não esperar a integração real para corrigir falhas reproduzíveis em laboratório. G1 não é concedido somente porque os exemplos importam a biblioteca.

## Estados que devem ser explícitos

| Objeto | Estados/fatos que não podem ser confundidos |
|---|---|
| Conexão | socket aberto; contrato válido; geração; lane admitida; reconcile pendente/falho/concluído |
| Operação | intenção identificada; autorização; admissão durável; efeito possível; recibo; publicação/ACK |
| Evento | no journal; enfileirado; escrito na conexão; ACK validado; ACK aplicado ao Core |
| Stop | reserva de tentativa; não entregue; efeito possível; parado observado; pendência durável; resolvido |
| Candidato | tipo registrado; instalação encontrada; build observado; qualificação; contenção; seleção; autorização |

Nenhum timeout transforma resultado incerto em seguro para novo ID. Nenhuma regra de seleção utiliza o primeiro elemento como autoridade. Nenhum booleano `containment` concede capabilities. Uma negativa temporalmente permitida ainda valida alvo/pedido/autoridade. Um evento pode ser retransmitido sem repetir o trabalho do agente; as duas idempotências são distintas.


## CN2-00 — Baseline, contratos e preservação

**Dependências:** nenhuma.


### CN2-00.01 — Fixar entrada e reproduzir

**Achados:** N01–N09. **Estado inicial:** PENDING.

**Implementar:** Registre HEAD, branch, dirty state, hash do ZIP, Python, dependências e pin do Core. Confronte 28d1476 com o HEAD real sem reset. Copie o pacote completo, inclusive regressoes/. Rode os 20 casos CN2 e o subconjunto de 14 sementes originais CN1 antes de alterar. Diferencie a observação de carga dos três controles; os 16 FAIL são expectativas corretas. Erros iniciais de fixture/codec foram corrigidos no pacote final e não são achados.

**Não resolver assim:** Não reconstruir testes somente a partir do relatório quando os executáveis foram entregues; não copiar dependência diferente com o mesmo número de versão.

**Aceite obrigatório:** XML/log por campanha, proveniência do Core e antes/depois. Preservar as nove falhas ambientais/fixtures separadamente do resultado funcional.


### CN2-00.02 — Estabelecer inventário das fronteiras

**Achados:** N01–N08. **Estado inicial:** PENDING.

**Implementar:** Mapeie para cada entrada CLI/IPC/WSS a origem autenticada, chave de sessão, ações autorizadas, fila, validação depois da espera, método Core, operation_id, recibo e finalização. Inclua frames não produtivos: ACK, reconcile, detach e mensagens de negociação. Registre a quem pertence cada task durável e o que sobrevive à perda do socket/cliente.

**Não resolver assim:** Não tratar um método com nome guarded ou uma Task registrada como prova de isolamento ou de sobrevivência de seu produtor.

**Aceite obrigatório:** Tabela cobre open/submit/steer/interrupt/close/query/reconcile/approval/input e os eventos; ausência de implementação é explícita, não sucesso sintético.


### CN2-00.03 — Acordar o contrato com o Server

**Achados:** N04/N06/N07/N09. **Estado inicial:** PENDING.

**Implementar:** Produza handoff de rotas de intenção, autorização, publicação de recibo, tickets, confirmação de lane/reconciliação e snapshot do inventário. Use o bundle NXL do Core como contrato executável. O r3 não permite propriedades adicionais no event.ack; obtenha geração da conexão autenticada ou evolua o protocolo deliberadamente. APIs novas só são aceitas quando especificadas pelos dois consumidores e cobertas por peer de contrato.

**Não resolver assim:** Não inventar localmente um protocolo alternativo, emitir permissões com defaults nem acrescentar campos que o schema recusa.

**Aceite obrigatório:** Decisões versionadas, owners identificados; fronteiras não acordadas ficam bloqueadas antes de efeito. Core continua biblioteca sem endpoint MCP/rede adicional.


## CN2-01 — Autoridade fim a fim e namespaces

**Dependências:** CN2-00.


### CN2-01.01 — Carregar um envelope validado até a sessão

**Achados:** N01. **Estado inicial:** PENDING.

**Implementar:** Introduza DTO interno imutável para operação admitida: identidade da conexão, Server/executor, agente/binding/workspace, SessionKey, generations, revisões, operation_id, intent_hash, payload, expected_turn e referência ao grant local verificado. O dispatcher deve passar esse DTO inteiro ao RuntimeManager. Remova a resolução remota por session_id isolado; session_by_key deve usar chave originada do transporte. Compare binding, agente e workspace da sessão encontrada antes de construir ExecutionContext.

**Não resolver assim:** Não usar a identidade da sessão encontrada para corrigir silenciosamente o frame; não aceitar uma sessão de A porque só ela possui aquele texto de ID.

**Aceite obrigatório:** B03 foreign recusa com zero writes; B03 same Server continua produzindo uma escrita. Mesmo session_id/binding_id em dois Servers não impede selecionar o namespace certo.


### CN2-01.02 — Revalidar no despacho após espera

**Achados:** N01/N02. **Estado inicial:** PENDING.

**Implementar:** Ao reservar uma operação, capture a versão/epoch da lane e o grant efetivo. Após fila/semaphore, revalide conexão corrente, lane não removida/rotacionada, deadline, generations e revisions contra a mesma reserva antes de chamar o Core. Um detach/revoke durante a espera fecha admissões em memória sem aguardar rede ou armazenamento. Não remova uma operação durável já conhecida só por mudar conexão; ela pode retornar recibo, mas não gerar efeito novo.

**Não resolver assim:** Não apenas chamar _run_operation_guarded antes do semáforo. Guardar validação antiga não a torna atual após espera.

**Aceite obrigatório:** B02 passa mantendo a operação enfileirada até depois do detach. Variantes de rotação/geração/shutdown também têm zero efeito; recibo idêntico continua consultável.


### CN2-01.03 — Conservar o grant aprovado sem ampliar ou reancorar prazo

**Achados:** N01/N06. **Estado inicial:** PENDING.

**Implementar:** Armazene contexto completo recebido e aprovado por sessão, não só allowed_actions e um deadline separado. Renovação troca o snapshot atomicamente após autorização/resultado do Core, preservando revisões e owner. Remova allowed.update de controles solicitados pelo peer. A exceção de lease para decline/interrupt autoriza apenas controles previstos pelo contexto/contrato; shutdown interno proprietário tem trilha separada. Receber uma mensagem ou terminar submit não estende lease em 90 segundos por regra local.

**Não resolver assim:** Não interpretar containment=True como autorização nova nem derivar prazo por max/min que ressuscite lease vencida. Dados de linha durável não criam grant.

**Aceite obrigatório:** Ações vazias continuam vazias; ação não concedida permanece recusada. Campo de agente/owner/workspace divergente e lease vencida não são ignorados. Renovação legítima continua funcional.


### CN2-01.04 — Uniformizar escopo de estado e remoções

**Achados:** N01/N03/N07. **Estado inicial:** PENDING.

**Implementar:** Use chaves tipadas completas para registros, tasks, approvals, tickets, ACKs, leases e obrigações. A interface humana pode resolver alias/ID curto somente quando inequívoco; retorne alternativas com Server/executor em caso de ambiguidade. APIs internas de mutação devem receber chave já resolvida. Troque concatenação ad hoc de binding#session por tipo que represente revisão/instalação/sessão e adapte todos os caminhos get/reconcile.

**Não resolver assim:** Não resolver por next(iter(...)), primeiro binding/identidade ou posição do inventário. Não quebrar consulta a recibo de sessão evictada.

**Aceite obrigatório:** Remoção, rotação, ACK e stop de A não alteram B com IDs iguais. Reconcile de sessão viva encontra seu Core original, não uma instância vazia.


## CN2-02 — Admissão limitada e controles responsivos

**Dependências:** CN2-00, CN2-01.


### CN2-02.01 — Separar capacidade de dados e controle

**Achados:** N02. **Estado inicial:** PENDING.

**Implementar:** Projete filas limitadas e classes de trabalho: efeitos produtivos normais versus controles de recurso já autorizado. Reserve despacho de interrupt/close/deny sem depender dos quatro submits em andamento. Requests que concedem trabalho como steer/accept não ganham dispensa de autorização por pertencer à fila urgente. Imponha limites por stream/lane e globais, conservando correlação de resultados fora de ordem.

**Não resolver assim:** Não aumentar OPERATION_CONCURRENCY como única correção; não serializar todo o projeto em lock global; não colocar close/observe/força dentro da fila produtiva.

**Aceite obrigatório:** B01 alcança interrupt enquanto quatro submits continuam bloqueados. Outro agente não perde controle por saturação de uma lane; asserções observam entrada no handler/Core correto.


### CN2-02.02 — Aplicar backpressure antes de criar tarefas

**Achados:** N02/N03. **Estado inicial:** PENDING.

**Implementar:** Configure explicitamente limites de tarefas pendentes, itens, bytes e tamanho de frame. Reader valida/admite em estrutura limitada antes de criar task produtora; com limite atingido, mantém a intenção durável quando já aceita ou recusa antes do efeito com erro inequívoco. A leitura de heartbeat/ACK/negociação continua progredindo. O número de slots escolhido é decisão do produto e deve constar da configuração/teste; 300 do laboratório é uma observação, não um teto obrigatório.

**Não resolver assim:** Não usar um semáforo dentro de milhares de tasks já criadas como limite de admissão; não retornar sucesso de enqueue recusado.

**Aceite obrigatório:** Teste usa limite configurado pequeno e prova que pendências não crescem além dele. Nenhuma task de erro/rejeição cresce sem limite; fila simultânea corrigida CN1 continua lossless.


### CN2-02.03 — Conservar resultados e produtores durante troca de conexão

**Achados:** N02/N04. **Estado inicial:** PENDING.

**Implementar:** Separe waiter/socket da operação aceita e de seu recibo. Ao cair WSS, encerrar a espera da conexão não deve descartar o resultado nem criar efeito duplicado. Não reenvie frames enfileirados de gerações antigas antes de novo handshake. Registre obrigação de publicar receipt se fila estiver cheia; o replay consulta Core e usa o mesmo operation_id/hash. Monitore exceções de tasks possuídas e restaure apenas transportes, não tarefas do agente.

**Não resolver assim:** Não cancelar indiscriminadamente _inflight e considerar tudo não enviado; não gerar novo ID porque a resposta não saiu.

**Aceite obrigatório:** Partição durante submit e fila cheia geram um efeito máximo e recibo recuperável. Controle continua disponível; nenhuma exceção de task fica sem observação.


## CN2-03 — Publicação durável, finita e validada de eventos

**Dependências:** CN2-01, CN2-02.


### CN2-03.01 — Ler lotes finitos por API do Core

**Achados:** N03. **Estado inicial:** PENDING.

**Implementar:** Substitua _journal_batch baseado em runtime.events de acompanhamento por leitura finita do Journal port público, com cursor/limite e namespace correto. Alternativa válida é consumidor contínuo proprietário com flush limitado por quantidade/latência; escolha uma solução e mantenha o journal como fonte. A leitura pode devolver menos de 128 ou zero imediatamente para o snapshot disponível. Não faça SQL paralelo no Connector.

**Não resolver assim:** Não esperar o evento 128 para enviar o primeiro; não matar o iterator com timeout descartando eventos já consumidos; não escolher o executor do primeiro binding.

**Aceite obrigatório:** B04 passa com um evento persistido e nenhum posterior. Casos 0/1/127/128/129 preservam ordem e limite; não há espera indefinida de dados futuros.


### CN2-03.02 — Distinguir enfileirado, escrito e confirmado

**Achados:** N03. **Estado inicial:** PENDING.

**Implementar:** Guarde cursores separados e por geração: ingresso na fila não comprova escrita no socket; escrita não comprova ACK durável. O replay começa do último ACK validado, conservando uma janela limitada de envio. Na reconexão ou disponibilidade do sender, agende retomada dos streams pendentes, mesmo sem novo evento nativo. Duplicatas de eventos não repetem operações e são deduplicadas pelo Server.

**Não resolver assim:** Não usar _sent_through como ponto irrevogável após timeout de ACK; não apagar o journal para sincronizar um cursor RAM incorreto.

**Aceite obrigatório:** B05 envia novamente seq1 sem ACK. Controle com ACK válido avança e não reenvia desnecessariamente. Reconexão sem nova produção drena os registros pendentes.


### CN2-03.03 — Validar ACK antes de confirmar no Core

**Achados:** N03/N01. **Estado inicial:** PENDING.

**Implementar:** Valide Server/executor contra a conexão, sessão/epoch contra stream conhecido e sequência monotônica contígua até o limite realmente escrito naquela sessão. ACK de conexão substituída não atravessa o fence. O r3 não possui generation no frame; mantenha a origem no envelope interno, ou revise schema/negociação pelos owners se necessário. ACK inválido retorna diagnóstico sem chamar acknowledge/compact do Core.

**Não resolver assim:** Não inserir extra fields incompatíveis nem confiar somente na identidade textual session_id. Não encaminhar qualquer watermark por ser um inteiro.

**Aceite obrigatório:** B05b/B05c passam; ACK futuro, cruzado, regressivo ou de stream inexistente não avança journal. ACK válido duplicado é idempotente e preserva execução saudável.


### CN2-03.04 — Supervisionar pumps e snapshots de cursor

**Achados:** N03/N04. **Estado inicial:** PENDING.

**Implementar:** Registre streams por chave completa com task/estado/cursor e limite de memória. Falha de read/send/ACK fica diagnosticada e recuperável com retentativa coalescida. Drain só finaliza observers conforme sua propriedade; pendências duráveis continuam consultáveis. Startup/reconnect lê claims/cursosres pelas APIs públicas, não depende de um evento novo para descobrir que algo ficou pendente.

**Não resolver assim:** Não deixar task morrer definitivamente no primeiro erro nem criar uma retentativa por publish. Não duplicar inbox canônica.

**Aceite obrigatório:** Reader/sender temporariamente indisponíveis voltam a publicar. Milhares de notificações offline não geram lista proporcional de payloads em RAM nem threads/tasks sem limite.


## CN2-04 — Reconciliação sem falso READY e sem dependência de binário

**Dependências:** CN2-01, CN2-02, CN2-03.


### CN2-04.01 — Fazer readiness depender de fatos de negociação

**Achados:** N04. **Estado inicial:** PENDING.

**Implementar:** Modele transporte conectado, contrato negociado, lanes em admissão, reconciliação pendente/falha/concluída e elegibilidade produtiva. Se projeção inicial falha, mantenha a recuperação pendente e recuse efeito novo; não substitua por relatório vazio. Enqueue/write/aceite de protocolo são fatos diferentes; o contrato com Server define qual prova conclui cada etapa. Empty report só é válido após consulta bem-sucedida sem evidências.

**Não resolver assim:** Não marcar _reconciled=True por executar uma função que capturou sua própria falha, nem usar wait_online como prova de que todas as lanes estão admitidas.

**Aceite obrigatório:** B06 passa. Falha ou fila cheia durante initial reconcile mantém novo submit bloqueado e não impede consultas/recovery. Handshake normal segue funcional.


### CN2-04.02 — Desacoplar consulta histórica da composição de runtime

**Achados:** N04. **Estado inicial:** PENDING.

**Implementar:** Use o Journal port/Core público para recibos/claims/lease/cursores sem exigir um candidato executável atual. Para snapshots vivos, localize o Core proprietário pelo namespace/sessão correto; para registros sem handle, reporte desconhecido com histórico disponível. Não construir factory de provider para apenas consultar um recibo. Se faltar uma agregação pública no Core, proponha extensão mínima e coordenada, não importe estruturas privadas ou SQL ad hoc.

**Não resolver assim:** Não chamar candidate_for/prepare para responder à história; não usar o primeiro Core de outro binding; não iniciar processo para saber se uma operação existiu.

**Aceite obrigatório:** B10 devolve o recibo real após remover executável. Reconcile com runtime por sessão usa sua observação viva; consulta estrangeira não retorna dados de outro scope.


### CN2-04.03 — Persistir intenção antes da mutação HTTP e recuperar por ID

**Achados:** N04/N05. **Estado inicial:** PENDING.

**Implementar:** Conclua a autoria acordada em CN1: comando local cria identificador estável antes do primeiro POST mutável; Server resolve/autoriza essa intenção e o Connector preserva seu resultado. Erro de resposta após possível efeito devolve chave consultável, nunca exige repetir com outro ID. Publicação de receipt precisa de endpoint/semântica de receipt, não outro pedido de execução cujo payload agora contém só text_length. Mutação, autorização e ACK são contratos distintos.

**Não resolver assim:** Não concluir que a melhoria no flag ReadTimeout já resolveu o ciclo inteiro; não reenviar prompt para descobrir se foi aceito.

**Aceite obrigatório:** Peer de Server com perda de resposta conta uma intenção/um efeito. Reiniciar o daemon antes da publicação conserva consulta da operação e não requer binário instalado.


## CN2-05 — Encerramento e ownership entre aplicação e Core

**Dependências:** CN2-01, CN2-04.


### CN2-05.01 — Classificar erros e recibos de stop

**Achados:** N05. **Estado inicial:** PENDING.

**Implementar:** Formalize StopAttempt com ID, fase, produtor possuído, receipt e evidência física/durável. Capture CoreError por seus campos e ConnectorError de rede separadamente; CancelledError não prova ausência de efeito. FAILED sem efeito remove apenas a reserva de stop e mantém a sessão/controles; unknown conserva resultado e supervisão; sucesso remove o gerenciamento só conforme o contrato efetivo de fechamento/observação do Core. A mesma implementação deve atender close remoto/local.

**Não resolver assim:** Não usar stage != OUTCOME_UNKNOWN como prova de parada nem capturar Exception para sempre redefinir closing=False.

**Aceite obrigatório:** B07 nas duas variantes passa. Casos de resposta perdida, close repetido e recibo conhecido não geram novo ID/efeito automaticamente; CoreError pré-admissão não prende closing.


### CN2-05.02 — Dar um único orçamento e dono ao shutdown

**Achados:** N05/N02. **Estado inicial:** PENDING.

**Implementar:** Consolide drain da aplicação, operações em andamento, shutdown dos Cores, evento/receipt pendente e disposal. Contenção de recurso vivo não espera POST normal de outro recurso. Relatório mantém chave completa e distingue físico incerto de liberação durável pendente. Cancelamento do waiter IPC não destrói producers do Core. Se o daemon precisa sair com unknown, documente e qualifique transferência/containment do SO; sem essa prova, mantenha recuperação e IPC limitado disponíveis até política explícita.

**Não resolver assim:** Não considerar exit code 1 uma forma de conservar objetos/tasks depois que o loop Python foi encerrado. Não fechar stores com commit ainda dependente deles.

**Aceite obrigatório:** Dois runtimes (um incerto) continuam controláveis pelo lifecycle público. Orçamento total é comprovado; desligamento repetido converge sem imports privados ou processo órfão assumido.


### CN2-05.03 — Preservar estado por tentativa e isolamento no reuso

**Achados:** N05/N01. **Estado inicial:** PENDING.

**Implementar:** Contexto da sessão e referência ao Core não podem ser reutilizados depois de mudança de binding/credencial sem revisão. Start/reuse deve confrontar binding revision, instalação e grant, não só o texto do binding e último receipt. Cache precisa ter inicialização single-flight por chave de composição e produtor conhecido. Uma falha de publish após submit conserva o receipt e não estende lease localmente por 90 segundos.

**Não resolver assim:** Não trocar a capability de sessão ativa pela da última chamada, descartar o Core antigo com trabalho ou gerar outro processo para corrigir erro de cache.

**Aceite obrigatório:** Testes concorrentes de duas sessões e rebind preservam cada ambiente/grant; cancelamento de um cliente não abandona uma abertura já aceita pelo Core.


## CN2-06 — Configuração efetiva e matriz remota de funcionalidades

**Dependências:** CN2-01, CN2-04, CN2-05.


### CN2-06.01 — Aplicar a URL e capability ao cliente real do harness

**Achados:** N06. **Estado inicial:** PENDING.

**Implementar:** Use os templates públicos do Core em um mecanismo de configuração realmente consumido pelo processo: argumentos qualificados, arquivo efêmero ou plan/apply de campos próprios. Transmita URL direta aprovada e referência de bearer, não apenas a variável com token. Propague a configuração até o PreparedLaunch/environment/start usando API pública; se falta opção no Core, coordenar extensão, sem copiar parser nativo. Preserve config de terceiros via CAS/backup e evite segredo em argv/log/prompt.

**Não resolver assim:** Não declarar MCP configurado só porque NEXUS_MCP_TOKEN aparece no ambiente. Não implementar proxy MCP/stdio como fallback.

**Aceite obrigatório:** B09 passa com projeto/home limpos. Peer de processo observa URL e bearer corretamente associados; duas sessões usam credenciais diferentes; ferramenta liga diretamente ao Server.


### CN2-06.02 — Corrigir a decisão nativa com DTO nomeado e rota completa

**Achados:** N06. **Estado inicial:** PENDING.

**Implementar:** Corrija chamada de _authorized_context com ação derivada da operação validada e preserve a regra de negative reply do Core sem ampliar o grant. Construa NativeApprovalOperation por nomes: operation_id, session_id, request, decision, operator_response. Request não pode receber uma string de decisão por inversão posicional. Ligue a resposta autorizada do Server ao método final, com SessionKey e estado bifásico; IPC/CLI não autoaprovam pelo próprio agente quando requer operador.

**Não resolver assim:** Não corrigir apenas o TypeError e deixar request/decision invertidos, nem adicionar uma rota sem checar a autoridade da decisão.

**Aceite obrigatório:** B08 alcança a porta pública com campos corretos. Accept expirado e pedido cruzado são negados; decline seguro segue para o mesmo pedido sem consumir resposta que nunca foi enviada.


### CN2-06.03 — Compor recursos opcionais pelos contratos públicos do Core

**Achados:** N06. **Estado inicial:** PENDING.

**Implementar:** Forneça pi_native_action, capacidade/escopo e backend de ações canônicas na composição, somente onde qualificado. Configure native approvals e resume Codex por callbacks/DTOs públicos e evidências de owner, sem ligar flags globalmente em todos os runtimes. Separe capability declarada, implementada, configurada e qualificada. Ausência de substrato attach não pode ser contornada pelo Connector.

**Não resolver assim:** Não exigir que o modelo interprete texto como comando, nem copiar extensão/parser dos runtimes. Não receber todo home de provider como solução indiscriminada.

**Aceite obrigatório:** Start Pi sintético originado na aplicação chega ao backend HTTP controlado com escopo; teste de Core isolado não encerra este requisito. Credenciais mínimas por sessão e modo inválido recusado antes de efeito.


### CN2-06.04 — Implementar o ciclo remoto anunciado ou delimitar entregas

**Achados:** N06/N04. **Estado inicial:** PENDING.

**Implementar:** Finalize runtime.open, operation.query, submit/steer/interrupt/close e decisões/input no transporte contratado, preservando operation_id/hash/expected_turn/candidato/revisão/grant. O Server peer deve conseguir iniciar sessão sem CLI local preparatória, embora bindings/credenciais locais previamente aprovados sejam necessários. Se contrato Server ainda faltar, implemente lado Connector contra schema acordado e registre integração real BLOCKED; não declare funcionalidade entregue quando branch retorna UNSUPPORTED.

**Não resolver assim:** Não inventar autorização no remote open nem fazer segunda resolução HTTP para uma intenção remota já identificada. Não marcar G1 do ciclo completo com open indisponível.

**Aceite obrigatório:** Teste vertical de laboratório via receiver e Core reais abre, envia, observa, decide, interrompe, fecha e consulta recibo; versões/verbos não suportados falham explicitamente.


## CN2-07 — Lanes, transporte seguro e inventário do executor

**Dependências:** CN2-01, CN2-02, CN2-06.


### CN2-07.01 — Vincular bootstrap e ticket à mesma identidade

**Achados:** N07. **Estado inicial:** PENDING.

**Implementar:** Escolha primeiro um binding elegível do Server e então recupere exatamente seu agent_id/credential_epoch/secret_handle. Ticket devolve escopo, expiração e epoch por DTO, não apenas string. Sem lane elegível não envie chave arbitrária. Verifique origin/audience de controle e rotação antes de construir cabeçalhos.

**Não resolver assim:** Não usar a primeira identidade do Server e a primeira lane independentemente. Não tentar chaves de agentes em sequência até uma funcionar.

**Aceite obrigatório:** B11 pede key_b para binding B mesmo com identidade A primeiro na lista. Dois Servers e aliases repetidos mantêm segredos isolados.


### CN2-07.02 — Fazer reload e renovação agirem no transporte ativo

**Achados:** N07. **Estado inicial:** PENDING.

**Implementar:** Adição em READY agenda attach possuído imediatamente, com fila cheia mantendo obrigação; remoção fecha grant local antes do detach; rotação existente troca epoch e invalida provas antigas. Renovação é single-flight por lane, com expiração monotônica ancorada à resposta e jitter/backoff limitado. Defina o que confirma attach no r3 acordado sem fingir que entrada no dicionário é conexão. Dados inválidos não refrescam watchdog de aplicação.

**Não resolver assim:** Não depender de reiniciar todo o socket para uma lane nova nem descartar expires_at; não transformar ping/pong em renovação de lease de execução.

**Aceite obrigatório:** B11b passa no transporte vivo. Revoke A não interrompe B; ticket expirado/rotacionado não admite operação por grant antigo; não há fila de refresh ilimitada.


### CN2-07.03 — Validar origem WSS inclusive overrides

**Achados:** N08. **Estado inicial:** PENDING.

**Implementar:** Centralize validação de endereço de gerenciamento e controle antes de obter segredos. Exija wss para rede remota, certificado verificado, ausência de userinfo e origem/audience aprovada. Defina exceção ws loopback como política explícita de laboratório. Um override não contorna TLS; origem distinta só por decisão autorizada e versionada, com ticket adequado. Preserve suporte de proxy/certificados sem desligar verificação.

**Não resolver assim:** Não assumir que validar base_url torna link_url_override seguro nem verificar depois de entregar Authorization à biblioteca.

**Aceite obrigatório:** B12 faz zero chamadas autenticadas para ws remoto. WSS aprovado e loopback explicitamente permitido funcionam; redirects/portas/userinfo recebem testes por versão da biblioteca.


### CN2-07.04 — Consumir disponibilidade e resolver seleção no Core

**Achados:** N09. **Estado inicial:** PENDING.

**Implementar:** Integre get_runtime_catalog + discovery + evaluate_runtime_availability e AvailabilityReport formato2. Reutilize resolve_installation para uma seleção exata; somente caminhos locais resolvem o candidato. Não confundir trust/build qualificado/containment com autorização do agente. A avaliação é produzida no executor; Server aplica políticas e não recalcula com seu SO. Catálogo e qualificação continuam fonte única do Core.

**Não resolver assim:** Não limitar a atualização a importar resolve_installation sem chamá-lo ou persistir ref sem avaliar prontidão. Não copiar listas/allowlist para UI.

**Aceite obrigatório:** Duas cópias iguais resolvem distintamente; missing probe/attach/unqualified/containment ausente não habilitam disponibilidade. Consumidor usa apenas API pública do Core.


### CN2-07.05 — Publicar snapshot versionado e validade real do inventário

**Achados:** N09/N01. **Estado inicial:** PENDING.

**Implementar:** Anexe executor_id, versão Core/catálogo/disponibilidade, revisão derivada da evidência atual e validade do snapshot. Atualizações de instalações/qualificação alteram a revisão independentemente da versão do schema do StateStore. Defina rota de aplicativo versionada ou evolução NXL acordada; não enviar campos extras no frame antigo. Server expõe esses dados no seletor, aplica OFFLINE/STALE e revalida seleção no executor antes do efeito.

**Não resolver assim:** Não usar (schema_version<<8)|1 como revisão de inventário que muda; não escolher a primeira instalação nem expor paths completos no snapshot remoto.

**Aceite obrigatório:** Novo runtime no registro do Core aparece sem arrays nos aplicativos. Seleção de revisão antiga/foreign executor é recusada sem trocar agente/chave; UI real fica teste do Server, não PASS implícito aqui.


## CN2-08 — Provas integradas, versão e liberação

**Dependências:** CN2-01, CN2-02, CN2-03, CN2-04, CN2-05, CN2-06, CN2-07.


### CN2-08.01 — Executar cenário causal completo e controles

**Achados:** N01–N09. **Estado inicial:** PENDING.

**Implementar:** Execute CN2 duas vezes mantendo barreiras, Core real nos casos especificados e zero provider real sem autorização. Preserve 14 originais CN1 e faça variantes de limites/erro/executor. Corrija fixtures de permissão Linux e probe unitário de forma explícita, sem desabilitar preflight produtivo. Transforme a observação de tasks em teste normativo com limite configurável implementado, sem impor um valor arbitrário novo.

**Não resolver assim:** Não relaxar assert, substituir serializer por mock que recusa cedo ou considerar SKIP do managed MCP como prova do fluxo inteiro.

**Aceite obrigatório:** Cada critério possui node/camada/resultado. Os três controles positivos continuam passando; corrupção de fixture se documenta, não vira correção do produto.


### CN2-08.02 — Gerar artefatos identificáveis e smoke externo

**Achados:** N01–N09. **Estado inicial:** PENDING.

**Implementar:** Bump de versão development do Connector ou outro identificador inequívoco de artefato, mantendo Core pin exato e hash registrado. Construir wheel/sdist fora da fonte, instalar sem clones irmãos, importar com -I e executar catálogo/availability/CLI e contratos. Hash do wheel construído pelo revisor não é presumido igual ao do executor. Faixa de httpx/websockets deve corresponder a versões realmente ensaiadas ou ser restringida.

**Não resolver assim:** Não publicar remotamente, atualizar credenciais reais ou empacotar dependência de teste/stub como produção. Não presumir GitHub CI verde a partir de XML local.

**Aceite obrigatório:** Artefato por commit/hash, pacote mínimo instalado e runner reproduzível. Nenhum acesso privado a Core no caminho dos consumidores; matriz de compatibilidade entregue.


### CN2-08.03 — Revisar matriz e executar gate vertical autorizado

**Achados:** N01–N09. **Estado inicial:** PENDING.

**Implementar:** Reabra as tarefas CN1 afetadas e preserve as concluídas. Declare G0 artefato, G1 laboratório, G2 integração real e G3 escopo conforme a prova, não por número bruto de testes. Anexe handoff ao Server para um adaptador primeiro: binding, remote open, ferramentas HTTP diretas, eventos/ACK, decisão, interrupt/close e replay pós-partição. Provider/SO/autostart reais e Server em dois hosts exigem campanhas próprias e podem permanecer BLOCKED separadas da implementação.

**Não resolver assim:** Não afirmar todos P1/P2 resolvidos enquanto branch produtivo permanece sem wiring. Não confundir falta do Server real com impossibilidade de testar o lado Connector contra peer rígido.

**Aceite obrigatório:** Relatório final lista o que foi corrigido, não executado, bloqueado e os responsáveis. Nenhum contorno duplicando adaptadores, MCP ou identidade.


## Entrega obrigatória

Entregar diff/commits, versões/hashes, migracões, mudanças públicas, tabelas de estado, comandos/XMLs, situação de cada cenário e procedimento público de recovery. Evidência informa se é unitária, codec/peer, Core/journal real, backend de SO, provider real ou integração entre aplicativos. Somente rotas realmente disponíveis recebem conclusão. O pacote de revisão e seus testes devem permanecer junto ao projeto com rastreabilidade.

Nenhuma tarefa deste documento autoriza push, publicação, alteração de permissões de agentes, revogação de credenciais ou execução de providers com segredos reais. As prioridades são técnicas, não uma decisão automática de deploy.
