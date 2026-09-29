# Plano de correção do Nexus Connector — CN1

**Baseline:** `8bc50259dc7eb353f349f1eb762be2d33cc393de`, Connector `0.1.0.dev0`.  
**Escopo:** achados A01–A17 do relatório. Não substitui o plano original nem reabre a arquitetura.  
**Execução:** agente do repositório Connector; mudanças de contrato com Server/Core são coordenadas, não copiadas localmente.  
**Estados iniciais:** PENDING. Uma tarefa já corrigida no HEAD pode ser encerrada com prova, sem refazer trabalho.

## Mandato

Implementar as correções e gerar evidências. Não responder somente com outro plano. Não fazer reset ou apagar alterações posteriores. O Core continua único dono dos adaptadores, qualificação, catálogo e mecanismos de processo. O Connector deve usar corretamente suas portas, gerir a conexão autenticada e preservar identidade/recibos/ownership. MCP somente HTTP direto harness→Server; stdio de protocolo nativo permitido; nenhuma conta de usuário Nexus.

Ler o relatório, `achados.json`, o plano original R3 e os testes recebidos. O conjunto desta auditoria contém 34 casos, não um requisito de criar exatamente 34 testes novos. O pacote usa peers de laboratório; não autoriza uso de credenciais reais de provider nem publicação.

## Ordem e invariantes

A implantação não começa atualizando somente o pin do Core. Primeiro preservar dados do candidato e fechar escopos; filas e recuperação usam esses contratos. Configuração managed e controles remotos são então ligados à mesma operação autorizada. UI e serviços concluem sobre a base corrigida. Catalog/availability e isolamento podem avançar em paralelo desde que o contrato compartilhado seja único.

| Dimensão | Invariante |
|---|---|
| Autoridade | Um frame autenticado no canal ainda precisa corresponder à lane, geração, agente e operação. Nenhum campo descartado pode ser substituído por autoridade mais ampla. |
| Idempotência | Retry de transporte não é nova intenção. Perda de resposta não comprova ausência de efeito. |
| Estado físico | Pedido de close/force não prova STOPPED. Unknown conserva supervisor/capacidade ou transferência explícita qualificada. |
| Persistência | Timeout do waiter não cancela o produtor durável nem apaga sua obrigação. |
| Catálogo | Core fornece tipos e disponibilidade; aplicativos apenas projetam e aplicam autorização/conectividade. |
| Escopo | Server/executor/binding/session/request fazem parte das chaves conforme seu domínio; primeiro elemento não é resolução de identidade. |
| Limites | Controle não espera execução normal; memória/fila por bytes e itens; journal é replay, não outra inbox. |

## Tarefas explícitas

## CN-00 — Baseline, contrato e evidências

**Dependências:** nenhuma.

### CN-00.01 — Fixar entradas e preservar HEAD

**Achados:** A01, A17.

**Implementar:** Registrar commit, branch, dirty state, versão Python/SO e hashes do ZIP/arquivos. Confrontar achados com o HEAD atual sem reset. Copiar o pacote inteiro, inclusive regressoes e runner, para plans/correction-connector-01. Conservar correções posteriores e classificar o que já estiver resolvido com teste causal.

**Aceite obrigatório:** Registro reproduzível de baseline; nenhum arquivo do usuário removido; o commit avaliado e o commit corrigido ficam distintos.

### CN-00.02 — Reproduzir as campanhas antes de editar

**Achados:** A01, A02, A04, A08.

**Implementar:** Executar 34 casos recebidos, incluindo controles, com Core fixado e depois com versão-alvo deliberada. Corrigir apenas incompatibilidade justificada de fixture, registrando diff. Não xfail/skip bugs. O resultado esperado no snapshot é 30 falhas e 4 aprovações, sendo uma observação. Executar suíte fornecida separadamente e classificar seis fixtures de executável Linux e expectativa Pi; não desligar preflight.

**Aceite obrigatório:** XML, comando, versão e exit code salvos. Controles de autorização do Core, catálogo e redirect continuam passando. Asserções de zero efeito permanecem.

### CN-00.03 — Inventariar operações anunciadas e reais

**Achados:** A02, A09, A10, A11.

**Implementar:** Criar tabela: entrada CLI/IPC/WSS, scope/grant exigido, operação NXL, método Core, fonte do operation_id, receipt, eventos e recovery. Incluir open, submit, steer, interrupt, close, approvals/input, query, reconcile, attach/detach lane e lease. Separar recurso já existente de novo efeito. Não anunciar verbos ainda sem wiring.

**Aceite obrigatório:** Cada capacidade anunciada tem rota e teste; cada ausência é BLOCKED/UNSUPPORTED com diagnóstico. Plano original C00–C10 não fica globalmente DONE por conveniência.

### CN-00.04 — Definir contrato entre os três agentes

**Achados:** A17, A11.

**Implementar:** Core mantém protocolos/qualificação/catálogo. Connector implementa segurança do transporte e ciclo do host. Server implementa grants, intenção canônica, decisão de binding e projeção de UI. Registrar mensagens/rotas compartilhadas usando schemas reais e casos de uso, sem converter mocks atuais em especificação unilateral.

**Aceite obrigatório:** Documento de handoff com owners, versão exata e dúvidas de contrato resolvidas antes dos respectivos efeitos; sem implementar outro MCP ou outra inbox.

## CN-01 — Core atual e seleção inequívoca

**Dependências:** CN-00.

### CN-01.01 — Atualizar dependência e testar por wheel

**Achados:** A17.

**Implementar:** Escolher versão concreta do Core com catálogo, availability, correções duráveis e identidade da instalação quando disponível. Na data da auditoria, só foi fornecido código até 0.2.9 e o plano C11: não presumir API futura. Fixar versão/hash e empacotar por wheel; remover necessidade de clone irmão nos testes de packaging usando caminho de artefato explícito. Qualificar a API websockets por import explícito e versões testadas.

**Aceite obrigatório:** Mesmo artefato nos consumidores; nenhuma importação privada de factory/provider; instalação e -I imports funcionam fora da fonte; matriz de versões declarada.

### CN-01.02 — Preservar candidato completo na persistência

**Achados:** A01.

**Implementar:** Versionar BindingRecord para arquitetura, build/fingerprint, alvos locais e referência/revisão do inventário. Migrar registros antigos aditivamente. Candidato incompleto exige rediscovery; não preencher com valores inventados para passar allowlist. Deixar o Core verificar o build real no prepare/open.

**Aceite obrigatório:** Teste01 e evidência de qualificação com arquitetura passam. Migração não muda agent_id/chave/histórico e não converte UNKNOWN em READY.

### CN-01.03 — Usar projeção técnica do Core e publicar inventário seguro

**Achados:** A17.

**Implementar:** Consultar get_runtime_catalog e a API pública de disponibilidade no host executor. Não reproduzir qualified_build/preflight nem copiar arrays no daemon. Publicar snapshot seguro versionado com executor_id, inventory_revision e TTL; manter caminhos físicos locais. Campos novos em NXL só com revisão explícita; alternativa é API versionada de aplicativo acordada.

**Aceite obrigatório:** Server local usa fatos locais; Server remoto recebe fatos do Connector. Plataforma incompatível/build desconhecido/attach não qualificado não habilitam binding.

### CN-01.04 — Resolver a seleção exata e manter aliases de apresentação

**Achados:** A14, A17.

**Implementar:** A opção da CLI deve transportar ref/objeto de instalação, não apenas adapter_id. Resolver exatamente um candidato por executor+adapter+ref+revisão. Não usar matches[0]. Consumir contrato C11 real quando entregue; o usuário pode diferenciar A/B por rótulo local, nunca por índice persistente. Duas cópias byte-idênticas são casos obrigatórios.

**Aceite obrigatório:** Teste24 seleciona B; reordenar inventário não altera escolha. Referência legada ambígua exige reseleção explícita; não rotacionar chave ou trocar runtime silenciosamente.

### CN-01.05 — Coordenar rebind, drift e cache do host

**Achados:** A01, A03, A14.

**Implementar:** Comparar alvo selecionado/configuração em create_binding antes de reutilizar registro. Rebind faz diff/CAS com aprovação e revisão nova, ou recusa mudança; não devolver binding velho dizendo que novo candidato foi aplicado. Cache por chave completa e revisão; validade da configuração e callback por sessão não ficam presos na primeira chamada de build.

**Aceite obrigatório:** Atualização de binário aprovada é efetiva; mudança sem aprovação falha. Duas sessões recebem seus próprios dados e um binding de outro Server não reutiliza instância.

## CN-02 — Autoridade, lanes e namespaces

**Dependências:** CN-00, CN-01.

### CN-02.01 — Introduzir chaves de escopo completas

**Achados:** A03, A09.

**Implementar:** Substituir chaves string soltas em runtimes, pumps, lease tasks, approvals, configuração e remoções por tipos imutáveis que incluam Server/executor e o ID pertinente. Mudar assinaturas locais que só recebem session_id para aceitar chave resolvida. Aliases podem continuar amigáveis, mas duplicatas exigem scope explícito.

**Aceite obrigatório:** Teste02 e variantes com mesmos IDs em A/B passam. Remover/rotacionar/acknowledge uma instalação não toca as demais.

### CN-02.02 — Separar socket de autorização para operação

**Achados:** A02, A12.

**Implementar:** Estados: transport conectado, negociação válida, geração corrente, reconciliação concluída, lane admitida. Usar contrato real para definir como attach é confirmado; enqueue por si não é prova. Não marcar runtime elegível no connect TCP. Rejeitar comandos prematuros antes do despacho com código estável.

**Aceite obrigatório:** Testes05/06/07 passam; welcome válido adota a geração que o Server autorizou; contador local de tentativas não vira autoridade; fila cheia não marca attached.

### CN-02.03 — Construir ExecutionContext somente de evidência autenticada

**Achados:** A02.

**Implementar:** Validar igualdade de Server/executor/agente/binding/workspace, owner/connection generations, revisões e grant efetivo. Guardar ações e prazo aprovados por sessão/lane. Deadline usa relógio local ancorado à recepção e duração validada, nunca max(30) para ressuscitar prazo. Remover fallbacks de teste em produção. Frame não pode portar simplesmente ações autodeclaradas; usar grant verificado pelo host.

**Aceite obrigatório:** Testes03/04/22 passam; frame errado gera zero send_turn com Core real e peer. Controle da ação originalmente negada continua recusado; accept/input não herdam exceção temporal de interrupt/deny.

### CN-02.04 — Renovar e revogar lanes corretamente

**Achados:** A12, A02.

**Implementar:** Guardar expiração/ticket_epoch por lane. Renovação single-flight com jitter e política de retry de autenticação. Bootstrap seleciona um binding e exatamente sua identidade. reload_state executa diff de adição, remoção e rotação; fecha concessões antigas antes de aplicar epoch nova. Lane nova em socket online precisa completar attach, não só entrar no dicionário.

**Aceite obrigatório:** Vários agentes, expiração, revogação seletiva, credencial trocada e fila urgente saturada são exercitados. Nenhuma chave de A solicita ticket de B por ordem da lista.

### CN-02.05 — Tratar NXL por operação sem descartar metadados

**Achados:** A02, A11.

**Implementar:** Passar operação validada completa ao manager, preservando operation_id, intent_hash, expected_turn_id, payload e contexto derivado. Usar DTOs/reducers públicos do Core. Não chamar stop local de uma operação remota gerando outro ID. Proteger erros do Core e mapear recibo/erro sem perder possible_effect.

**Aceite obrigatório:** Remote close recebe e devolve o mesmo ID; replay idêntico é consulta, não novo efeito; CoreError conhecido vira frame válido e não derruba todo o receiver.

## CN-03 — Transporte seguro, filas e responsividade

**Dependências:** CN-02.

### CN-03.01 — Aplicar origem/TLS antes de segredos

**Achados:** A06.

**Implementar:** Comparar scheme/host/porta normalizados; exigir HTTPS/WSS fora de loopback e verificar certificados. Exceção de loopback explícita para laboratório, não nome remoto qualquer. Corrigir prefixo na importação MCP e urljoin no tratamento de redirect. Mudança de origem aprovada precisa de revisão/consentimento, não atualização silenciosa do perfil conhecido.

**Aceite obrigatório:** Testes10/12 e redirect control passam; nenhuma requisição autenticada é entregue para HTTP remoto, host por prefixo ou redirect cruzado. Casos de porta/default port/userinfo testados.

### CN-03.02 — Corrigir fila sem perda e tratar backpressure

**Achados:** A04.

**Implementar:** Implementar retirada única por get, com prioridade e fairness; nenhuma task concorrente pode consumir item e ter seu resultado descartado. Limites por bytes/itens. Distinguir fila cheia de mensagem aceita: callers de attach/receipt/event devem conservar a obrigação ou emitir erro explícito.

**Aceite obrigatório:** Teste08 entrega urgent e normal exatamente uma vez. Cancelamento e dois puts simultâneos não perdem dados. Flood normal não bloqueia interrupt/revoke.

### CN-03.03 — Desacoplar reader e scheduler de execução

**Achados:** A05.

**Implementar:** Reader somente decodifica, valida e encaminha. Scheduler tem limites por lane/sessão e tarefas possuídas; controles independentes não esperam fim de submit. Admissão deve ser durável e idempotente antes do efeito, utilizando Core. Não lançar asyncio.create_task ilimitado por frame.

**Aceite obrigatório:** Teste26 recebe interrupt enquanto submit está preso; ACK/heartbeat continuam processados; resultados fora de ordem mantêm correlação; fechar socket não duplica trabalho.

### CN-03.04 — Implementar watchdog e limites de estado de conexão

**Achados:** A05, A12.

**Implementar:** Watchdog periódico ou wait com deadline próprio, atualizado por atividade válida recebida. Integrar ping/pong de biblioteca sem confundir com autorização/lease. Fechamento normal de sender, timeout, GOAWAY e falha fatal/transiente têm transições explícitas. Backoff não reexecuta intenção.

**Aceite obrigatório:** Teste09 passa com tasks ainda pendentes e tráfego saudável renova só indicador de atividade. Com socket vivo mas peer aplicativo parado, o estado não permanece READY indefinidamente.

### CN-03.05 — Remover cópia ilimitada de eventos e validar ACK

**Achados:** A13.

**Implementar:** Manter journal como fonte do replay, ler lotes por orçamento e guardar apenas cursores/notificações em memória. Scope do ACK inclui Server/executor/sessão/epoch/generation e sequência monotônica contígua realmente enviada. Observador lento causa backpressure, não armazenamento RAM ilimitado. Core ACK só após confirmação durável do Server.

**Aceite obrigatório:** Carga offline cresce no journal limitado, não proporcionalmente em listas Python. ACK futuro/antigo/cruzado não compacta dados; reconnect drena do cursor; resource controls progridem sob flood.

## CN-04 — Idempotência HTTP e reconciliação de restart

**Dependências:** CN-02, CN-03.

### CN-04.01 — Classificar incerteza por estágio do pedido

**Achados:** A07.

**Implementar:** Separar prova pré-envio de resposta perdida após possível efeito. Não concluir possible_effect=False de ReadTimeout/HTTPError/5xx genérico em mutação. Registrar uma intenção estável antes do primeiro POST e reutilizá-la na consulta/resolução. Métodos read-only possuem política própria.

**Aceite obrigatório:** Teste11 preserva unknown após transporte receber POST. Controle connect failure comprovadamente anterior continua seguro; nenhuma orientação de retry com ID novo quando efeito é possível.

### CN-04.02 — Unificar autoria da operação local/remota

**Achados:** A07, A11.

**Implementar:** Acordar com Server a semântica de intents:resolve e runtime/operations: qual registra autorização, qual produz efeito, qual recebe receipt. O Connector nunca executa localmente e depois solicita uma segunda execução canônica do mesmo trabalho. Falha ao publicar receipt local não elimina esse receipt.

**Aceite obrigatório:** Peer de Server conta uma execução por intenção sob sucesso, ACK perdido e retries. Resultado local persiste e é consultável mesmo se HTTP subsequente falhar.

### CN-04.03 — Serializar reconciliação com schema real

**Achados:** A09.

**Implementar:** Passar namespace originário explicitamente. Construir snapshots e receipts com projeções tipadas que encode/decode NXL aceitam; não str(dataclass). Agregar runtimes corretos do mesmo scope, sem primeiro binding. Rejeitar campos extra/version skew deliberadamente.

**Aceite obrigatório:** Testes16/17 passam; relatórios não vazios, sessão antiga e dois Servers com mesmos IDs geram frames corretos e segregados.

### CN-04.04 — Consultar journal na ausência de runtime em memória

**Achados:** A09.

**Implementar:** No startup e reconexão ler claims/recibos/epochs/fences do Core por suas APIs públicas. Não interpretar ausência de handle como inexistência de operação ou como morte. Não reabrir pipes nem retomar PID sem ownership suportado. Reconciliação deve preceder novas admissões produtivas.

**Aceite obrigatório:** Daemon reinicia após commit local e antes de publicar receipt: responde sobre a mesma operação sem segundo spawn/turn. Vazio é usado somente se realmente não há evidência no namespace solicitado.

### CN-04.05 — Reativar pumps e conservar watermarks com limites

**Achados:** A13, A09.

**Implementar:** Rastrear tasks de evento por SessionKey e estado do stream. Recuperar reader que falhou com backoff limitado; logs efêmeros não substituem journal. Ao reconectar transmitir apenas projeção/deltas autorizados. Reaproveitar operações Core de acknowledge/compact, sem outra inbox.

**Aceite obrigatório:** Erro temporário do sender não abandona permanentemente o stream; replay/ACK duplicado preserva sequência e idempotência; remover um Server não cancela pumps de outro.

## CN-05 — Lifecycle do host sem perder o supervisor

**Dependências:** CN-02, CN-04.

### CN-05.01 — Bloquear novas admissões no início do drain

**Achados:** A08.

**Implementar:** Estado draining verificado por IPC e WSS antes de resolver/persistir novo trabalho. Não impedir consultas e contenção autorizadas. Registrar aberturas em andamento e deixar Core cuidar de resultados tardios sem cancelar produtores quando só o request termina.

**Aceite obrigatório:** Solicitar shutdown e depois start/submit produz recusa anterior ao efeito; abertura já admitida conserva resultado/ownership até resolução.

### CN-05.02 — Separar solicitação de stop e resultado físico

**Achados:** A08.

**Implementar:** Criar StopAttempt por sessão com ID e resultado. Falha HTTP comprovadamente anterior deve permitir retomada; resultado desconhecido conserva tarefa/sessão técnica. Não apagar _sessions/_pumps por qualquer receipt. Remover só depois de estados físicos/duráveis compatíveis fornecidos pelo Core.

**Aceite obrigatório:** Testes13/14 passam; segundo stop não fica preso em already_closing falso. Timeout depois do efeito não é seguro para repetir a operação com ID novo.

### CN-05.03 — Unificar encerramento das duas camadas de host

**Achados:** A08.

**Implementar:** Consolidar RuntimeManager.shutdown e CoreRuntimeHost.shutdown_all sob um proprietário de lifecycle. Aguardar sem destruir produtores do Core; orçamento total é explícito e há controles independentes para múltiplos runtimes. Não excluir instância com unknown nem fechar stores antes das obrigações que os utilizam.

**Aceite obrigatório:** Teste15 passa. Com um Core unknown e outro resolvido, relatório conserva ambos corretamente e o segundo não é atrasado indevidamente pelo primeiro.

### CN-05.04 — Relatar graceful, forced e pending com fatos

**Achados:** A08.

**Implementar:** Remover inferred forced=possible_effect e graceful após qualquer stop. Agregar ShutdownReport, receipts e pendências duráveis; definir códigos de saída e relatório estruturado. Se processo aplicativo precisar sair com unknown, justificar transferência para contenção/guardian/recovery do SO com evidência, não retorno zero genérico.

**Aceite obrigatório:** Não afirmar morte sem observação nem persistência sem commit; CLI e log expõem pendência recuperável; segundo lifecycle público consegue concluir sem imports privados.

### CN-05.05 — Inicialização e disposal single-flight

**Achados:** A08, A03.

**Implementar:** Proteger criação do journal/ledger/RuntimeCore contra starts concorrentes e distinguir ownership de recursos injetados. Verificar construtores síncronos e StateStore no event loop com teste de contenção, migrando operações bloqueantes para worker quando necessário. Não alterar o Core para esconder lifecycle incorreto do host.

**Aceite obrigatório:** Dois starts compartilham exatamente o store pretendido, não workers órfãos. Cancelamento do waiter não destrói initialization/commit em voo. Recursos resolvidos são fechados por API pública.

## CN-06 — Ligar ferramentas e controles remotos de verdade

**Dependências:** CN-01, CN-02, CN-05.

### CN-06.01 — Instalar configuração de cliente MCP HTTP por sessão

**Achados:** A10.

**Implementar:** Usar templates públicos do Core para URL do Server e referência de capability. Gerar env/argv/config efêmera no formato qualificado do harness. Preencher LaunchOverlay ou estrutura equivalente realmente consumida por prepare/start. Não iniciar MCP stdio, HTTP proxy, relay WSS ou SDK servidor no Connector.

**Aceite obrigatório:** Teste27 passa; peer de processo observa URL/credencial de sessão esperada. O tráfego de ferramenta vai diretamente harness→Server; tools-only funciona sem daemon.

### CN-06.02 — Resolver credenciais por sessão, inclusive reuso do binding

**Achados:** A10, A03.

**Implementar:** Evitar callback fechado sobre a capability da primeira sessão. Registrar grant/env/config pelo session/open token e remover referências somente após fim apropriado. Cada processo recebe apenas suas credenciais de provider e capability Nexus limitada. Expiração/rotação não copia chave canônica como fallback.

**Aceite obrigatório:** Duas sessões sucessivas e concorrentes usam tokens diferentes; nenhum token da primeira aparece na segunda; config/prompt/logs não expõem o segredo global.

### CN-06.03 — Compor bridge Pi nativa no daemon

**Achados:** A10.

**Implementar:** Fornecer callback pi_native_action e backend para APIs canônicas, via portas públicas do Core. Validar grant, escopo, duração, ações e ID. Integrar assets empacotados do Core, não duplicar parser JS nem interpretar texto de modelo. Configurar login/provider refs somente por fluxo local aprovado.

**Aceite obrigatório:** Teste exercita import da aplicação Connector, start Pi sintético e ação passando pelo backend HTTP controlado. Testar biblioteca Core isolada não encerra essa tarefa. Não há endpoint MCP.

### CN-06.04 — Implementar matriz remota declarada

**Achados:** A11.

**Implementar:** Conectar remote open, steer, interrupt com expected_turn_id, close com receipt, query/reconcile e aprovações/input suportados. Preservar hash/IDs/correlação e usar capacidade específica do adaptador. Modos inexistentes/attach não qualificado permanecem indisponíveis.

**Aceite obrigatório:** Server peer controla ciclo inteiro pelo WSS sem CLI iniciar previamente a sessão. Falta de capacidade retorna erro antes do efeito; remote close conserva ID e receipt.

### CN-06.05 — Aprovação humana e nativa com estado bifásico

**Achados:** A11, A02.

**Implementar:** Separar queue de solicitação, reserva de decisão e resultado confirmado/unknown. Não remover pedido antes da validação/network. Exigir token/capability do operador quando a autoridade diferir do próprio agente; o Server continua dono da decisão. Chamar Core.decide_native_approval no caminho final com request/turn corretos.

**Aceite obrigatório:** Decisão inválida não consome pedido. ACK perdido mantém consulta sem segunda resposta. Accept expirado é negado; decline seguro de pedido vigente segue contrato. Outro agente/request_id em outro Server não substitui o pedido.

## CN-07 — CLI, estado e autostart

**Dependências:** CN-01, CN-05.

### CN-07.01 — Corrigir bind create e uso de identidade importada

**Achados:** A14.

**Implementar:** Trocar _ImportStub por DTO real; usar referência do cofre do agente quando apropriado, sem nova coleta manual recorrente da chave. Validar a identidade do binding antes de mutações. Integrar sucesso com reload de estado efetivo.

**Aceite obrigatório:** Teste25 passa e criação usa a identidade selecionada. Não criar usuário ou novo agente para contornar a falha.

### CN-07.02 — Respeitar headless e consentimento

**Achados:** A14.

**Implementar:** Propagar non_interactive até a confirmação agregada. Definir flag/consentimento previamente aprovado para automação ou devolver erro prescritivo. Não converter modo headless em aprovação automática. Resolver primeiro a seleção exata de CN-01.

**Aceite obrigatório:** Teste19 passa sem prompt; configuração incompleta retorna código estável; execução aprovada usa mesmos escopos que modo humano.

### CN-07.03 — Consolidar writer do estado

**Achados:** A15.

**Implementar:** Implementar save por writer atômico existente sob lock; especificar replace versus CAS. Schema migration preserva campos, aliases e refs de segredo. Não efetuar operações de arquivo que bloqueiam o loop em callbacks sensíveis.

**Aceite obrigatório:** Teste18, falha antes do replace, update concorrente e permissões passam; state corrupto é diagnosticado sem sobrescrever automaticamente.

### CN-07.04 — Gerar serviços corretos no root aprovado

**Achados:** A16.

**Implementar:** Usar plistlib com booleans/dict reais; incluir --state-dir/env em launchd e task Windows; lidar com espaços e XML. Linux recebe usuário real, unidade com escaping correto e daemon-reload quando necessário. Derivar a matriz de sobrevivência de campanhas reais.

**Aceite obrigatório:** Testes20/21 passam; em cada SO-alvo serviço inicia o mesmo root e cofre. Não marcar boot/logout PASS apenas por gerar texto.

### CN-07.05 — Verificar ownership de configuração e remoções

**Achados:** A03, A10, A14.

**Implementar:** Indexar registros de campo por Server/binding/arquivo/entry; aplicar diff/CAS/backup e remover apenas campos comprovadamente próprios. Alias/root/id iguais de outro Server permanecem. Upgrade deve drenar ou recusar sessão ativa, não prometer retomar pipes.

**Aceite obrigatório:** Dois arquivos MCP existentes e duas identidades: apply/remove/rebind de A preserva B e terceiros. Falha intermediária não apaga histórico ou chave.

## CN-08 — Aceite por camada e integração vertical

**Dependências:** CN-03, CN-04, CN-05, CN-06, CN-07.

### CN-08.01 — Executar regressões e testes derivados dos achados

**Achados:** A01, A02, A03, A04, A05, A06, A07, A08, A09, A10, A11, A12, A13, A14, A15, A16, A17.

**Implementar:** Rodar testes recebidos antes/depois duas vezes e incluir cenários estáticos complementares. Preservar controles positivos. Criar casos sem mock de qualificadores para ao menos uma combinação real autorizada; fixtures Linux corrigem executabilidade sem habilitar builds falsos.

**Aceite obrigatório:** Cada achado tem test node/camada/resultado; 34 testes recebidos não substituem cenários ausentes; nenhum PASS por xfail, permissões inventadas ou desativação de preflight.

### CN-08.02 — Qualificar pacote fora da árvore

**Achados:** A17, A16.

**Implementar:** Construir wheel/sdist, instalar em ambiente limpo com fonte explícita do wheel Core, executar CLI/catalog/availability e contratos. Não escolher lexicograficamente último wheel de clone irmão. Testar mínimo de versões de bibliotecas ou restringir faixa. Validar rfc8785 real e recursos empacotados.

**Aceite obrigatório:** Hash e versão exatos registrados; Core e Connector não dependem da árvore-fonte nem pacote Server. Versões incompatíveis falham cedo com diagnóstico.

### CN-08.03 — Executar um caminho integrado real antes de ampliar matriz

**Achados:** A01, A02, A09, A10, A11.

**Implementar:** Com autorização, Server em A sem executáveis/projetos/credenciais dos providers; Connector em B com o mesmo Core escolhido. Criar binding via UI/CLI com catálogo remoto, iniciar runtime remotamente, enviar tarefa inocente, receber eventos, decisão necessária, interrupt/close e reconnect. Repetir Server+Core local sem Connector.

**Aceite obrigatório:** IDs e recibos preservados; Server não valida paths remotos localmente; ferramentas MCP HTTP diretas; nenhum segundo efeito depois de resposta perdida. Isso é gate integrado, não teste do Core sozinho.

### CN-08.04 — Revisar status e entregar responsabilidades remanescentes

**Achados:** A17.

**Implementar:** Atualizar C00–C11 com prova por requisito: implementado, demonstrado com fixture, qualificado em SO/provider, integrado entre hosts. Redigir relatório de saída com commits, matrizes, XMLs, hashes, migrações e owners de pendências. Não publicar pacote/push sem autorização.

**Aceite obrigatório:** Nenhuma declaração global DONE sobre funções sem wiring. Pendência externa de Server distingue-se de defeito interno do Connector. Desenvolvimento paralelo continua com contrato fixado; liberação limitada ao escopo realmente comprovado.

## Gates de entrega

**G0 — Artefato para desenvolvimento:** imports/catalog/codecs instaláveis, limitações explicitadas. Não significa execução distribuída pronta.

**G1 — Connector corrigido no escopo de laboratório:** bloqueios P1 encerrados, nenhuma rota anunciada sem implementação, receipt/reconcile/cancelamento e isolamento provados com peers que exercitam aplicação e Core reais nas fronteiras pertinentes. Não equivale a provider/SO qualificado.

**G2 — Integração delimitada:** um adaptador real e um conjunto de plataformas demonstrados em Server local e Server remoto+Connector com o mesmo wheel Core. Ferramentas, decisões e partições examinadas; agentes e seleção sem duplicação de listas.

**G3 — Escopo original completo:** demais combinações prometidas, autostart e J-matrix fechadas com evidência própria. Attach só entra quando efetivamente qualificado. Nenhum gate é concedido por número bruto de testes ou por herdar evidência do Core sem testar o host.

## Registro por tarefa

Guardar task_id, achados, commit, caminhos, teste/node, plataforma, Core/wheel hash, comando, exit code, evidência antes/depois, estado de implementação, estado de qualificação e pendências. Logs devem ser redigidos; tokens de laboratório não devem ser substituídos por credenciais reais em testes automáticos. Não excluir os testes que falhavam do relatório final.

## Handoff dos aplicativos

O Server deve aceitar/prover operações e grants versionados, validar o agente e mostrar catálogo remoto. O Connector deve construir os fatos/receipts que preservem essa autoridade e o Core deve resolver os candidatos e atuar por seus contratos públicos. Qualquer API nova de resolução C11 precisa existir no artefato adotado; não há autorização para inventar nomes e depois exigir adaptação silenciosa do outro repositório.

Não criar painel web/MCP novo no Core para transportar catalogue. Não converter simulação em evidência de rede real. Depois de um caminho vertical passar, ampliar a matriz deliberadamente em vez de continuar produzindo camadas de abstração sem executar o fluxo completo.
