# CN5 — instruções de implementação para o agente executor

**Repositório:** `okto-nexus-connector`. **Baseline auditado:** `87b8fd2e3e403cb6a70a1ce265618e29c7a6b86c`, versão `0.4.0.dev0`. **Dependência testada:** Core `0.2.10.dev0`, fonte correspondente a `1560d314ed2b478515dcbbe533436d7d0b027b09`.

**Escopo:** quatro grupos confirmados, Q01–Q04. São 6 fases e 20 tarefas. Este documento define as decisões a implementar; não solicita ao executor que escolha uma nova arquitetura. Conservar o plano original, CN1–CN4 e os respectivos comportamentos aprovados. Trabalhar no HEAD atual, sem reset. Não publicar, fazer push, alterar permissões ou utilizar credenciais de produção.

## 1. Resultado obrigatório

O caminho de aprovação deve receber a proposta sem destruir os campos exigidos pelo Core, selecionar a sessão no namespace autenticado, confirmar a decisão no Server antes do efeito e manter sua execução independente do cliente que espera. A rotação de uma lane deve iniciar o novo attach mesmo quando o anterior estava em andamento. O publicador de eventos deve retomar depois de falha transitória, inclusive quando não há novo evento do harness.

Não alterar transportes, não criar MCP stdio/HTTP/proxy no Connector ou no Core. MCP HTTP continua diretamente entre harness e Server. Não copiar adaptadores, parsers, regras de qualificação ou catálogo do Core. A identidade canônica é a do agente.

## 2. Provas desta rodada e prioridades

| ID | Problema confirmado | Prioridade | Reprodução |
|---|---|---|---|
| Q01a | A entrada `_on_remote_approval` transforma `request_hash` em `[redacted]`; approve e deny confirmados no Server falham no Core | P1 funcional | Duas parametrizações P01 do CN4 original adaptado |
| Q01b | O Server B é consultado corretamente, mas a etapa nativa seleciona o Core A pelo `session_id` isolado | P1 de isolamento | `test_q01_foreign_approval_never_selects_a_core_in_other_namespace` |
| Q02 | Cancelamento do waiter cancela a chamada ao Server e deixa `server_pending` sem produtor ativo; repetição dá conflito permanente | P2 | `test_q02_cancel_waiter_does_not_cancel_accepted_server_decision` |
| Q03 | Rotação cancela attach anterior, mas o scheduler rejeita a criação do substituto porque a task cancelada ainda não terminou | P2 | `test_q03_rotation_reschedules_after_old_attach_is_cancelled` |
| Q04 | Falha de leitura mata a task do stream; reconexão só sinaliza um Event sem reiniciar a task | P2 | `test_q04_event_stream_recovers_on_reconnect_without_new_native_event[transient_page_failure]` |

Q01b observa o contexto efetivamente passado ao Core real. O Core ainda recusa o hash corrompido; não foi demonstrada aplicação nativa não autorizada. Não remover Q01a sem corrigir Q01b: uma recusa acidental por dado inválido não substitui isolamento.

## 3. Decisões fixas para implementar

### 3.1. Duas representações da aprovação

Adicionar `services/approval_state.py` com os DTOs internos abaixo. Eles pertencem ao Connector, não à API do Core ou ao NXL. Utilizar dataclasses tipadas; chaves e alvo são imutáveis.

```python
@dataclass(frozen=True, slots=True)
class ApprovalKey:
    server_id: str
    executor_id: str
    request_id: str

@dataclass(frozen=True, slots=True)
class ApprovalTarget:
    key: ApprovalKey
    binding_id: str
    agent_id: str
    workspace_id: str
    session_key: SessionKey | None
    # Snapshot aprovado/local para conferir mudanças antes do efeito.
    connection_generation: int | None
    session_owner_generation: int | None
    authorization_revision: int
    configuration_revision: int
```

`PendingApproval` possui `target`, `proposal_bytes` imutáveis, `display_proposal` redigida, tipo do pedido, recebimento e tentativa atual. `proposal_bytes` é uma cópia JSON profunda do payload operacional recebido e validado; não é o texto mostrado ao usuário. A cópia conserva `request_id`, `request_hash`, `method`, `params` e seus valores. Não recalcular `request_hash`: ele corresponde à proposta já observada pelo Core.

A projeção `to_public_dict()` é a única usada por `approvals.pending`, logs, histórico de apresentação e export. Ela pode mostrar os IDs necessários à seleção e a fase, mas redige conteúdo/segredos. Nunca serializar o dataclass interno inteiro. Manter os limites existentes de frame e não persistir respostas sensíveis brutas em `StateStore`.

### 3.2. Estados da tentativa de decisão

Usar `DecisionAttempt` com `attempt_id`, `ApprovalKey`, alvo, decisão local, decisão Core, digest da intenção, `native_operation_id`, `producer_task`, fase, confirmação do Server, recibo/erro Core e resultado público. Não usar o segredo como chave. O `native_operation_id` é derivado uma vez de uma serialização canônica de namespace completo + request_id + identidade da decisão; limite de ID compatível com o Core. Nenhum retry automático gera um novo ID.

| Fase | Efeito permitido | Repetição do mesmo pedido |
|---|---|---|
| PENDING | Nenhum | Pode reservar uma tentativa validada |
| SERVER_PENDING | Um produtor de confirmação ativo | Compartilhar sua consulta/resultado; não outro POST |
| SERVER_UNKNOWN | Nenhuma nova resposta nativa | Informar incerteza; sem novo POST automático |
| SERVER_REFUSED | Nenhum efeito nativo | Resultado terminal consultável |
| SERVER_CONFIRMED | Aplicação nativa só ao alvo ainda compatível | Nunca repetir o POST já confirmado |
| NATIVE_PENDING | Um produtor Core ativo | Compartilhar consulta/resultado |
| NATIVE_REFUSED | Nenhum byte demonstrado/recusa tipada | Resultado terminal, não falso “em andamento” |
| NATIVE_UNKNOWN | Efeito pode existir | Consultar o mesmo recibo; não reenviar |
| APPLIED | Resultado conhecido | Devolver o resultado da mesma tentativa |

O usuário pode iniciar uma nova decisão somente pelo fluxo explicitamente autorizado e com novo pedido/consentimento quando exigido pelo Server. Não resetar `NATIVE_REFUSED` para `PENDING` e não repetir o POST canônico para resolver um erro local.

### 3.3. Geração do attach

Cada lane mantém um contador local `attach_generation` e uma única task coordenadora. Cada tentativa captura, ANTES do primeiro await, provider, agent_id, credential_epoch, authorization_revision, ticket_epoch, identidade da conexão e attach_generation. Não ler valores mutáveis da lane depois de obter um ticket antigo e usá-los para rotular esse ticket como novo.

### 3.4. Dono do publicador

Um stream possui no máximo uma task publicadora e um `TimerHandle` de retry. Criar `_ensure_stream_task(key)` e `_schedule_stream_retry(key)`. Todas as entradas `publish`, `ack_arrived` e `transport_online_again` chamam `_ensure_stream_task` antes de sinalizar o Event. Separar `_closed` do bridge de offline do transporte. `drain()` encerra o bridge; reconexão não o encerra.

## CN5-00 — Preparação e baseline

### CN5-00.01 — Conferir entradas e preservar os arquivos

1. Registrar `git rev-parse HEAD`, branch, `git status --short`, Python/SO, Core e hashes.
2. Comparar o HEAD com 87b8fd2. Se o achado já estiver corrigido, executar sua reprodução e preservar a mudança; não reimplementar por convenção.
3. Copiar o pacote completo para `plans/correction-connector-05/`, incluindo `regressoes/` e `executar_verificacao.py`.
4. Não editar dependências para fazê-las declarar a versão desejada; instalar o artefato real pinado.

**Aceite:** baseline e comando identificados; nenhuma alteração do usuário perdida.

### CN5-00.02 — Executar as provas antes de corrigir

1. Rodar o runner deste pacote. Na baseline, o conjunto CN4 adaptado + CN5 dá 6 FAIL / 14 PASS.
2. Rodar separadamente `tests/regressoes/test_connector_audit_cn4.py`: 18 PASS na baseline.
3. Ler `evidencias/adapted_cn4.diff`: só inicializa o novo histórico e conecta a enumeração da fixture ao novo serviço que fornece candidatos completos. Não mudar proposta, hash, decisões, namespaces ou asserções.
4. Conservar XMLs iniciais. Erro de setup é corrigido na fixture, não classificado como defeito.

**Aceite:** reproduções distinguem os caminhos. Não substituir a entrada `_on_remote_approval` por atribuição direta em `_approvals` nos testes end-to-end.

## CN5-01 — Pedido íntegro e alvo inequívoco

**Dependência:** CN5-00. **Arquivos:** `daemon/app.py`, novo `services/approval_state.py`, `services/runtime_service.py`, `redaction.py` e testes.

### CN5-01.01 — Criar DTOs e projeções separadas

Implementar os tipos da seção 3.1. `PendingApproval` guarda uma cópia operacional imutável e uma projeção redigida. Para os campos conhecidos do pedido nativo, validar tipos/limites sem alterar seus valores. Campos e método do provider continuam sendo validados definitivamente pelo Core; não criar codec nativo no Connector.

Em `redaction.py`, manter a proteção geral de textos. Não apagar `_HEX64` globalmente. O ajuste ocorre na escolha de qual representação será redigida. Hashes podem ser omitidos/redigidos na apresentação, mas o valor operacional deve continuar idêntico.

**Aceite:** proposta original e `proposal_bytes` são equivalentes; mudar o objeto recebido depois da inserção não altera o registro. A saída de logs/IPC não revela tokens de laboratório classificados como segredo.

### CN5-01.02 — Corrigir o recebimento real

Substituir a atribuição de `proposal=redact_mapping(...)` em `_on_remote_approval` pelo armazenamento de `PendingApproval`. Conservar `executor_id` do canal/frame validado, atualmente descartado. Não inferir o Server com alias, prefixo ou split.

Resolver o binding por Server + executor + binding_id. Quando o schema r3 não transportar agente/workspace, completar esses fatos SOMENTE do binding local encontrado por essa chave; se os campos estiverem presentes, exigir igualdade. Ausência ou ambiguidade do binding resulta em erro tipado antes da confirmação remota.

Um replay do mesmo pedido, com mesma chave e mesma proposta, conserva tentativa e fase. Uma proposta diferente sob a mesma chave retorna conflito; não sobrescrever uma tentativa em voo por `phase='pending'`. Pedido já terminal consulta o tombstone limitado.

**Aceite:** approve e deny atravessam `_on_remote_approval` → `dispatch(approvals.decide)` → Server peer → manager → Core real → peer nativo, sem atribuição direta em `_approvals` pela fixture.

### CN5-01.03 — Tornar a aplicação nativa obrigatoriamente escopada

Alterar a assinatura interna do manager para parâmetros nomeados:

```python
async def decide_native_approval(
    self, *, target: ApprovalTarget, operation_id: str,
    request: Mapping[str, object], decision: str,
    response: Mapping[str, object] | None,
) -> dict[str, object]: ...
```

Implementação obrigatória:
1. Se `target.session_key is None`, esta função não deve ser chamada.
2. Resolver com `session_by_key(target.session_key)`. Proibir `session(session_id)` neste caminho.
3. Comparar Server/executor/binding/agente/workspace e as revisões/gerações capturadas com o snapshot autorizado vigente.
4. Divergência retorna `BINDING_NOT_AUTHORIZED` ou `STALE_GENERATION` conforme a dimensão, antes de chamar o Core. Não construir contexto a partir de outra sessão para reparar a divergência.
5. Passar `NativeApprovalOperation` por argumentos nomeados ao Core. O contexto conserva exatamente as ações autorizadas. Não adicionar `approval.decide`, `input.provide` ou controles por default.

No app, remover o teste global `session_id in session_ids()`. Um pedido nativo que aponta para sessão ausente no namespace deve ser recusado; não converter em pedido administrativo. Pedido administrativo é identificado por seu tipo/contrato, não por falha de lookup. Atualizar TODOS os callers e testes da assinatura.

**Aceite:** aprovação de B não chama o Core A, mesmo quando A é a única sessão que possui aquele ID. A e B podem ter o mesmo request/binding/session textual e cada pedido resolve seu recurso correto.

### CN5-01.04 — Conferir alvo e confirmação antes de aplicar

Executar a validação de alvo antes do POST e novamente depois de sua conclusão. A confirmação do Server deve ser estritamente positiva (`True` no contrato atual); valor ausente, string ou objeto não vira autorização por truthiness. Ajustar `NexusHTTPClient.approval_decision` para recusar tipos inválidos de `applied` e manter recusa explícita para `False`.

Traduzir approve→accept e deny→decline depois da confirmação. Usar a proposta operacional, não a projeção. O Core continua sendo a autoridade para reconhecer método, tipo de input e correlação do pedido nativo. Não escolher `input.provide` apenas porque uma string de UI diz input; usar o contexto completo autorizado e a semântica reconhecida da proposta. Não duplicar tabelas de métodos nativos.

Se o alvo mudou durante o POST, conservar a confirmação canônica, registrar `NATIVE_REFUSED` e não responder ao alvo novo. Não voltar a PENDING e não executar outro POST.

**Aceite:** Server recusou = zero chamadas Core; Server confirmou + alvo mudou = zero efeito nativo e resultado local explícito; fluxo legítimo continua passando.

### CN5-01.05 — Completar os testes de entrada, não só do helper

Manter as duas parametrizações P01 recebidas. Acrescentar proposta com texto contendo padrão de token e hash de correlação para demonstrar simultaneamente redaction de apresentação e integridade operacional. Testar dois Servers, mesmo ID e proposta terminal repetida. Verificar a sequência observada `Server confirmado → Core → peer` e a escolha do contexto entregue ao Core.

**Aceite:** Q01a e Q01b passam. O teste de namespace não é satisfeito apenas por um `VALIDATION_ERROR` posterior: deve comprovar zero despacho ao Core estrangeiro.

## CN5-02 — Produtor da decisão e recuperação de erro

**Dependência:** CN5-01. **Arquivos:** `daemon/app.py`, `services/approval_state.py`, IPC e testes.

### CN5-02.01 — Separar produtor da espera IPC

Implementar `_run_decision(attempt)` como task pertencente ao registro de tentativa. `_decide_approval` deve reservar uma única tentativa, criar seu produtor, armazená-lo ANTES de aguardar e usar `await asyncio.shield(attempt.producer_task)`.

O produtor deve possuir o contexto `async with NexusHTTPClient(...)`; ele não pode usar um client pertencente ao waiter. Ao cancelar o waiter, não executar `producer_task.cancel()` nem remover a tentativa. Uma repetição idêntica consulta ou compartilha a mesma task; uma decisão diferente em voo retorna conflito tipado.

**Aceite:** o teste Q02 cancela o waiter mantendo a barreira do Server fechada; o produtor não recebe cancelamento. Depois da liberação, a mesma tentativa termina e pode ser consultada sem segundo POST.

### CN5-02.02 — Finalizar todas as saídas explicitamente

Capturar separadamente `ConnectorError`, `CoreError` e encerramento real do produtor. Mapear código, estágio, `possible_effect`, `retry_safe` e operação sem apagar a causalidade. Não usar `except Exception: phase='pending'`.

Tabela obrigatória:
- HTTP recusa definitiva: SERVER_REFUSED, zero nativo.
- HTTP resultado possivelmente aplicado: SERVER_UNKNOWN, sem nativo e sem novo POST automático.
- HTTP True: SERVER_CONFIRMED; somente esta tentativa pode avançar.
- Core recusa antes do efeito: NATIVE_REFUSED, confirmação canônica conservada e erro consultável.
- Recibo/erro Core com efeito possível: NATIVE_UNKNOWN; consultar o mesmo ID pelo Core.
- Core sucesso: APPLIED.
- Exceção inesperada: gravar erro redigido e estado conservador conforme o último marco; nunca deixar fase que afirma produtor ativo quando task terminou.

**Aceite:** o CoreError que antes deixava `native_pending` torna-se saída tipada terminal/recuperável. Repetição não informa “em andamento” sem task viva e não refaz o POST confirmado.

### CN5-02.03 — Expor consulta e preservar obrigações no lifecycle

Adicionar operação IPC local `approvals.status` por ApprovalKey/attempt_id. Devolver fase, presença de produtor, resultado/erro redigido e ação permitida. Não devolver proposal_bytes, segredos ou resposta sensível integral. `approvals.pending` deve distinguir aguardando operador de decisão em processamento.

O prazo do shutdown limita sua espera; não deve cancelar produtores de decisão só por expirar. Relatar decisões pendentes no drain. Não encerrar o loop alegando que o supervisor persiste. O procedimento existente de transferência/saída com unknown continua pendência qualificada separada.

O CN5 deve comprovar recuperação no mesmo processo. Reinício depois de resultado canônico incerto continua exigindo o contrato de consulta/intent estável do Server, ainda declarado externo. Sem esse contrato, devolver estado BLOCKED/SERVER_UNKNOWN consultável e não emitir novo POST automaticamente. Não inventar endpoint externo nem declarar recuperação pós-crash concluída.

**Aceite:** status corresponde à existência da task; cancelamentos IPC repetidos não multiplicam operações; o relatório distingue pendência local de dependência externa.

### CN5-02.04 — Deduplicar confirmação, aplicação e tombstone

Calcular e conservar digest da intenção validada (namespace, pedido, decisão, resposta por digest; não registrar resposta sensível em claro). Reservar antes do primeiro await. Finalizador verifica attempt_id antes de alterar o registro. Pedido repetido não substitui a proposta nem a task. Tombstone conserva resultado limitado a 128 entradas por política atual, sem segredo como chave.

Se a resposta nativa sofreu falha com possível escrita, consultar o recibo do mesmo `native_operation_id`; não reenviar aprovação para descobrir o resultado. Sucesso terminal repetido devolve o mesmo resultado. Decisão oposta não é apresentada como aplicada; devolver conflito ou resultado anterior explicitamente identificado.

**Aceite:** dois operadores, replay de frame durante POST, timeout e resposta perdida não duplicam produtor nem resposta nativa.

## CN5-03 — Rotação com attach em andamento

**Dependência:** CN5-00. **Arquivo:** `transport/wss_client.py` e callers de reload.

### CN5-03.01 — Capturar a tentativa imutável

Criar `LaneAttachAttempt` interno com token/generation, provider, valores de identidade/revisão e serial da conexão. `_attach_one_lane` recebe essa tentativa e não usa a lane mutável para rotular o ticket depois do await. Ao obter o ticket, conferir se lane e generation ainda são atuais. Resultado obsoleto é descartado; nunca gera attach nem READY.

**Aceite:** provider antigo que conclui tarde não produz frame com ticket antigo e revision/epoch novos.

### CN5-03.02 — Agendar o sucessor na conclusão do anterior

No `add_lane`, mudança real incrementa generation, fecha prontidão e registra `reattach_requested=True`. Se task antiga existir, solicitar seu cancelamento, mas não concluir que terminou. Seu callback deve observar resultado/exceção, conferir que é a task antiga registrada e chamar `_ensure_lane_attach` para o generation ATUAL quando o canal ainda estiver aberto/negociado.

`_ensure_lane_attach` cria no máximo uma task ativa por lane. Não deve simplesmente retornar depois de `cancel()` sem um callback que programe o substituto. Remoção/detach impede qualquer reattach. Rotação 1→2→3 antes de concluir 1 inicia somente o generation 3 necessário.

**Aceite:** Q03 consulta o novo provider e enfileira seu attach sem reiniciar o socket. A task anterior já cancelada não continua sendo o único trabalho registrado.

### CN5-03.03 — Conferir a geração antes de enviar e de marcar READY

Conservar metadados de attempt_generation em envelope interno da fila, sem inserir campo inválido no frame NXL. Antes de serializar o attach e depois do send, verificar que a tentativa corresponde à lane/conexão atuais. Campo de ticket não pode ser aproveitado por outra tentativa. Frames obsoletos são descartados com diagnóstico e não marcam READY.

Manter o estágio de confirmação de attach previsto no contrato compartilhado; não promover envio de bytes a nova autoridade de agente. Esta correção não redefine unilateralmente a negociação com o Server.

**Aceite:** attach antigo concluído depois da rotação não abre lane nova; nova prova segue a geração correta.

### CN5-03.04 — Preservar no-op e recuperação limitada

No-op genuíno não incrementa epoch/generation. Falha transitória do novo provider mantém PENDING e agenda um único retry com backoff, não tarefa por frame. Guardar o TimerHandle para cancelar em remoção/stop. Atualizar expiração apenas pela prova atual e não prolongar lease da sessão pelo ticket.

**Aceite:** controle de rotação de lane READY continua passando; teste em attach pendente também passa; outras lanes não são reiniciadas.

## CN5-04 — Recuperação de publicação sem novo evento do harness

**Dependência:** CN5-00. **Arquivo:** `daemon/app.py:EventBridge`.

### CN5-04.01 — Unificar criação/recriação da task do stream

Implementar `_ensure_stream_task(key)`:
1. Se bridge encerrado, não criar task.
2. Se task atual está viva, devolver a mesma.
3. Se terminou, recuperar sua exceção uma vez e registrar diagnóstico redigido.
4. Criar exatamente uma nova `_flush_loop(key)` e registrá-la antes de devolver.

`publisher_for`, `ack_arrived` e `transport_online_again` chamam o helper antes de sinalizar. `_wakeups` não é prova de que um publicador está vivo.

**Aceite:** Q04 restaura a leitura e chama apenas reconexão, sem publish novo; sequência 1 é enviada/confirmada uma vez.

### CN5-04.02 — Tratar falhas de página/send/ACK com retry possuído

Capturar erros transitórios das portas de leitura, envio e confirmação, mantendo cursores e `_pending_core_ack` conforme o último marco. Não reiniciar do último sent; replay continua após o ACK validado e aplicado. Erro de autenticação/schema terminal registra incompatibilidade e bloqueia aquela rota, sem retry apertado.

Usar um TimerHandle por stream com atraso progressivo 0,25→0,5→1→2→5 segundos, teto 5; reset após progresso. Reconexão pode antecipar a retomada, cancelando o timer existente. `CancelledError` no drain encerra o publicador sem rearmar timer. Não criar payload backlog em RAM; o journal continua fonte.

**Aceite:** leitura, send e ACK falham uma vez e convergem após recuperação; não existe `Task exception was never retrieved` nem task/timer acumulado a cada notificação.

### CN5-04.03 — Preservar o predicate do lote e o encerramento

Manter `target=batch[-1].sequence`. ACK1 continua satisfazendo replay1 e não batch2. Se a aplicação do ACK no Core falhar, repetir somente essa aplicação antes de reenviar o lote. Em drain, cancelar timers, marcar `_closed` e encerrar somente observers que pertencem ao bridge; nunca cancelar operação de runtime ou apagar journal.

**Aceite:** controle saudável e P02 CN4 passam; reconnect/retry não duplica trabalho do agente; apenas eventos podem ser reapresentados conforme ACK.

## CN5-05 — Campanhas e entrega

### CN5-05.01 — Executar as mesmas entradas na fonte e no wheel

Executar runner duas vezes; CN4 executor, CN3 e históricos; suíte completa com motivo explícito para skips/deseleção. Construir wheel/sdist numa cópia, instalar fora da árvore e executar o mesmo runner com o pacote instalado. Não usar apenas testes que atribuem `_approvals` diretamente. Eles continuam úteis como unitários, mas não substituem `_on_remote_approval` → IPC → Server → Core.

Não remover contenção por falta de `/proc` no sandbox. As nove falhas de plataforma/fixtures da baseline devem ser classificadas individualmente; não são os seis FAIL de CN5. Nenhum provider real é necessário para os seis comportamentos confirmados.

**Aceite:** seis falhas corrigidas, controles preservados, commands/exit/XML/hashes registrados; nenhum PASS por xfail ou mock que evita a fronteira.

### CN5-05.02 — Atualizar escopo sem conclusão falsa

Entregar relatório por Q, tarefa, commit, teste e camada. Declarar exatamente o que continua externo: início remoto, confirmação/consulta persistente de intenção/recibo no Server, publicação remota do snapshot, callbacks opcionais e transferência de supervisão ao sair com unknown. Não converter IPC local em prova de UI remota. Não declarar G1 integral enquanto Q01 e decisões em voo estiverem incorretos.

Core segue `0.2.10.dev0` salvo mudança deliberada coordenada. Nenhuma correção aqui exige alterar a lista de runtimes, duplicar qualificação ou introduzir MCP no Connector. Consumidores podem continuar desenvolvendo em paralelo.

**Aceite:** pacote completo com testes e relatório; implementação, backend/SO, provider e integração real são estados separados.

## Regra final para o executor

Não escolher outro mecanismo em substituição aos especificados para obter testes verdes. Uma incompatibilidade real de API deve ser registrada com assinatura atual, trecho e proposta mínima de adaptação, preservando o comportamento descrito. Nunca “corrigir” o teste pulando o recebimento real, substituindo hash inválido no fixture, liberando barreira antes da assert ou inserindo autorização que o Server não concedeu.
