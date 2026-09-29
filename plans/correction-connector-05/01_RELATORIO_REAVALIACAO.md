# Reavaliação do Nexus Connector — 0.4.0.dev0

## Parecer

A versão é nova e corrige vários caminhos do CN4. Não há justificativa para reescrever o Connector, o Core ou o catálogo. Contudo, **o CN4 ainda não pode ser considerado encerrado**: seis falhas confirmadas, agrupadas em quatro áreas, continuam impedindo o aceite dos fluxos afetados.

A principal falha funcional foi encontrada ao executar o recebimento real de uma aprovação, uma entrada que os testes reconstruídos do executor não utilizam: o hash do pedido é redigido antes de sua aplicação. Também há seleção de Core por sessão sem namespace na mesma rota; a recusa do hash impede a execução nativa no cenário atual, mas não torna esse roteamento correto.

As três outras áreas são recuperação após cancelamento de decisão, rotação durante attach em voo e recuperação do publicador de eventos. São cenários concretos, não sugestões de estilo ou novos requisitos arquiteturais.

## 1. Entrada, dependências e método

- ZIP: `okto-nexus-connector-main(5).zip`.
- Commit informado no comentário do ZIP: `87b8fd2e3e403cb6a70a1ce265618e29c7a6b86c`.
- Connector: `0.4.0.dev0`; pin: `nexus-connector-core==0.2.10.dev0`.
- Baseline anterior: `e9307975a544c584fc64fe54c906ad512ae1b82b`, `0.3.0.dev0`.
- 147 arquivos originais preservados; hashes individuais antes/depois no pacote.
- Python de revisão: 3.13.5. Pytest 9.0.2, pytest-asyncio 1.3.0, websockets 16.0, httpx 0.28.1, jsonschema 4.26.0.

O ambiente não conseguiu baixar dependências por pip. O Core foi construído numa cópia usando o ZIP anterior 0.2.9 e os cinco módulos-fonte alterados oficialmente no C11/0.2.10. Cada um desses cinco módulos foi comparado byte a byte por hash de blob Git com o conteúdo oficial já fornecido pelo GitHub; todos coincidem. O restante da implementação executável não mudou nesse commit. `rfc8785` usa os dois módulos upstream v0.1.4 também conferidos por blob, sem serializador aproximado. A proveniência está em `evidencias/PROVENIENCIA.json`.

O wheel do Connector foi produzido em cópia e instalado fora da árvore. As provas adicionais foram repetidas contra esse import instalado. Não foi disponibilizado o mesmo wheel do executor, portanto não afirmo igualdade com seu hash binário. Não alterei fonte do produto para executar as reproduções; alterações de fixture estão separadas e documentadas.

## 2. Resultados

| Campanha | Resultado |
|---|---|
| CN4 reconstruído pelo executor | 18 PASS |
| CN4 original, após adaptação de fixture | 12 PASS / 2 FAIL |
| Seis verificações adicionais CN5 | 2 PASS / 4 FAIL |
| Conjunto CN4 original + CN5 no wheel instalado | 14 PASS / 6 FAIL |
| Suíte fornecida, sem dois testes de packaging | 196 PASS / 9 FAIL / 4 SKIP; 2 deselected |
| Build wheel/sdist e import isolado | PASS |

As seis falhas não significam seis novos problemas independentes: duas são approve/deny do mesmo problema de redaction. Os subconjuntos, o pacote instalado e repetições não podem ser somados como cobertura nova.

As nove falhas da suíte coincidem por node com a revisão anterior: seis fixtures que não tornam o arquivo executável no Linux, dois probes barrados por `proc_children` ausente e uma expectativa de discovery Pi incompatível com o resultado passivo do Core. Não as utilizei como prova dos quatro grupos abaixo. A comparação por nome está no JSON específico.

Não executei os dois testes originais de packaging com rede/clone irmão; construí e instalei offline separadamente. Não executei providers reais, Nexus Server real, UI, dois hosts ou serviços Windows/macOS. Isso continua não sendo G2.

### Adaptação das sementes originais

A fixture criada com `DaemonApp.__new__` passou a inicializar `_approval_history`, campo que o construtor real já possui. A fixture Pi passou a fornecer o mesmo candidato completo a `inventory_candidates`, o novo ponto de enumeração do produto. Não foram alterados o hash do pedido, a decisão, o Core, os bytes dos candidatos nem as expectativas. Após esses ajustes, 12 das 14 sementes passaram e as duas aprovações falharam genuinamente no Core.

O teste inicial de observação do Core estrangeiro ainda apontava a instrumentação ao host enquanto a sessão já carregava `runtime` diretamente. Corrigi a instrumentação para observar a referência realmente usada, delegando ao mesmo Core real. A campanha final e a repetição instalada usam esse teste corrigido; a primeira execução desse rascunho não é evidência de isolamento aprovado.

## 3. Correções confirmadas positivamente

As provas originais passaram para ordem de confirmação do Server antes da aplicação, roteamento HTTP de B, predicate de ACK do lote, diretórios com IDs longos, preservação do par Pi nas superfícies, revisão sensível a alteração de conteúdo e rotação quando a lane não possui attach anterior em voo. Os novos controles de stream saudável e rotação de lane READY também passaram.

Não encontrei reintrodução de MCP ou cópia de adaptadores nos arquivos alterados. O catálogo e as regras de disponibilidade continuam originados no Core. A alteração atual preserva o candidato integral em vez de reduzi-lo ao executável Node.

## 4. Q01 — Proposta corrompida e namespace incompleto na aprovação

**Prioridade P1 nos fluxos de aprovação.**

### Q01a: o recebimento destrói a prova de correlação

Âncoras: `daemon/app.py:492–520`, `redaction.py:14–52`, `services/runtime_service.py:542–579`.

`_on_remote_approval` guarda `proposal=redact_mapping(frame['proposal'])`. O redactor troca qualquer sequência hexadecimal de 64 caracteres por `[redacted]`. Isso inclui `request_hash`, exigido pelo Core como hash hexadecimal de 64 caracteres e utilizado para comparar a proposta àquela observada no runtime.

As sementes P01 abrem Core/kernel/SQLiteJournal reais, fazem o pump observar um pedido válido, passam o pedido pelo recebimento da aplicação e confirmam a decisão num peer HTTP. Para approve e deny, a sequência foi:

```
Proposta íntegra observada pelo Core
→ _on_remote_approval redige request_hash
→ Server confirma decisão
→ manager traduz corretamente accept/decline
→ CoreError(VALIDATION_ERROR, approval_decide)
→ zero respostas nativas
```

O controle de `accept` direto na API do mesmo Core passou. A tradução adicionada nesta versão está correta; o dado de entrada já chega corrompido.

**Por que os 18 testes do executor passam:** eles constroem `_pending(...)` e atribuem esse registro diretamente em `_approvals`. São provas úteis do estágio posterior, mas não percorrem `_on_remote_approval`, onde ocorre a modificação. Não é legítimo substituir a semente que recebe o frame por esse caminho para fechar o achado.

**Correção:** conservar proposta operacional imutável e projeção de apresentação separadas. Redigir apenas esta última. Não desligar o redactor global nem recalcular o hash recebido.

### Q01b: o HTTP usa B, mas o Core selecionado é A

Âncoras: `daemon/app.py:823–843`, `services/runtime_service.py:155–179,542–579`.

O binding HTTP agora é buscado no Server correto. Na etapa seguinte, o app usa `session_id in self.runtimes.session_ids()` e passa apenas a string ao manager. Este utiliza `session(session_id)`. Se somente A possui aquela string, A é selecionado mesmo que o pedido seja de B.

A reprodução contém um Core real em A e um binding de B, com o mesmo texto de binding/sessão. A confirmação HTTP foi enviada a `https://b.example`. Um observador que delega integralmente ao Core registrou o contexto final `srv_a/ag_a/session_a`.

**Limite da conclusão:** o Core recusou o hash já redigido e não houve efeito nativo não autorizado. A prova é do roteamento/contexto errado na fronteira do Core. Corrigir Q01a sem Q01b remove a recusa acidental que hoje impede o efeito nesse cenário.

**Correção:** ApprovalKey/ApprovalTarget completos, preservar executor do canal e usar SessionKey na API interna do manager. Uma ausência no namespace B não pode ser reinterpretada como sessão A ou pedido administrativo. Revalidar binding/agente/workspace e contexto antes e depois da confirmação.

## 5. Q02 — Cancelar o cliente cancela a decisão e deixa fase fictícia em andamento

**Prioridade P2.** Âncora: `daemon/app.py:790–859`.

O app adicionou fases e tombstones, mas continua executando o POST e a aplicação nativa na própria coroutine de `_decide_approval`. Não há task produtora pertencente a uma tentativa independente.

No teste, um pedido administrativo válido chega ao peer de confirmação e permanece aguardando a resposta. O waiter é cancelado. Observou-se:

| Fato | Resultado |
|---|---|
| Cancelamento chegou ao backend controlado | Sim |
| Registro depois do cancelamento | `server_pending` |
| Segunda solicitação idêntica | `OPERATION_CONFLICT` |
| Chamadas HTTP | Uma, já encerrada por cancelamento |

O registro afirma que existe uma operação em andamento, mas nenhum produtor continua para terminá-la. Não demonstrei perda real de um commit remoto; o peer controla a fronteira de resposta. O que foi demonstrado é cancelamento do produtor e ausência de finalização/retomada local.

O mesmo método captura apenas `ConnectorError` ao chamar o Core. O `CoreError` comprovado em Q01 deixa `native_pending`, em vez de uma recusa tipada e consultável. Essa é outra saída da mesma máquina de estados incompleta.

**Correção:** DecisionAttempt possui o produtor; IPC aguarda por shield com referência forte. Sucesso, recusa comprovada, CoreError e resultado incerto têm finalização própria. Confirmação canônica já recebida não deve gerar outro POST. Estado unknown precisa ser consultável e não permitir replay cego. Recuperação pós-crash depende do contrato externo já declarado e não foi comprovada aqui.

## 6. Q03 — Rotação durante attach não agenda o substituto

**Prioridade P2.** Âncoras: `transport/wss_client.py:382–462,1151–1187`.

`add_lane()` chama `lane.attach_task.cancel()` e, na mesma passagem, `_schedule_lane_attach()`. Este encontra a task antiga ainda não concluída e retorna. Não há callback de conclusão que inicie a tentativa da nova geração.

O teste mantém o provider antigo aguardando, altera credential_epoch e authorization_revision, aguarda o cancelamento antigo terminar e procura a nova tentativa:

```
novo provider consultado: False
lane.state: pending
lane.attach_task: a mesma task antiga, cancelada
frames binding.attach novos: 0
```

O controle com rotação a partir de READY, sem attach em voo, passa. Isso localiza a falha na transição concorrente, não na capacidade genérica de obter tickets.

**Correção:** capturar snapshot/token por tentativa e reprogramar o generation atual quando a antiga finalizar. Não rotular resultado tardio do provider antigo com dados mutáveis da lane nova. Remoção impede reattach; múltiplas rotações se reduzem à geração mais recente sem tempestade de tasks.

## 7. Q04 — Um erro transitório encerra o publicador sem retomada na reconexão

**Prioridade P2.** Âncoras: `daemon/app.py:103–143`.

O tratamento de falha de aplicação do ACK no Core foi implementado. Mas `_journal_page()` e outras portas ainda podem terminar `_flush_loop`. `transport_online_again()` apenas sinaliza `_wakeups[key]`, sem verificar se a task já morreu.

No teste, um stream possui a sequência 1 e a primeira leitura falha temporariamente. A leitura seguinte já seria bem-sucedida. Após observar o fim da task e chamar somente a reconexão:

```
leituras: [0]
batches enviados: []
ACKs aplicados: []
task finalizada: True
```

Não há perda comprovada do journal. O registro está disponível, mas não é republicado até algum outro caminho criar uma task. O controle sem erro envia a sequência 1 e aplica ACK1 normalmente.

**Correção:** helper único para assegurar a task do stream, utilizado por publish/ACK/reconnect. Falhas transitórias deixam retry coalescido e bounded; drain cancela timers sem reativar o bridge. Preservar alvo por lote e replay a partir de ACK válido, nunca a partir do último envio.

## 8. Pendências anteriores — não contadas como novas falhas

O executor declara ainda: runtime.open remoto indisponível; publicação e consulta persistente de intenções/recibos dependentes do Server; publicação remota do snapshot; alguns callbacks opcionais; política completa de renovação de tickets; e transferência/recuperação ao sair com ownership incerto.

A revisão não executou esses produtos externos. Não os marquei como bugs novos reproduzidos, mas tampouco como concluídos por existirem helpers locais. Em particular, catálogo/avaliação locais não comprovam a UI remota e código de saída 1 não conserva um loop que terminou.

Os trabalhos dos três repositórios podem continuar em paralelo. Não ratificar encerramento integral do CN4 ou G1 no fluxo de aprovação enquanto Q01/Q02 permanecerem. Qualificação de provider/SO e G2 continuam requerendo suas campanhas específicas.

## 9. Entrega

`02_PLANO_CORRECAO_CN5.md` fixa DTOs, assinaturas, estados, ordem de efeitos, tratamento de exceção/cancelamento e testes. São 20 tarefas, sem reestruturação do Core. O pacote contém as sementes originais adaptadas, seis verificações adicionais, runner e evidências. Nenhuma correção foi aplicada ao produto.
