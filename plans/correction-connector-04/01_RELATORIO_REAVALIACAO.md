# Reavaliação do Nexus Connector — CN4

## Decisão

A versão **0.3.0.dev0**, ZIP no commit **e9307975a544c584fc64fe54c906ad512ae1b82b**, é diferente da anterior e corrigiu as reproduções específicas do CN3. **Continuar o desenvolvimento e a integração dos três projetos; não declarar o escopo completo de executor remoto aprovado.** Há cinco grupos de pendências reproduzidas, delimitados abaixo. Não é necessária reescrita do Core ou do Connector.

O catálogo continua pertencendo ao Core. A nova CLI e o novo IPC chamam a avaliação, mas alteram indevidamente o candidato antes de avaliá-lo. O caminho de aprovação ganhou um caller real; ao atravessar esse caller, aparecem incompatibilidade de decisão, escolha do Server errado e ordem inadequada entre autorização e aplicação. Não assumir que um helper testado prova a integração inteira.

## Fonte e método

Arquivo: `okto-nexus-connector-main(4).zip`; SHA-256 `a761b1c217aaff54467f33ddfd842aab2689e241b816107c21e6da35cf98cde4`. A versão está coerente em pyproject/export: 0.3.0.dev0. Dependência fixa: Core 0.2.10.dev0. A comparação com 2354392 encontrou 14 arquivos adicionados, um removido e 15 alterados. Os 139 arquivos originais foram preservados por hash.

Foram lidos plano CN3, evidência do executor, diferenças de código e rotas afetadas. Executaram-se as 14 sementes do executor, as 12 sementes originais, a suíte sem os dois testes de packaging e 14 testes adicionais. As novas verificações foram repetidas na fonte e no wheel instalado. Não se alterou o produto para produzir as provas.

O Core utilizado tem os cinco arquivos Python alterados/adicionados no C11 confrontados com seus hashes Git oficiais, aplicados sobre o ZIP 0.2.9 fornecido. O código executável corresponde ao contrato 0.2.10; o wheel foi construído nesta auditoria, não é o wheel do executor. Os dois módulos reais rfc8785 v0.1.4 foram verificados por Git blob; não houve stub de canonicalização. Proveniência e limitações estão em `evidencias/PROVENIENCIA.json`.

## Resultados

| Campanha | Resultado |
|---|---|
| CN3 entregue pelo executor | 14 PASS |
| CN3 original, primeiro ensaio | 11 PASS e 1 erro de fixture |
| CN3 original após adaptação causal | 12 PASS na fonte e no wheel |
| Verificações novas CN4 | 11 FAIL e 3 controles PASS |
| Repetição/instalação | Mesmo resultado 11/3; agregando CN3 original: 11 FAIL e 15 PASS |
| Suíte entregue, `-m not packaging` | 178 PASS, 9 FAIL, 4 SKIP, 2 deselected |
| Wheel/sdist/importação isolada | PASS |

A fixture original substituía o host por SimpleNamespace com `journal_if_open`; a API corrigida usa `ensure_history_journal`. Acrescentou-se somente um AsyncMock que devolve o mesmo journal real já criado. A expectativa de recuperação e o dado observado não mudaram. O diff está no pacote. Não é defeito do produto.

As nove falhas da suíte coincidem por nome com as da rodada anterior: seis fixtures de executáveis/permissão POSIX, dois probes dependentes de contenção indisponível neste sandbox e uma expectativa de discovery Pi. Os gates do produto não foram desligados. Não somar as repetições/subconjuntos ao total.

## Correções CN3 confirmadas

As sementes originais agora recusam revisões/gerações divergentes, recusam reconcile estrangeiro, devolvem a quota exata e recuperam histórico a frio. ACK duplicado é reconhecido quando consultado diretamente; namespaces curtos distintos não colidem na configuração. Hash adulterado continua recusado e uma operação legítima segue executando no peer. Esses comportamentos devem ser preservados.

## P01 — Aprovação ainda não atravessa a aplicação corretamente

**Âncoras:** `daemon/app.py:602–684`; `services/runtime_service.py:542–579`; contrato Core `NativeApprovalOperation` e `LocalRuntimeCore.decide_native_approval`.

### 1. Decisão do operador não é traduzida

A CLI/IPC aceita `approve` e `deny`; esses valores são passados diretamente ao manager, que os põe em NativeApprovalOperation. O Core aceita `accept`, `decline` ou `cancel`. Duas reproduções abriram sessão com Core/kernel/journal reais, grant de aprovação e pedido realmente observado no pump. Ambas produziram **CoreError(VALIDATION_ERROR, approval_decide)**, sem chegar à confirmação HTTP.

Um controle aplicou `accept` diretamente pela API pública do mesmo Core e produziu um recibo SUBMITTED e uma resposta no peer. Isso isola a falha no mapeamento da aplicação, não na capacidade nativa da biblioteca. Não basta substituir uma string: a ordem e o namespace a seguir precisam ser corrigidos no mesmo fluxo.

### 2. Aplicação nativa precede a decisão canônica

`_decide_approval` chama `runtimes.decide_native_approval` antes de `http.approval_decision`. A sentinela da fronteira do manager registrou **tentativa nativa → recusa do Server**. Esta prova demonstra a ordem do despacho, não um bypass completo do Core: com a implementação real atual, o erro de vocabulário anterior já bloqueia a aplicação. Corrigir o vocabulário isoladamente deixaria essa ordem perigosa exposta.

A decisão/aprovação do Server deve preceder qualquer aplicação permissiva ao runtime. A chamada que pede ao Server uma decisão não é o mesmo fato que receber uma decisão autorizada e correlacionada. O relato de autoridade não pode aparecer apenas em uma string de saída.

### 3. Pedido de B pode usar binding/credencial de A

Embora o dicionário de pedidos agora use `(server_id, request_id)`, o binding é procurado somente por binding_id. Com A e B usando `bind_a`, foi selecionado explicitamente o pedido de B e a chamada HTTP foi encaminhada à origem de A. O peer controlado recebeu A; deveria receber B. Não foram utilizados secrets reais, nem alegado vazamento de prompt/provider.

A resolução do binding, identidade, sessão e pedido deve usar a mesma chave completa. Uma chave textual curta não pode mudar a origem. O método também reutiliza a variável `key`: começa como chave do pedido e acaba como segredo do cofre; o `pop(key)` final não remove o pedido correto. Esse erro está no mesmo caminho e deve ser corrigido, sem contá-lo como outra campanha independente.

### Encaminhamento

Uma máquina de estados curta deve separar pedido pendente, reserva de decisão, decisão confirmada pelo Server, aplicação no Core e confirmação nativa/resultado incerto. Guardar uma chave imutável e o produtor em curso. Não liberar `deciding` para um segundo despacho concorrente. Não considerar qualquer VALIDATION_ERROR evidência de que a sessão não é gerenciada. Não restaurar um pedido como novo quando houve write possível. Contenção negativa tem semântica própria, mas não cria credencial/permissão de operador.

## P02 — O caller não informa o alvo do ACK e repete o lote novo

**Âncoras:** `daemon/app.py:93–129`; `transport/wss_client.py:495–530`.

`wait_event_ack(target=...)` foi implementado corretamente como condição monotônica. O EventBridge, porém, ainda o chama sem `target`. O default considera suficiente qualquer `acked_through > 0`.

Na reprodução com bridge, transporte, sender e codec reais: sequência 1 foi escrita e ACK1 validado; depois surgiu sequência 2, sem ACK2. O bridge releu o snapshot três vezes após ACK1 e aplicou ACK1 repetidamente. A fixture bloqueou o terceiro ciclo para não produzir um flood ilimitado. O registro observado foi `after_ack1_reads=3`, `applied=[1,1,1]`, três batches escritos.

Não se comprovou perda ou exclusão de dados: o journal não foi compactado além de 1. A falha é um laço de reenvio/progresso que usa uma confirmação antiga para concluir a espera do lote novo.

Corrigir o caller para esperar o watermark do lote (ou progresso incremental explicitamente modelado), sem tratar ACK1 como ACK2. Conservar o comportamento desejado: replay de seq1 pode usar ACK1 já conhecido; lote até seq2 não pode. Falha ao aplicar o ACK no Core conserva uma obrigação distinta; não reenviar a tarefa do agente para recuperar eventos.

## P03 — IDs longos ainda colidem na configuração MCP

**Âncoras:** `services/runtime_service.py:953–1002` e `_safe_segment:1099–1103`.

O namespace agora possui Server/executor/binding/sessão, mas cada segmento é sanitizado e truncado em 80 caracteres. Dois Server IDs válidos, com 85 caracteres e diferença apenas no final, viraram a mesma pasta. Os dois IDs foram aceitos pelo codec NXL real. Gerar a configuração de B sobrescreveu `.codex/config.toml` de A e o marker também foi reescrito. O controle com IDs curtos distintos passou.

A correção deve produzir um nome limitado sem perder a identidade completa — por exemplo, digest versionado de uma tupla canônica de ownership, e um marker validado antes de qualquer escrita. Não basta ampliar o truncamento: substituição de caracteres também pode colidir. Não executar migração que mova HOME/config de runtime ativo. Nenhum harness foi executado nesta prova; o efeito demonstrado é a sobrescrita do arquivo real pelo renderer do Core.

## P04 — O inventário publicado não preserva o candidato avaliado

**Âncoras:** `cli/commands/discover.py:46–65`; `daemon/app.py:571–586`; `services/discovery_service.py:268–291`; `services/connect_service.py:104–116`.

A disponibilidade está agora ligada à CLI e ao IPC. Entretanto, ambos reconstroem candidatos com `candidate(adapter_id, executable, explicit=True)`. Isso descarta versão observada e, em Pi, o launch_script e a identidade do pacote. O Core avalia corretamente o objeto errado que o consumidor lhe forneceu.

Duas reproduções criaram uma instalação Pi de laboratório real no layout suportado, com Node, CLI e package.json. O candidato completo veio de `candidate_pi_node_cli`. A enumeração foi injetada nas superfícies para não depender de PATH; todo o restante — conversão, disponibilidade, serialização e dispatch — era código do produto. CLI e IPC publicaram a referência de **Node sem CLI**, `version=null` e outra identidade de build. O resultado não designa a instalação apresentada pela descoberta. Não foi preciso qualificar ou iniciar Pi para demonstrar a perda de dados.

A revisão do snapshot também usa apenas `candidate_ref`, versão e estado. Alterar bytes da CLI, mantendo caminho, versão e estado técnico, alterou build_identity/fingerprint reais, mas preservou `executor_revision`. O controle sem alteração continuou estável. Portanto, a revisão não identifica integralmente sua própria evidência.

Conservar InstallationCandidate completo, sem re-selecionar implicitamente como trusted. Usar um único snapshot efetivo na exibição, seleção, resolução e persistência; não recalcular a revisão de um subconjunto de um candidato e tratá-la como revisão do inventário inteiro. Fingerprints, build, arquitetura, confiança e razões relevantes devem entrar na revisão canônica. Levar core_version/executor/formato/TTL na projeção apropriada dos aplicativos.

O resolvedor público continua disponível, mas `resolve_selection` ainda não tem caller produtivo. O IPC de snapshot não equivale à publicação remota no Nexus nem ao seletor completo. O Server continua dono da autorização; Core continua único dono dos mecanismos/qualificação.

## P05 — Rotação de lane não é aplicada no caminho de reload

**Âncoras:** `daemon/app.py:279–306`; `transport/wss_client.py:377–414`.

O teste anterior alterava a revisão diretamente na lane. Essa mudança fecha a reserva de operações corretamente. No fluxo real de reload, bindings já presentes são ignorados. A reprodução gravou revisão/credential_epoch 2 no StateStore, chamou `reload_state()` e observou lane ainda pronta em `(1,1)`.

Um segundo teste chamou `add_lane` para atualizar a lane existente em socket já disponível. O método mudou para pending, mas retornou sem instalar o novo ticket_provider nem agendar attach. O novo provider não foi chamado. Não foi alegada execução de uma operação indevida nesta campanha; as provas tratam da atualização e da retomada da conexão por agente.

Implementar diff de lanes existentes, invalidar a prova antiga antes de waits, instalar referências atuais e reanexar pela rota existente. No-op real não deve rotacionar epochs. O campo de expiração foi adicionado, mas os provedores da aplicação ainda descartam o prazo retornado; registrar essa pendência sem alegar renovação automática completa por existir o campo.

## Pendências declaradas que não contam como novos defeitos reproduzidos

A evidência do executor reconhece: abertura remota ainda UNSUPPORTED por contrato do Server pendente; intenção HTTP estável/publicação de recibos sem contrato final; callbacks opcionais Pi/resume/native approvals ainda não compostos; saída do daemon com unknown ainda sustentada por exit code 1, sem transferência de supervisão qualificada. Essas pendências impedem afirmar conclusão integral R3/G2/G3. Não significam que todo desenvolvimento deve parar.

A alegação global de fases DONE no status antigo precisa ser reconciliada com essas pendências e com as rotas agora verificadas. Implementação, prova com peer, qualificação de SO/provider e integração em dois hosts são fatos diferentes.

## Entrega e limites

CN4 contém correção explícita dos cinco grupos e preservação de CN3. O ZIP acompanha testes, runner e logs; os artefatos são **builds do snapshot não corrigido** para reprodução. Nenhum push, publicação, utilização de credencial real ou alteração do código-fonte original foi feito. Não foram executados providers reais, Nexus Server real, UI ou topologia em dois hosts. O teste de IPC utiliza o handler real após autenticação local, não constitui teste de ACL/rede.

Prioridade: fluxo de aprovação e namespace; depois caller do ACK, identidade dos diretórios e inventário/rotação. Os agentes do Server e Connector podem continuar em paralelo, sem duplicar adaptadores nem inventar listas ou políticas locais do Core.
