# Reavaliação do Nexus Connector — CN3

**Fonte:** ZIP `okto-nexus-connector-main (1).zip`, comentário Git `235439242e4de8edc7ca8940b80a331275be9fb6`.  
**Connector:** `0.2.0.dev0`. **Core fixado:** `0.2.10.dev0`, fonte oficial `1560d314ed2b478515dcbbe533436d7d0b027b09`.  
**Comparação:** CN2, baseline `28d147625c2a47417ad70f72d3b453cd8bd2e984`.  
**Data da análise e campanhas locais:** 29/09/2026.  
**Integridade:** 127 arquivos originais conferidos por SHA-256; nenhum alterado.

## 1. Decisão

A atualização resolveu os cenários específicos que motivaram o CN2, depois das adaptações de fixture justificadas. Há progresso real em namespace de operações, controles separados, leitura finita do journal, rejeição de ACK estrangeiro/futuro, stop e geração da configuração MCP HTTP. Não recomendo reescrever o Connector nem alterar os adaptadores do Core.

Restam **seis grupos de defeitos confirmados**, demonstrados por **nove casos que falharam**. Três controles positivos passaram. Os resultados foram reproduzidos na fonte e no wheel instalado, novamente na repetição. Os bloqueadores mais relevantes são o descarte de gerações/revisões autorizadas, a exposição de recibos de outro Server por reconcile e a resposta vazia depois de reiniciar o host sem abrir seu journal.

A integração pode continuar em paralelo; a aplicação não deve ser declarada um executor remoto integralmente governado enquanto essas fronteiras estiverem incorretas. Não atribuir ao Core uma falha produzida por contexto ou namespace incorretos fornecidos pelo host.

Há ainda tarefas funcionais anteriores sem wiring completo. O catálogo continua no Core; os novos helpers de availability/resolução não são chamados pelo fluxo principal do Connector. Remote open, composição da bridge Pi/resume/aprovações e aspectos de tickets/idempotência/saída do daemon continuam pendentes ou precisam de prova do caminho completo. Não são contados como novas falhas executadas.

## 2. Método, ambiente e limites

Extraí o ZIP em diretório próprio e comparei o código com o baseline CN2. Conservei hashes antes e depois. Os testes adicionais são externos à árvore original. Os builds foram realizados em cópia da fonte com `setuptools.build_meta`, sem alterar o produto.

O ambiente usou Python 3.13.5, pytest 9.0.2, pytest-asyncio 1.3.0, jsonschema 4.26.0, httpx 0.28.1 e websockets 16.0. As dependências exatas, caminhos de importação e hashes estão em `evidencias/PROVENIENCIA.json` e `installed_imports.log`.

O wheel do Core produzido pelo executor não estava anexado. O código foi reconstruído a partir do ZIP 0.2.9 e dos arquivos efetivamente alterados no commit oficial 0.2.10; os Git blob hashes desses arquivos foram verificados. O wheel local não é apresentado como o mesmo artefato binário do executor. `rfc8785` utiliza os dois módulos upstream reais da versão 0.1.4, verificados por hash; não foi utilizado serializador aproximado. O ambiente não possui metadata de distribuição para esses módulos e não disponibilizou downloads pelo pip. Isso está separado de qualquer diagnóstico funcional.

Não executei providers reais, operações num Nexus Server real, integração em dois hosts, autostart/reboot Windows/macOS ou os dois testes originais de empacotamento dependentes de rede/clone irmão. Executei construção e importação instaladas offline. Peers usados nos testes não representam uma qualificação desses ambientes.

Os cenários de autorização usam o codec do Core, receiver, dispatcher, manager, kernel e SQLiteJournal reais, com um runtime nativo de laboratório. As falhas de armazenamento e rede são injetadas somente nas fronteiras indicadas. Nenhuma credencial real foi utilizada. A configuração MCP foi renderizada pelo código real, sem abrir uma sessão MCP ou executar um harness.

## 3. Campanhas e causalidade das fixtures

| Campanha | Resultado | Evidência |
|---|---|---|
| Suíte fornecida, exceto `tests/e2e/test_packaging.py` | 164 PASS / 9 FAIL / 4 SKIP | `suite.xml` |
| 20 testes CN2 entregues pelo executor | 20 PASS | `cn2_executor.xml` |
| 20 sementes originais CN2, adaptadas à API atual | 20 PASS | `cn2_adapted.xml` |
| Mesmas sementes com Connector instalado | 20 PASS | `cn2_installed.xml` |
| 12 novos casos contra a fonte | 9 FAIL / 3 PASS | `cn3_final.xml` |
| Mesmos casos contra o wheel | 9 FAIL / 3 PASS | `cn3_installed.xml` |
| Repetição com o wheel | 9 FAIL / 3 PASS | `cn3_repeat.xml` |
| Wheel/sdist e importação `python -I` | PASS | `connector_build.log`, `installed_imports.log` |

Subconjuntos, versões de fixtures e repetições não são cobertura aditiva. As contagens não se somam à suíte completa.

As nove falhas da suíte fornecida coincidem por nome com a campanha anterior: seis ligadas a executáveis de laboratório sem permissão POSIX/consequências, dois probes recusados por ausência de `proc_children` neste sandbox, e uma expectativa de discovery Pi que exige lista vazia fora de Windows apesar de a descoberta passiva encontrar a fixture. Esses resultados não foram usados como nove bugs novos. Não removi o gate de contenção.

A semente CN2 bruta inicialmente produziu erros de fixture por mudanças de API e observação. O diff entregue documenta: `_reconcile_state` substitui a antiga flag; handlers recebem DTO; `_journal_page` substitui o reader antigo; o fake de host fornece o port de journal; ACK futuro é testado na fronteira real que agora valida; URL MCP é observada no arquivo gerado, não somente no argv; a lane online representa socket existente; TLS é recusado na construção; e o interrupt inclui o `expected_turn_id` exigido pelo schema efetivo. A condição de quatro submits bloqueados foi preservada. Não alterei as expectativas para aceitar os defeitos antigos.

A suspeita inicial de que `intent_hash` não seria verificado foi descartada: o codec real rejeita o payload adulterado. Esse comportamento é um controle aprovado, não um achado. A ausência de uma segunda checagem no dispatcher não é, por si só, defeito.

## 4. CN3-01 — Gerações/revisões deixam de acompanhar a operação até o Core

**Prioridade P1. Correlação:** N01, CN2-01.01/02/03.  
**Fonte:** `transport/wss_client.py:98–123,790–854`; `services/runtime_service.py:624–780`.

`ValidatedOperation` passou a conservar o namespace e parte das revisões, mas não contém `session_owner_generation`. `_resolve_remote_session` agora confere Server/executor/binding/agente/workspace corretamente. Depois dessa seleção, os métodos remotos descartam as diferenças do envelope e obtêm `_authorized_context(session, action)`, construído com as gerações e revisões locais antigas.

Isso impede o Core de rejeitar um envelope incompatível: ele recebe um contexto local que ainda é válido, e não o contexto cuja incompatibilidade precisaria ser diagnosticada.

### Reproduções

A sessão real do Core começa com connection generation 3, owner generation 1 e configuration revision 1. Os frames são válidos pelo schema e têm hashes corretos. Foram exercitados separadamente:

| Variação | Fato incompatível | Resultado observado |
|---|---|---|
| `configuration_changed` | Frame informa configuration revision 99 | `send_turn` executado sob configuração local 1 |
| `owner_changed` | Frame informa owner generation 99 | Campo não chega ao DTO; `send_turn` executado sob owner 1 |
| `generation_changed` | Transporte negociado/frame na geração 5; Core/sessão ainda na 3 | O manager fornece geração 3 e `send_turn` é executado |

O controle com envelope coerente produz exatamente um envio. O controle com hash adulterado produz zero envios. Não há uma exploração remota anônima demonstrada: são peers autenticados de laboratório usados para verificar a fronteira do host.

Outra parametrização da mesma lacuna usa o scheduler real: quatro operações ocupam os slots; uma quinta espera. A revisão de autorização da lane muda de 1 para 2 enquanto ela está enfileirada. Ao liberar os slots, a operação reservada com revisão 1 chega ao handler. `lane_reservation_valid` compara ticket epoch/estado/agente/conexão, mas não a revisão de autorização capturada. Este teste prova chegada ao handler, não um segundo efeito de provider real.

### Correção necessária

Conservar contexto aprovado e expectativas do envelope como objetos distintos. Comparar todos os campos pertinentes antes de chamar o Core; não instalar autoridade a partir do frame. Uma nova geração legítima precisa completar a renovação/reconciliação pública do Core antes de aceitar trabalho nela. Capturar token/revisões da lane no ingresso e revalidá-los após a espera. Atualizações devem trocar um snapshot coerente, não campos parciais que deixam ações e revisões divergirem.

Recibo conhecido pode continuar consultável sem reexecutar. Operação nova incompatível deve ter erro tipado e zero efeito. Não é necessário duplicar o algoritmo de hash que o codec já verifica.

## 5. CN3-02 — Reconcile aceita namespace estrangeiro ao canal e expõe recibo

**Prioridade P1. Correlação:** CN2-00.02/01.04 e N01/N04.  
**Fonte:** `transport/wss_client.py:644–654`; `daemon/app.py:414–433`; `services/runtime_service.py:786–835`.

A validação de escopo está nas operações produtivas e no event ACK, mas não protege `reconcile.request`. O frame é encaminhado diretamente ao callback e os campos declarados nele são usados para escolher o journal namespace.

### Reprodução

O journal real contém `op_open` de Server A. Configurei uma conexão/receiver de laboratório correspondente a Server B. O frame é válido pelo codec, mas pede o recibo no namespace A. A resposta enfileirada no transporte de B contém `reconcile.report`, namespace A e o recibo `op_open`/`SUBMITTED`.

A divulgação demonstrada é de identidade/estágio da operação durável. Não houve vazamento de prompts ou segredos demonstrado; tampouco foi utilizado um Server real. O controle de consulta do próprio Server A retorna corretamente seu recibo.

### Correção necessária

A origem autenticada deve acompanhar também frames não produtivos. Server/executor do frame são verificações de igualdade com o canal, não autoridade para escolher outro namespace. Validar antes do handler e da leitura de armazenamento. Aplicar a mesma disciplina a approvals, detach, negociação e controles com escopo, com decisões específicas por tipo; não exigir artificialmente READY para mensagens necessárias ao próprio handshake/recovery.

Respostas e erros devem voltar ao canal correto sem incorporar dados estrangeiros. Não usar `_default_namespace` ou primeiro binding para resolver uma requisição de rede ambígua.

## 6. CN3-03 — A quota de bytes não volta a zero após operações concluídas

**Prioridade P2. Correlação:** CN2-02.02.  
**Fonte:** `transport/wss_client.py:264–296,590–609,925–926,985–1010`.

A admissão contabiliza a serialização do frame original. A liberação contabiliza outro dicionário, reconstruído por `_as_error_frame_payload`, que não conserva os mesmos campos e usa `workspace_binding_id="wb"`. Os comprimentos podem diferir, mesmo sem fila ou operação pendente.

### Reprodução

Enviei 12 operações válidas em sequência, cada uma concluída antes da próxima, com um workspace binding ID permitido de maior comprimento. Todas chegaram ao handler. No final:

```text
waiting_items = 0
operações em voo = 0
waiting_bytes = 1824
```

Não foi um teste de falta de memória. O efeito confirmado é consumo residual do contador: carga normal repetida pode acabar recusada por capacidade mesmo com fila vazia. Também não seria correto liberar em excesso e mascarar o erro com `max(0)`.

### Correção necessária

Uma reserva deve possuir seu custo exato e token de finalização única. Cobrar e liberar a mesma unidade, sem resserializar um envelope transformado. Tratar sucesso, recusa após fila, exceção, cancelamento e falha de criação da task. Preservar a separação de controle/produtivo e os limites antes de criar tarefas, que já funcionam no cenário CN2.

## 7. CN3-04 — Host recém-iniciado retorna histórico vazio apesar de recibo persistido

**Prioridade P1 para recuperação prometida. Correlação:** N04, CN2-04.01/02.  
**Fonte:** `services/core_host.py:journal_if_open/ensure_journal`; `services/runtime_service.py:786–835`; `daemon/app.py:167–188,468–479`.

A consulta não exige mais binário quando o journal está aberto. Porém, utiliza apenas `journal_if_open()`. O startup não abre esse journal antes da reconciliação inicial. Num processo/host novo sem runtime composto, o mesmo caminho devolve vazio, sem verificar os dados existentes em disco.

### Reprodução

Criei o primeiro host, abri seu journal real, persisti `op_durable`, fechei a conexão e descartei o host. Instanciei outro `CoreRuntimeHost` e `RuntimeManager` no mesmo diretório/estado, sem abrir runtime. A consulta pública com o namespace exato e ID da operação retornou `receipts=[]`.

É diferente da semente CN2 aprovada: ela removia o executável, mas conservava o journal aberto em memória. Esta reprodução atravessa o reinício do componente. Não executei processo daemon real nem demonstrei uma segunda tarefa nativa; a falha é a resposta falsa de ausência de histórico pela recuperação que o daemon utiliza.

### Correção necessária

Abrir/recuperar assíncrona e explicitamente a persistência técnica pelo port público antes de responder. Não construir provider factory nem revalidar executável para ler história. Falha ao abrir/ler não vira sucesso vazio. Completar também o bootstrap de claims/cursors paginados quando ainda não existem IDs de sessões em memória, para que reconnect sem novo evento recupere as obrigações antigas.

Autoridade do namespace deve estar validada antes da consulta. A abertura do store deve ser single-flight e não bloquear controles ou abandonar produtor por cancelamento do waiter.

## 8. CN3-05 — ACK duplicado válido deixa de satisfazer uma espera de replay

**Prioridade P2. Correlação:** CN2-03.02/03/04.  
**Fonte:** `transport/wss_client.py:419–456,674–721`.

`send_events()` limpa o Event usado pelo waiter, mesmo quando reenvia um registro cujo ACK já foi validado. `_apply_event_ack()` descarta um ACK igual ao watermark atual antes de notificar a espera. O valor durável confirmado pelo Server continua em memória, mas `wait_event_ack()` não o consulta antes de esperar pelo sinal.

### Reprodução

O sender real escreve a sequência 1 e aceita ACK 1. A primeira espera retorna 1. A sequência 1 é reapresentada, como pode ocorrer quando o ACK não foi aplicado ao journal do Core ou um publicador retoma o lote. O Server de laboratório repete ACK 1. O transporte conserva `acked_through=1`, mas a nova espera retorna `None` por timeout.

Não demonstrei compactação indevida nem perda no journal. É um erro de progresso sob repetição válida; um stream que não produz mais eventos pode ficar esperando uma confirmação que já possui.

### Correção necessária

Esperar por uma condição monotônica vinculada ao watermark-alvo e à conexão, não somente por uma borda de Event. ACK válido repetido é idempotente e deve permitir concluir/aplicar a prova já conhecida. Para um lote novo até sequência 2, um ACK antigo 1 não basta: a solução não pode transformar qualquer sinal em confirmação suficiente.

Separar recebimento do ACK do Server de aplicação no Core. Se a segunda etapa falha, conservar e retentar a obrigação sem exigir um ACK novo ou produzir nova tarefa do agente. Preservar rejeições CN2 para namespace estrangeiro, stream desconhecido e watermark futuro.

## 9. CN3-06 — Configuração MCP efêmera colide entre namespaces

**Prioridade P2. Correlação:** CN2-01.04/06.01, CN1-06.02.  
**Fonte:** `services/runtime_service.py:908–935`.

O caminho gerado é `root/runtime/mcp/<session_id>`. Não inclui Server, executor ou binding. Os modelos permitem IDs textuais iguais em namespaces distintos; essa separação já é usada pelo cache e pelas operações corrigidas.

### Reprodução

No mesmo host, gerei templates HTTP reais do Core para duas bindings, uma de A e outra de B, com o mesmo `session_id`. O primeiro aponta para `https://a.example/mcp`, o segundo para `https://b.example/mcp`.

Os dois receberam o mesmo diretório HOME. Depois de gerar o segundo, o arquivo `.codex/config.toml` do primeiro foi alterado pelo segundo. Não executei harness, não fiz chamada MCP e não transmiti segredo real. Foi demonstrada a sobrescrita de configuração entre namespaces, não um provider efetivamente conectando ao destino errado.

### Correção necessária

Delimitar ownership do diretório pelo scope completo e pela tentativa/sessão necessária, com uma chave opaca local validada. Gravar atomicamente, manter referências de credencial por lançamento, não remover configuração que outro recurso possui. Uma ref de instalação ou ID de agente não substitui a identidade da sessão proprietária.

A mudança deve preservar MCP HTTP direto. Não criar processo proxy ou MCP stdio para isolar configurações. A compatibilidade da mudança de HOME com autenticação do provider precisa de teste no harness qualificado; não presumi que qualquer provider dependa exclusivamente de arquivo de credencial no HOME.

## 10. Pendências funcionais do CN2: helpers não equivalem a fluxo integrado

### G01 — Availability e resolução adicionadas, mas não utilizadas pelo aplicativo

`services/discovery_service.py` publica wrappers `evaluate_availability`, `availability_snapshot` e `resolve_selection`. A inspeção das chamadas em `src/` não encontrou consumidores do snapshot ou do resolvedor: o único encadeamento é snapshot → evaluator. A criação de binding ainda calcula `inventory_revision` a partir de `state.schema_version`, enquanto o helper novo calcula outra revisão. Não há publicação do snapshot pelo daemon/IPC/transporte ou seleção end-to-end que utilize esses wrappers.

Isso não é uma nova lista duplicada de runtimes. O catálogo permanece no Core e os wrappers reaproveitam suas APIs corretamente. A pendência é conectar o contrato ao fluxo real. A declaração de evidência “N09 wired/publishes” é mais ampla do que o código entrega. Ver `evidencias/WIRING.md`.

Concluir discovery → avaliação no executor → snapshot versionado com executor/Core/revisão/TTL → projeção aprovada ao Server/UI → seleção → resolução no mesmo inventário. A revisão deve refletir o conteúdo de evidência relevante, não só schema, nome de versão ou estado agregado. Testes com helpers isolados não qualificam a UI do Server.

### G02 — Escopo anunciado e capacidades ainda indisponíveis

O executor declara honestamente remote `runtime.open` como indisponível e callbacks Pi/resume como pendentes. Preservar esse diagnóstico; não anunciar conclusão do produto distribuído por passar testes de submit/close sobre sessão previamente iniciada.

O método público de aprovação no manager agora usa a assinatura e DTO corretos, mas não é chamado pelo fluxo de decisões da aplicação. `_decide_approval` continua removendo o pedido antes da validação/rede e envia a decisão HTTP; não há rota final que aplique a decisão autorizada no Core. Aprovações em memória também continuam indexadas pelo request ID isolado. É uma pendência de ligação e escopo, não outro TypeError alegado.

`CoreRuntimeHost.build` não injeta callbacks de Pi native action/resume nem habilita aprovações nativas no artefato examinado. Concluir por ports públicos ou manter essas capabilities indisponíveis de forma explícita até o handoff com Server. A falta do Server real não impede testes de contrato da aplicação com um peer, mas sua execução não equivale a qualificação real.

O bootstrap passou a escolher a credencial do binding correto e live attach de lane passou. Entretanto, expiração retornada pelo ticket é descartada em `_expires`, e rotação/revisão de lanes existentes não tem o ciclo completo. Isso deve ser tratado no mesmo snapshot de autoridade de CN3-01, com atualização revogável e single-flight.

A mutação HTTP local ainda não possui o ciclo completo de intenção estável anterior ao primeiro POST, consulta e publicação de receipt como operação distinta. `_publish_operation_receipt` chama a operação HTTP com payload de comprimento de texto, não um contrato final de receipt; o acordo com Server continua necessário. Um flag correto de timeout não implementa sozinho idempotência end-to-end.

O daemon encerra IPC/transportes e retorna 1 quando existem resultados unknown. Isso reporta a pendência, mas não comprova transferência de ownership do supervisor Python a um mecanismo de SO. Não reproduzi um órfão real; o gate de saída com recursos incertos continua exigindo política e evidência próprias.

## 11. Recomendações e liberação

Priorizar os bloqueios CN3-01/02 e recuperação CN3-04. Quota, ACK repetido e diretórios efêmeros são correções localizadas que podem avançar em paralelo. O plano CN3 acompanha critérios explícitos e referências ao CN2; não substitui o backlog original por um escopo menor silencioso.

G0/integração experimental continua possível. Não ratificaria G1 para o conjunto anunciado enquanto uma conexão consegue consultar outro namespace ou uma nova geração é executada como a antiga. G2 exige Server real, o mesmo wheel do Core local/remoto, inicialização solicitada remotamente, ferramentas HTTP diretas, decisões, eventos, reconexão e parada. G3 exige a matriz declarada completa. Qualificação do Core não se transfere automaticamente ao host aplicativo.

As seis falhas foram reproduzidas sem alterar fonte original. Os problemas estão no consumo do Core e no lifecycle do Connector. Nenhuma correção de adaptador, novo protocolo MCP, usuário Nexus ou duplicação de regras de disponibilidade é recomendada. Não houve push, publicação ou uso de credenciais reais.
