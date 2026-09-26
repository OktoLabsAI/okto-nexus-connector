# Plano 2 — Criação do Nexus Connector

**Destino:** Codex em novo projeto apartado do Nexus Server.
**Nome de repositório proposto:** `okto-nexus-connector`; distribuição/CLI `okto-nexus-connector`; import `okto_nexus_connector`.
**Data:** 25 de setembro de 2026. **Revisão:** 3, três projetos e MCP exclusivamente HTTP direto.
**Estado:** especificação de implementação; não há aplicativo implementado por esta elaboração.
**Dependência:** distribuição independente `nexus-connector-core` do Plano 3, não pasta interna deste projeto.

## 1. Mandato ao Codex

> **Correção normativa r3:** MCP existe somente como HTTP direto no Nexus Server. Remover MCP stdio do Nexus; não implementar MCP stdio/HTTP, fachada ou proxy no Connector/Core. Configurar o cliente do harness não significa intermediar chamadas. Stdio nativo de runtime permanece. Esta revisão substitui r2; ver A.13.1 e J31–J34.

Crie a aplicação leve que vive na máquina dos harnesses. Ela importa credenciais de agentes já existentes no Nexus, descobre instalações locais, vincula identidade/harness/projeto e administra runtimes via Core. A comunicação remota é WSS de saída com autenticação por escopo de agente e reconciliação durável.

Não implemente login de usuário Nexus, cadastro canônico de agentes, outra inbox ou cópia dos adapters. O daemon é aplicação persistente; a CLI é interface de gestão. Não há servidor, fachada ou proxy MCP no Connector, de qualquer transporte. Clientes MCP HTTP falam diretamente com o Server. O usuário que executa um serviço no SO não se torna entidade do domínio Nexus.

```text
Leia este plano e os Anexos A/B. Crie o projeto local com fronteiras e testes.
Não publique pacote nem crie repo remoto sem autorização específica.
Consuma o wheel/bundle imutável de nexus-connector-core; não dependa do Server
como biblioteca nem mantenha cópia dos protocolos Codex/Pi/Claude.

Implemente C00 e avance por fases com dependências satisfeitas. Integração com
um peer fake é teste de contrato, não prova de conexão real com Nexus.
Preserve configurações existentes dos harnesses. Não varra credenciais,
não escolha identidade por nome/JSON, não conceda escopo por autodiscovery.
Registre evidência por tarefa/teste e bloqueios externos reais; entregue
código e execução verificável, não somente outro plano.
```

Criar `plans/implementation/{IMPLEMENTATION_STATUS.md,BACKLOG.json,DECISIONS.md,TEST_MATRIX.md,evidence/}`. Estados de tarefa/teste seguem o Anexo A. Este plano substitui o antigo projeto que continha dois packages, incluindo o runtime: agora o Core é terceiro repo/publicação independente.

## 2. Experiência completa

### 2.1. Primeiro uso

Instalar uma distribuição que traga o Core como dependência. No Nexus, abrir agente existente e copiar o comando `connect`. Executá-lo no host remoto, importar a mesma chave usada pelo MCP por entrada protegida ou entrada MCP explicitamente selecionada, confirmar identidade e escolher harness dentre os locais.

Resolver o projeto pela pasta atual. Mostrar confirmação agregada do vínculo: nome/ID do agente, endereço/fingerprint da instalação Nexus, harness/versão, pasta, credencial de provider por referência e limites. Gerar binding/endpoint/perfil e configuração técnica. Persistir a chave Nexus no cofre e não no JSON do projeto. Iniciar daemon automaticamente quando necessário, com lock e readiness. Iniciar runtime somente se solicitado.

A CLI não precisa listar os agentes de todo o Server, mesmo que existam centenas de milhares. Lista apenas identidades importadas localmente e objetos que a identidade autenticada pode ver. Se a chave representa outro agente, abortar; não “corrigir” o cadastro alterando a identidade.

### 2.2. Uso recorrente

`runtime start <alias>` reutiliza binding/sessão compatível. Sem chave nova, endpoint/profile IDs, JSON ou autorização manual a cada hora. Tickets operacionais são internos e renovados com a credencial canônica vigente. Rotação/revogação dessa credencial requer sua substituição real, com mensagem clara e sem loop agressivo.

`status` separa daemon, transporte, binding, runtime e provider. `logs --follow` e TUI podem ser fechados sem encerrar o trabalho. O painel remoto pode iniciar/interromper/parar a mesma sessão usando as capacidades autorizadas.

### 2.3. Modos e limites

O modo gerenciado abre processo headless controlado pelo adapter, não necessariamente janela TUI nativa. `tools-only` é acesso direto do harness ao MCP HTTP do Server e não exige Connector; `attach` usa somente substrato qualificado. Ajudar a configurar o cliente HTTP não adota a conversa existente nem instala serviço MCP.

O Connector não sincroniza repositórios nem instala/atualiza providers silenciosamente. Ausência de binário/login vira instrução local guiada. Não mandar segredos de provider para o Server.

## 3. Componentes e organização

```text
okto-nexus-connector/
  pyproject.toml
  src/okto_nexus_connector/
    cli/                # comandos humanos/automação; sem lógica nativa duplicada
    daemon/             # composition root, singleton, lifecycle
    ipc/                # controle local autenticado e handles scope-bound
    identity/           # refs de chaves canônicas e aliases por Server/agente
    transport/          # HTTPS + cliente WSS + reconnect
    services/           # fluxos de connect/bind/start; usa Core
    storage/            # bindings/config local; Core fornece journal técnico
    harness_config/     # configuração declarativa do cliente MCP HTTP direto; sem runtime MCP
    platform/           # integração com serviços de SO; não adapters nativos
  tests/{unit,contract,integration,e2e}/
  docs/
  plans/implementation/
```

Não criar `packages/runtime`, vendoring de Core ou import de `okto_nexus`. A API do Core é única e pública. A aplicação monta dependências: journal, secret resolver, transport, clocks, bridge e event sink.

| Componente | Responsabilidade |
|---|---|
| CLI | Intenção, entrada protegida, seleção, confirmação, saída legível/JSON e recibos |
| Daemon | Instância persistente, múltiplos agentes/Servers, IPC, conexões e composição do Core |
| Identity vault | Chaves Nexus por Server/agente; nenhum usuário Nexus; aliases e refs sem segredos no config |
| WSS/HTTPS | Autenticação escopada, tickets, operações/eventos, quotas, reconciliação |
| Core | Descoberta, realização, adapters, processos, journal, controles e bridge driver |
| IPC/bridge host | CLI/sessões e extensão nativa não MCP sob escopo limitado; nenhum serviço/catálogo/proxy MCP ou autorização independente |
| Integração de SO | Start/stop/autostart, locks/ACL e ciclo de vida da aplicação |

## 4. Daemon e processos

### 4.1. Inicialização

`daemon start` inicia em background e retorna depois do IPC pronto. `daemon run` executa em foreground para diagnóstico/containers; Ctrl+C pede shutdown. `connect`/`runtime start` podem assegurar daemon sob lock. Chamadas concorrentes não criam duas instâncias.

Uma conta de SO e um diretório de estado definem o domínio de confiança local. Dentro dele, atender vários agentes e ao menos dois perfis de Server, com namespaces independentes. Não instalar um serviço por agente nem abrir WSS para identidades não importadas. Uma instância já existente é reutilizada somente após validar lock, identidade de processo e readiness — PID file não basta.

`service install` configura autostart após aprovação local. Serviço de usuário é default; admin não é exigido para uso comum. Em Linux/macOS/Windows, qualificar o mecanismo escolhido, o comportamento após fechar terminal/logout/boot e o acesso ao cofre. Em container, foreground é caminho principal. Sem mecanismo suportado, explicar limitação em vez de prometer persistência.

### 4.2. Shutdown e crash

Stop bloqueia novas admissões, drena com prazo, interrompe e encerra processos próprios pelo Core, preserva journal e informa resultado por sessão. Não deixa runtimes próprios indefinidamente sem supervisor. Attach só desanexa.

Na queda WSS, manter política de lease, não matar imediatamente nem recriar runtime. No crash do daemon, containment/guard do Core limita órfãos; restart reconcilia antes de novos efeitos. Não assumir recuperação de pipes antigos. Reiniciar daemon não reexecuta último prompt.

Atualização do aplicativo/Core deve drenar ou recusar sessões ativas segundo política explícita. Não prometer hot upgrade transparente da conversa se o processo/protocolo não oferecer essa garantia.

## 5. Credenciais e configuração local

### 5.1. Credenciais Nexus

A aplicação guarda a chave canônica importada em cofre do SO, referenciada em configuração por handle. Não persiste chave nos manifests do projeto, logs, command line de filhos ou journal. O daemon usa o secret store para gestão; cada runtime recebe apenas referência/capability limitada de sua sessão para autenticação direta no Server, nunca acesso a todas as chaves. Não existe fachada MCP.

Importar configuração MCP requer escolher arquivo/entrada com consentimento; verificar que a origem aponta ao Server esperado e testar a chave. Não remover a configuração original. Revogar/desconectar binding não revoga automaticamente a identidade inteira usada em outros locais. `identity remove` explica diferença entre remoção local e revogação central.

Fallback sem keychain usa arquivo com ACL restrita e aviso explícito de proteção efetiva. No modo serviço, diagnosticar cofre bloqueado/indisponível sem copiar a chave para local world-readable. Rotação da chave exige substituição na identidade correta, revalidação e reconciliação antes de voltar a admitir.

### 5.2. Providers e alterações no harness

Core descobre binário/versão e opções de auth por referências, sem colher segredos. A aplicação conduz a escolha/login local. Aplicar configuração efêmera por sessão sempre que possível. Se arquivo precisar mudar, usar plan/apply, backup, merge estrutural, CAS e registro de ownership de campos.

Não herdar todo home/config do provider sem considerar hooks/plugins. Não usar `--dangerously-skip-permissions`, equivalente global, ou execução irrestrita como solução para reduzir prompts. Limites reais e autorização do projeto são parte do vínculo.

### 5.3. Estado local

Guardar configurações não secretas, server_id, connector_id, aliases, binding revisions, preferências e estado de operações. Journal técnico usa a implementação do Core. Namespaces mínimos: Server → agente/binding → sessão/stream. Cópia do state dir não prova identidade do host ou ownership de processo.

Export diagnóstico redigido; export configuração omite secrets por default e não exporta tickets. Remoção/desinstalação preserva evidência pendente por default, com purge separado e explícito. Não apaga arquivos dos projetos.

## 6. CLI proposta e semântica

Os comandos abaixo são contratos a implementar, não comandos já disponíveis:

| Comando | Efeito |
|---|---|
| `connect --server URL [--agent ID] [--start]` | Importa/prova identidade; discovery e bind guiado; assegura daemon; start só se pedido |
| `identity add/list/show/remove` | Gestão de identidades importadas; nenhuma criação de Agent remoto |
| `identity replace-credential <alias>` | Substitui chave local após rotação real; valida antes de usar |
| `discover [--harness NAME]` | Inventário local redigido; não autorização nem spawn produtivo |
| `bind create/list/show/remove` | Caminho avançado de associação, sem alterar identidade canônica |
| `runtime start <alias> [--project .] [--harness NAME] [--new-session]` | Abre/reutiliza sessão autorizada; recibo consultável |
| `runtime status/inspect/logs` | Observa sem possuir ou reiniciar processo |
| `runtime interrupt <session>` | Cancela turno quando suportado, preservando runtime |
| `runtime stop <session>` | Encerra recursos gerenciados; detach quando externo |
| `daemon start/run/status/stop` | Ciclo de vida do serviço local; não significa start de todos os harnesses |
| `service install/uninstall/status` | Autostart do SO com aprovação, sem alterar identidades |
| `doctor [--json]` | Diagnóstico de origem, auth, binário, projeto, versão, IPC, canal e journal |

`--json`, `--non-interactive`, timeout e códigos estáveis nas operações de automação. Segredos via stdin/secret store; não aceitar chave canônica via argv como exemplo recomendado. Resposta inclui operação/sessão/status e ação necessária. Timeout de espera não transforma resultado em falha segura para reenviar.

TUI leve pode reunir identidades, bindings, sessões, logs e pedidos. Sem front-end web independente obrigatório. Canais de aprovação preservam a autoridade do Server: CLI não autoaprova pedido administrativo com a chave do próprio agente.

## 7. WSS de controle, MCP HTTP direto e bridge nativa não MCP

WSS conecta de dentro para fora, aceita múltiplas lanes de agente apenas após prova separada por ticket e negocia versão/capacidades. Tickets renovam automaticamente; não são uma chave raiz de instalação. Fila de controles separada de dados, backpressure e reconciliação do Anexo A são obrigatórios.

O Connector configura o **cliente** MCP HTTP do harness para a origem aprovada do Nexus Server. O harness negocia tools/resources/prompts/notificações/cancelamento diretamente com o Server; o Connector não termina sessões MCP, não traduz mensagens, não publica MCP HTTP/stdio e não encapsula MCP em WSS. Ajudar a configurar URL/credencial não inclui intermediar chamadas ou substituir a implementação MCP do harness. Calls mutáveis incertas não são repetidas cegamente pelo gerenciamento.

Para runtimes gerenciados com MCP HTTP, o Core realiza configuração direta por sessão e o Server valida a capability. Para Pi sem esse cliente, integrar apenas extensão nativa não MCP qualificada, com ações estruturadas limitadas via IPC/backend de API canônica; não publicar catálogo ou aceitar envelopes MCP. Não extrair comandos de texto gerado nem dar chave administrativa ao modelo. Fechar CLI/TUI não derruba daemon ou outra sessão. Ferramentas tools-only de conversa independente funcionam sem daemon.

## 8. Plano de execução por fases

### C00 — Scaffolding independente e inventário de contratos

**Dependências:** Nenhuma; pode iniciar imediatamente.

**Superfícies/entregáveis:** pyproject, package layout, CI inicial e plans.

| Tarefa | Implementação exigida |
|---|---|
| C00.1 | Criar aplicação Python >=3.11, CLI/daemon/IPC/transport separados; declarar Core como dependência sem instalar o Nexus Server. |
| C00.2 | Registrar estado do diretório/repositório e nomes propostos; não criar remote/pacote público nem adotar licença diferente sem autorização. |
| C00.3 | Ler contrato comum e backlog dos outros projetos; preparar fakes de portas, clock/secret/IPC/HTTP/WSS sem tratar isso como suporte real. |
| C00.4 | Definir modelo de ameaças e fronteiras: conta de SO, agentes, Servers, providers, config de repo, subprocessos e secrets; sem entidade de usuário Nexus. |
| C00.5 | Criar status/backlog/matriz/evidência e CI de lint/tipos/testes/build isolado; distribuir ownership para impedir adapters duplicados. |

**Gate de saída:** Aplicação mínima instala isoladamente e import não abre daemon/porta; ausência de dependência circular comprovada.

**Testes vinculados:** TC-01, TC-02.

### C01 — Cofre e importação de identidade canônica

**Dependências:** C00, K01

**Superfícies/entregáveis:** identity, secret store, server profiles e clientes auth.

| Tarefa | Implementação exigida |
|---|---|
| C01.1 | Implementar key refs namespaced server_id/agent_id, aliases locais e APIs de cofre; nunca guardar chave no journal/config de projeto. |
| C01.2 | Implementar importação mascarada/stdin/entrada MCP selecionada, com consentimento, origem esperada e verificação /me; não varrer credenciais ou todos os agentes. |
| C01.3 | Validar TLS/server_id/redirect e comparação de agent hint; key incorreta não cria identidade ou troca o alvo silenciosamente. |
| C01.4 | Implementar substituição de chave, remoção local distinta de revoke global e diagnóstico de cofre bloqueado em serviço de SO. |
| C01.5 | Implementar fallback de cofre restrito explicitamente aprovado e testes de redaction/ACL; resolver refs por agente/sessão, sem expor credenciais globais ou criar serviço de autenticação MCP local. |

**Gate de saída:** Chave MCP existente reutilizada sem rotação; vários agentes e Servers isolados e nenhum user/login Nexus introduzido.

**Testes vinculados:** TC-03, TC-04, TC-05, TC-06.

### C02 — Daemon singleton e IPC privado

**Dependências:** C00, K01

**Superfícies/entregáveis:** daemon, IPC, lock/readiness e lifecycle.

| Tarefa | Implementação exigida |
|---|---|
| C02.1 | Implementar daemon start/run/status/stop com lock interprocesso, identidade de processo, readiness e anti-race de dois clientes iniciando juntos. |
| C02.2 | Criar Unix socket/named pipe com ACL e autenticação/escopo do peer; fallback loopback só privado, autenticado e qualificado. |
| C02.3 | Diferenciar daemon running de Server online; carregar perfis/bindings sem abrir todos os harnesses e gerenciar pelo menos dois Servers isolados. |
| C02.4 | Implementar handles de IPC de gestão por CLI/sessão com controle limitado; fechar CLI/log follower não mata daemon/runtimes independentes. IPC não transporta MCP. |
| C02.5 | Integrar drain/shutdown da aplicação ao Core e startup recovery; relatório real por recurso e nenhum kill de processo externo por PID. |

**Gate de saída:** Uma instância sob concorrência, IPC protegido, terminal observador independente e nenhum processo iniciado somente por discovery.

**Testes vinculados:** TC-07, TC-08, TC-09, TC-10.

### C03 — Connect guiado, discovery e bindings por intenção

**Dependências:** C01, C02, K03

**Superfícies/entregáveis:** connect/bind services, CLI prompts e profile resolver Core.

| Tarefa | Implementação exigida |
|---|---|
| C03.1 | Orquestrar comando copiado da tela: importar identidade, descobrir locais via Core, selecionar harness/projeto e apresentar consentimento agregado. |
| C03.2 | Usar cwd e workspace binding, preparar/apply com revisão e recibo; Git/path são dicas, não autorização de associação. |
| C03.3 | Gerar perfil/argv/configuração por templates; separar login de provider e confiança em hooks/home; problema de autenticação vira ação local clara. |
| C03.4 | Persistir progresso e retomar onboarding parcial sem duplicar Agent/endpoint/binding; preservar outras configurações de MCP com backup/CAS. |
| C03.5 | Reutilizar identidade/binding no segundo uso; ambiguity/headless retorna erro e --start acrescenta intenção explícita, não efeito de importar chave. |

**Gate de saída:** Primeiro uso guiado e recorrente sem JSON/argv/grants manuais; Server não recebe secretos de provider ou exigência de paths locais.

**Testes vinculados:** TC-11, TC-12, TC-13, TC-14.

### C04 — Cliente WSS/HTTPS com autenticação por lane

**Dependências:** C01, C02, K04

**Superfícies/entregáveis:** transport, tickets, generations, sessions e journal Core.

| Tarefa | Implementação exigida |
|---|---|
| C04.1 | Implementar HTTPS para preparar/applicar binding/ticket e WSS de saída nxl.v1, sem listener remoto nem callback na máquina do harness. |
| C04.2 | Multiplexar bindings com tickets de cada agente; renovar single-flight com jitter, validar epochs e encerrar somente lanes revogadas quando apropriado. |
| C04.3 | Implementar handshake/inventário/heartbeat/lease e CAS de geração; concorrência entre instâncias não assume takeover automático ou atestação por hostname. |
| C04.4 | Consumir journal do Core para operations/events/watermarks e reconciliar antes de novas admissões; retry de conexão não é retry de efeito. |
| C04.5 | Adicionar filas/bytes/prioridades, limites de frame, backoff e redaction; conectar a peer de contrato primeiro e reservar gate real para N05. |

**Gate de saída:** Canal seguro interoperável em contrato, isolado por Server/agente, com renovação/reconnect sem nova chave manual ou duplicação.

**Testes vinculados:** TC-15, TC-16, TC-17, TC-18, TC-19.

### C05 — Gestão dos runtimes usando somente o Core

**Dependências:** C03, C04, K04

**Superfícies/entregáveis:** runtime services, daemon composition e event projection local.

| Tarefa | Implementação exigida |
|---|---|
| C05.1 | Implementar prepare/open/submit/control/inspect/close como adaptação da API pública Core; nenhum Popen/parser nativo fora da biblioteca. |
| C05.2 | Aplicar contexto de autorização recebido, revisões e limites locais, validar novamente root/realization antes de spawn. |
| C05.3 | Gerar recibo estável para start/reuse/new-session e expor estágio/ready real; não criar processo alternativo após timeout incerto. |
| C05.4 | Gerir pool limitado e política idle explícita; separar interrupt de stop e recursos próprios de attach externo. |
| C05.5 | Encaminhar eventos/approval/input e shutdown por Core, validar execução por painel e CLI sobre a mesma sessão sem ownership da janela. |

**Gate de saída:** Runtimes remotos usam exatamente o wheel Core usado localmente no Nexus; cancelar/parar/reconectar mantêm semânticas distintas.

**Testes vinculados:** TC-20, TC-21, TC-22, TC-23.

### C06 — Autoconfiguração MCP HTTP direto e extensão nativa não MCP

**Dependências:** C05, K09

**Superfícies/entregáveis:** harness_config, integração com capability MCP HTTP do Server, testes de conexão direta e bridge nativa não MCP. Nenhum endpoint ou proxy MCP.

| Tarefa | Implementação exigida |
|---|---|
| C06.1 | Compor templates do Core para gerar URL/credencial da sessão no cliente MCP HTTP do harness, apontando diretamente ao Nexus Server; verificar alcance no processo/sandbox e não criar porta/comando/subprocesso MCP. |
| C06.2 | Qualificar o cliente HTTP real do harness com o MCP do Server; o Connector não negocia nem encaminha envelopes/tools/resources/prompts. MCP apenas stdio recebe diagnóstico, nunca proxy ou fallback. |
| C06.3 | Solicitar capability de sessão e configurar autenticação direta mínima; testar validade/renovação/revogação sem handle IPC em cada call ou chave administrativa. Tools-only já configurado não exige daemon. |
| C06.4 | Integrar extensão Pi fornecida pelo Core somente como bridge nativa não MCP, com ações limitadas de contexto/claim/complete nas APIs canônicas; não catálogo/proxy/envelopes MCP nem parsing de texto livre. |
| C06.5 | Testar MCP HTTP direto com daemon ausente em tools-only e partições WSS/HTTP independentes no managed; expiração/call incerta não gera retry/restart/túnel. Verificar ausência de superfície MCP no pacote. |

**Gate de saída:** Cliente MCP HTTP do harness chama o Server diretamente; Connector não contém servidor/proxy MCP. Configuração automática e auth comprovadas; execute_work exige caminho de ferramentas válido, direto ou nativo não MCP qualificado.

**Testes vinculados:** TC-24, TC-25, TC-26, TC-27, J16, J33, J34.

### C07 — CLI/TUI, operação cotidiana e diagnóstico

**Dependências:** C03, C05, C06

**Superfícies/entregáveis:** CLI commands, optional TUI, outputs/exit codes e doctor.

| Tarefa | Implementação exigida |
|---|---|
| C07.1 | Implementar comandos da tabela deste plano com namespace inequívoco, completion simples, --json/non-interactive e mensagens sem secrets. |
| C07.2 | Concentrar first-use e second-use em connect/runtime start; defaults aprovados reutilizados e mudanças de escopo exibidas como diff. |
| C07.3 | Implementar status/logs/inspect por camada e múltiplos Servers/agentes; observadores não possuem processo ou autorização para mutações arbitrárias. |
| C07.4 | Apresentar pedidos HITL e encaminhar ao mecanismo autorizado do Server; negar autoaprovação pela chave do agente quando exigir autoridade distinta. |
| C07.5 | Implementar doctor prescritivo para auth, cofre, binário, versão, root, IPC, WSS, journal e unknown; não sugerir desligar TLS/sandbox para fazer funcionar. |

**Gate de saída:** Fluxo humano/headless reproduzível, sem componentes técnicos obrigatórios e sem status enganoso de disponibilidade/execução.

**Testes vinculados:** TC-28, TC-29, TC-30.

### C08 — Recuperação, limites e segurança de ponta a ponta

**Dependências:** C04, C05, C07

**Superfícies/entregáveis:** recovery, fault injection, vault/log redaction e quotas.

| Tarefa | Implementação exigida |
|---|---|
| C08.1 | Injetar queda antes/depois de journal/spawn/write/ACK e reinício do daemon; reconciliação não reenvia trabalho incerto. |
| C08.2 | Validar lease em partição longa, relógio alterado/reboot, revogação e mudança de server_id; sem renovação autoconcedida ou failover de execução. |
| C08.3 | Exercitar flood/slow peer/disco cheio e quotas por agente/Server; manter controles urgentes e relatar gap em vez de silenciar perda. |
| C08.4 | Testar IPC não autorizado, secret handle de outra sessão, path traversal, manifests/hook/config maliciosos e alteração concorrente de arquivos. |
| C08.5 | Validar exports/logs/dumps sem keys/tickets/provider secrets; registrar limites de isolamento da conta de SO e de cleanup de plataforma. |

**Gate de saída:** Falhas não duplicam efeitos nem ampliam escopo; armazenamento limitado, contenção e diagnóstico reais.

**Testes vinculados:** TC-31, TC-32, TC-33, TC-34, TC-35.

### C09 — Instalação, serviços de SO e upgrade

**Dependências:** C02, C08

**Superfícies/entregáveis:** packaging, service managers, upgrades/rollback e uninstall.

| Tarefa | Implementação exigida |
|---|---|
| C09.1 | Implementar service install/uninstall/status com consentimento, modo por conta de SO e fallback foreground; sem admin exigido no caminho comum. |
| C09.2 | Qualificar fechar terminal/logout/boot em Linux/macOS/Windows disponíveis, incluindo ACL/IPC/cofre; documentar resultados por plataforma, não generalizar. |
| C09.3 | Gerar pacote instalável que inclui dependências Core e bridge certas; sem Nexus Server, embeddings ou clone irmão como dependência. |
| C09.4 | Implementar upgrade com drain/reconcile, migração local e rollback seguro; não afirmar que pipes/sessão ativa sobrevivem a trocar daemon. |
| C09.5 | Remover somente configuração sob ownership e preservar evidência/identidades por default; purge e revogação global são operações distintas explícitas. |

**Gate de saída:** Instalação limpa e operação persistente qualificada, sem destruir config existente ou deixar serviços duplicados.

**Testes vinculados:** TC-36, TC-37, TC-38, TC-39.

### C10 — Qualificação consumidora e build de integração

**Dependências:** C06, C07, C08, C09, K11

**Superfícies/entregáveis:** CI matrix, wheels, provider/OS tests e documentação.

| Tarefa | Implementação exigida |
|---|---|
| C10.1 | Executar suites unit/contract/integration e gates dos quatro adapters via Core; registrar versões/bloqueios de provider real separadamente. |
| C10.2 | Validar dois Servers/vários agentes no mesmo daemon, identidade importada única e isolamento de credenciais/dados/pedidos. |
| C10.3 | Gerar wheel/sdist com Core pinado e bundle/hash, docs/CLI/runbooks e matriz de compatibilidade honesta. |
| C10.4 | Entregar build imutável para N13/C11, smoke contra Server real N12 quando disponível, sem exigir conclusão do gate conjunto para produzir build. |
| C10.5 | Preparar release/rollback/diagnóstico e status completo; publicação remota/PyPI não ocorre sem autorização específica. |

**Gate de saída:** Artefato Connector real instalável e pronto para campanha multi-host; mocks não qualificam providers/SO.

**Testes vinculados:** TC-40, TC-41, TC-42.

### C11 — Aceite conjunto com Nexus Server e Core

**Dependências:** C10, N12, K11

**Superfícies/entregáveis:** Topologia A/B/C, Anexo B e evidence compartilhada.

| Tarefa | Implementação exigida |
|---|---|
| C11.1 | Instalar build Connector/Core nos hosts B/C e Server N12 em A sem runtimes/credenciais/projetos; registrar topologia e firewall de saída. |
| C11.2 | Executar J01–J34 com mesmos artefatos/resultados de N13 e diferenciar provider/SO real de fixture de contrato. |
| C11.3 | Validar comando do agente → key existente → bind → start, segundo uso, mesma identidade e MCP HTTP direto; daemon ausente não bloqueia tools-only independente e nenhuma fachada MCP é publicada. |
| C11.4 | Exercitar controles/aprovações/partições/restart/drain e preservação de handoffs; analisar efeitos possíveis sem promessa exactly-once. |
| C11.5 | Consolidar evidência compatível nos três projetos e encerrar somente gates demonstrados; lista de bloqueios externa fica visível. |

**Gate de saída:** Produto remoto demonstrado ponta a ponta; nenhuma conta de usuário Nexus ou configuração técnica recorrente necessária.

**Testes vinculados:** TC-43, TC-44, TC-45, J01, J02, J03, J04, J05, J06, J07, J08, J09, J10, J11, J12, J13, J14, J15, J16, J17, J18, J19, J20, J21, J22, J23, J24, J25, J26, J27, J28, J29, J30, J31, J32, J33, J34.

## 9. Matriz de testes específica

Todos os casos começam `NOT_RUN`. Testes de contrato/unitários e testes reais possuem registros separados quando dependem de ambientes diferentes. Nenhuma inferência a partir de mocks encerra gate nativo/multi-host.

| ID | Caso | Preparação/ação | Resultado exigido |
|---|---|---|---|
| TC-01 | Instalação isolada | Instalar wheel Connector/Core sem pacote Nexus. | CLI importa; nenhuma dependência Server/dashboard/modelo pesada. |
| TC-02 | Sem conta Nexus | Percorrer modelos/CLI/onboarding/DB local. | Nenhum user/login Nexus necessário; conta SO apenas delimita processo/cofre. |
| TC-03 | Key canônica | Importar chave MCP válida, repetir import e bind. | Mesmo agent_id e referência reutilizados; nenhuma emissão/criação remota. |
| TC-04 | Agent hint falso | Dar chave A com hint B e alias ambíguo. | Abortar sem trocar identidade; segredo não ecoado. |
| TC-05 | Origem/cofre | Certificado inválido, redirect cross-origin e keychain indisponível. | Não vaza chave; diagnóstico e fallback somente explícito/restrito. |
| TC-06 | Isolamento múltiplo | Importar A/B em dois Servers com aliases iguais. | Namespaces distintos; handle/secret não acessa outro contexto. |
| TC-07 | Singleton | Duas CLIs e serviço de autostart iniciam daemon ao mesmo tempo. | Um owner/readiness; lock stale não mata processo alheio. |
| TC-08 | IPC | Outro usuário/peer não autorizado chama controle ou segredo. | Negação antes do efeito; ACL/handles verificados. |
| TC-09 | Terminal independente | Fechar CLI/log follower/TUI que iniciou o daemon. | Daemon e sessões independentes continuam; não há processo-fachada MCP nem ownership pela janela. |
| TC-10 | Daemon versus runtime | Iniciar daemon com muitos bindings registrados. | Nenhum harness abre sem intenção/autostart autorizado. |
| TC-11 | Discovery seguro | Binário falso no cwd/PATH ambíguo e versões diferentes. | Não executa candidato não aprovado; seleção/diagnóstico, sem colher segredos. |
| TC-12 | Connect completo | Comando da tela do agente + chave + escolha/aprovação. | Binding/config automáticos e reproduzíveis; usuário não fornece IDs internos/argv. |
| TC-13 | Falha de setup | Interromper apply e editar config MCP simultaneamente. | Retry idempotente/CAS e backup; entradas alheias preservadas. |
| TC-14 | Projeto remoto | Mesmo projeto em paths diferentes e symlink alterado. | Vínculo lógico explícito, validação física local e drift detectado. |
| TC-15 | WSS de saída | Firewall nega entrada no host B; Server A recebe Connector. | Controle bidirecional remoto funciona sem porta pública em B. |
| TC-16 | Tickets lanes | Renovar A/B, revogar A e tentar usar ticket em outro binding. | Renovação automática restrita; B isolado e ticket fora de escopo recusado. |
| TC-17 | Reconnect | Perder socket durante operação e restabelecer. | Reconciliar antes de admitir; não reiniciar processo/turno por reconnect. |
| TC-18 | Geração/clone | Dois processos de daemon usam state clonado; chega frame antigo. | Sem takeover cego; conflito/epoch/ownership explícitos, sem alegar hardware identity. |
| TC-19 | Prioridades | Flood de streaming e controle urgente simultâneo. | Filas finitas/fairness; interrupt/revoke/ACK não bloqueados indefinidamente. |
| TC-20 | Core único | Inspecionar import/process code e executar adapter nos dois hosts. | Nenhum Popen/parser nativo duplicado no Connector; mesma API/wheel. |
| TC-21 | Reuso | Start repetido e --new-session explícito; timeout de resposta. | Compatível reutilizado; novo processo apenas por intenção nova válida. |
| TC-22 | Interromper/parar | Interrupt turno, depois runtime stop; alvo attach externo. | Semânticas distintas e nenhuma morte de alvo externo no detach. |
| TC-23 | Lifecycle de saída | Parar daemon com trabalhos e eventos pendentes. | Drain/grace/forced report honesto; journal preservado e recursos próprios contidos. |
| TC-24 | MCP HTTP direto | Configurar cliente real do harness para Server; exercitar tools/resources/notificações compatíveis em tools-only sem daemon. | Tráfego vai do harness ao Server; identidade preservada e nenhuma dependência de Connector no caminho. |
| TC-25 | Ausência MCP no Connector | Inspecionar package/CLI/portas/config gerada; tentar opção MCP stdio/HTTP e harness MCP apenas stdio. | Sem servidor/proxy/relay MCP ou entrypoint mcp; diagnóstico claro para transporte incompatível e nenhum fallback. |
| TC-26 | Bridge nativa Pi | Invocar contexto/claim/complete por extensão não MCP qualificada. | Mesmo domínio do Server; sem envelope/catálogo/endpoint MCP; texto livre não altera handoff. |
| TC-27 | Dois canais e tool incerta | Derrubar WSS/HTTP separadamente e expirar capability após tool HTTP possivelmente aceita. | Managed respeita lease/scope; sem retry cego/proxy/restart e sem bloquear tools-only independente por daemon offline. |
| TC-28 | CLI cotidiana | Repetir start/status/logs em modo humano e --json. | Sem configuração técnica/renovação manual; recibos estáveis e códigos úteis. |
| TC-29 | Approval | Agente tenta aprovar sua escalada e duas interfaces respondem. | Autoridade canônica/CAS; própria chave não contorna operador exigido. |
| TC-30 | Doctor | Simular auth_required, version unknown, root offline e journal cheio. | Diagnóstico por camada, sem sugerir remover TLS/sandbox. |
| TC-31 | Crash boundaries | Matar daemon em torno de journal/spawn/write/ACK. | Recovery mantém possível efeito e não reenvia prompt incerto. |
| TC-32 | Lease relógio | Partição longa + rollback relógio + reboot. | Sem prorrogação por replay/clock; reautenticação/reconciliação antes de efeito. |
| TC-33 | Saturação disco | Encher journal e flood de eventos de um binding. | Bloqueio de novas admissões, reserva crítica e fairness; perda não escondida. |
| TC-34 | Config hostil | Tentar path traversal, hook não aprovado e handle de outra sessão. | Sem ampliação de root/secret/ações; config alterada tratada como drift. |
| TC-35 | Redaction | Revisar argv de filhos/config/log/export/dump. | Sem key Nexus/ticket/admin/provider secrets fora do escopo necessário. |
| TC-36 | Autostart | Instalar serviço sem admin no SO suportado e reiniciar sessão/host. | Comportamento qualificado de terminal/logout/boot, cofre e readiness. |
| TC-37 | Foreground | Rodar daemon em container/terminal e enviar Ctrl+C/SIGTERM. | Shutdown controlado e exit report, sem promessa de persistência após parent death. |
| TC-38 | Upgrade | Atualizar com sessão ativa e journal pendente. | Drain/reconcile ou recusa segura; não afirma reabrir pipes antigos magicamente. |
| TC-39 | Uninstall | Remover serviço/config própria com outros MCPs e projetos presentes. | Somente campos próprios removidos; histórico/keys preservados por default. |
| TC-40 | Packages | Instalar wheel fixado em máquina limpa sem clones irmãos. | Core/bridge assets corretos, CLI/daemon utilizáveis. |
| TC-41 | Dois Servers reais | Conectar perfis independentes e remover binding de um. | Credenciais/quotas/processos do outro não alterados. |
| TC-42 | Qualificação adapters | Usar Core com versões reais de Codex/Pi/Claude e attach delimitado. | Matriz evidencia comportamento/capacidade; indisponível fica NOT_RUN. |
| TC-43 | A/B/C | Executar Anexo B com Server sem binários/providers/projetos. | Distribuição funcional sem instalação de Server nos hosts de harness. |
| TC-44 | MCP identidade | Usar MCP HTTP direto e runtime remoto com a mesma chave/agente. | Unicidade canônica e consumo exclusivo, sem sessão/agente duplicado por transporte. |
| TC-45 | Release conjunto | Conferir artefatos/SHAs/hashes e todas as evidências J. | Build final só anuncia o que foi qualificado, sem fechar gates por fakes. |

## 10. Operação, upgrade e definição de pronto

### 10.1. Runbooks obrigatórios

Documentar instalação normal e sem service manager; connect/import de key existente; bind e runtime start; diferença entre interrupt/stop/detach; cofre bloqueado; provider login ausente; key revogada; Server mudou de identidade; daemon/IPC stale; journal cheio; resultado unknown; atualização/rollback e remoção de config própria.

A instrução de recuperação não pode recomendar apagar journal e iniciar novamente uma tarefa incerta. Não recomendar desligar TLS/sandbox ou dar credencial de operador ao harness. Incluir comando de diagnóstico/export redigido que não altere estado como efeito de inspeção.

### 10.2. Definition of Done do Connector

Uma instalação/daemon administra várias identidades e pelo menos dois Servers com isolamento. O comando do agente importa chave canônica e gera vínculo local com descoberta; não há login/cadastro de usuário Nexus. Uso recorrente requer somente intenção de iniciar/reusar; nenhuma edição de endpoint/profile/JSON/argv ou autorização manual de ticket por sessão.

Core implementa todos os mecanismos nativos; Connector não copia parsers/process supervision. WSS é de saída, duplex e autenticado por lane, com reconnect/lease/journal. MCP HTTP é direto entre harness e Server; o Connector só configura esse cliente e não possui serviço/proxy MCP. A bridge Pi é nativa não MCP e limitada; CLI não possui runtimes independentes. Fim de turno não completa handoff automaticamente.

Daemon/service lifecycle é qualificado por SO. Shutdown não abandona recursos próprios sem controle, nem mata alvo externo. Crash/reconnect não executam de novo resultado desconhecido. Cofre/ACL/redaction e modificação de configuração respeitam a fronteira de confiança real.

C10 produz artefato testável; C11/J fecha integração real. Não chamar peer fake de servidor funcional, nem Windows/macOS qualificado com evidência exclusiva de Linux. Falta de credenciais/hosts autorizados bloqueia esses gates e não impede registrar o trabalho realizado.

### 10.3. Entrega final do Codex

Projeto separado, wheel/sdist, CLI/help/TUI eventual, service definitions, config/journal migrations, suites/evidências por plataforma, docs/runbooks, compatibilidade do Core/Server, hashes/SHAs e lista explícita de gates bloqueados. Nenhuma publicação ou criação de repo remoto implícita.

---

# Anexo A — Contrato comum dos três projetos

**Revisão normativa:** `nxl-1-agent-centric-http-only-2026-09-25-r3`.
**Status:** especificação de implementação, não protocolo já disponível.
**Fonte única futura:** repositório independente `nexus-connector-core`, em `contracts/nxl/v1/`. Os três planos reproduzem este anexo a partir do mesmo texto; o manifesto do pacote registra seu SHA-256.

## A.1. Decisões que não podem ser reinterpretadas

1. O sujeito de colaboração e autorização do Nexus é o **agente canônico**. Não introduzir cadastro, login, tenant ou propriedade por usuário humano para fazer o Connector funcionar. A conta do sistema operacional que executa um serviço é apenas uma fronteira local de permissões.
2. A chave de agente já utilizada pelo MCP autentica a mesma identidade no onboarding remoto. Importar uma chave não cria um agente; escolher Codex/Pi/Claude não troca seu `agent_id`. Um identificador de Connector/executor não é um novo agente nem uma chave mestra.
3. O Nexus Server usa `nexus-connector-core` diretamente para runtimes locais. Não depende da aplicação Connector, de pareamento remoto ou de WSS em loopback. O Connector usa a mesma biblioteca para runtimes remotos.
4. Core é **terceiro projeto e distribuição Python independente**, não subpasta publicada a partir do Connector. A distribuição proposta é `nexus-connector-core`; o import é `nexus_connector_core`. Confirmar disponibilidade dos nomes antes de publicação autorizada; não mudar o limite arquitetural por conflito de nome.
5. O Connector inicia WSS para o servidor. O canal é bidirecional e controla runtimes. O único MCP do produto é o **MCP HTTP direto no Nexus Server** (Streamable HTTP compatível com o SDK/protocolo adotado). HTTPS do Connector atende autenticação/configuração, operações consultáveis, recursos e APIs canônicas não MCP; não encaminha MCP. WSS NXL não transporta nem encapsula MCP. SSE do dashboard e mecanismos HTTP existentes não precisam ser removidos.
6. `agent_id`, conexão, sessão de runtime e conversa nativa são entidades diferentes. A mesma identidade pode usar vários meios; isso não concede memória compartilhada, concorrência irrestrita ou execução duplicada de uma entrega.
7. Inbox, outbox canônica, handoffs, identidades, grants e decisões de aprovação ficam no Server. O journal técnico do executor não é outra inbox nem autoriza trabalho offline.
8. O caminho comum é configurar uma vez, reutilizar depois. Não exigir JSON, argumentos nativos, IDs de endpoint/perfil ou renovação manual de tickets por sessão. Decisões reais de segurança, instalação ausente, login de provider e mudança de escopo continuam explícitas.
9. Não desfazer trabalho posterior na branch, mudar licença, publicar pacotes ou criar repositórios remotos como efeito implícito de implementar estes planos.
10. **Remover o MCP stdio introduzido na v0.2.0 do Nexus; não preservar como legado, opcional ou fallback.** Connector e Core não oferecem servidor, fachada, proxy ou relay MCP, seja stdio ou HTTP. Quem consome MCP HTTP se conecta ao Nexus Server diretamente, inclusive no modo local.
11. Stdio de protocolos nativos de runtime permanece permitido. O transporte stdin/stdout de um adapter não se torna MCP por compartilhar esse mecanismo. Não apagar adapters nativos ao retirar o MCP stdio.
12. Configurar automaticamente o cliente MCP HTTP do harness não é intermediar suas chamadas. Nenhuma chamada MCP pode depender de processo-fachada, porta MCP do Connector, IPC do daemon ou túnel NXL. Clientes sem suporte HTTP não recebem fallback MCP stdio; diagnosticar a capacidade não suportada.

## A.2. Repositórios, dependências e responsabilidades

| Projeto | Conteúdo obrigatório | Conteúdo proibido |
|---|---|---|
| `OktoLabsAI/okto-nexus` | Domínio canônico; admissão/autorização; roteamento; executor embutido; ingresso WSS; API/MCP HTTP/CLI/dashboard; migração e remoção de MCP stdio | Implementação paralela dos protocolos nativos; dependência da aplicação Connector; resolução de paths remotos no host central; servidor MCP stdio ou shim de compatibilidade |
| `okto-nexus-connector` — novo repositório proposto | CLI; daemon; serviço de SO; armazenamento das credenciais importadas; IPC privado; cliente WSS/HTTPS para gestão; configuração do cliente MCP HTTP direto do harness | Cadastro de agentes independente; autorização de handoff offline; cópia dos adapters ou do kernel; login de usuário Nexus; servidor/fachada/proxy/relay MCP de qualquer transporte |
| `nexus-connector-core` — novo repositório | Tipos/contratos NXL; descoberta; templates; adaptadores; supervisão física; journal técnico; normalização; configuração declarativa de MCP HTTP direto; bridges nativas não MCP | Servidor Nexus; UI; daemon; autenticação canônica; WSS/HTTP de produto; política de inbox/handoff; implementação de servidor/proxy MCP ou extra MCP de fachada |

Dependências de instalação: `Nexus → Core` e `Connector → Core`. O Core não importa nenhuma das aplicações. O Server nunca importa o Connector. O Core não possui efeito colateral de abrir portas, iniciar threads/processos ou ler credenciais ao ser importado.

A implementação dos clientes WSS e das rotas HTTP pertence às aplicações; modelos, codecs, reducers e fixtures são compartilhados no Core. O armazenamento técnico possui uma implementação SQLite de referência no Core, via porta substituível. No modo embutido, o Server pode adaptar seu próprio UoW para evitar cópias desnecessárias; não misturar efeitos de processo/rede dentro de transações SQL.

## A.3. Identidade, chave e credenciais: significado exato

| Elemento | Significado e autoridade |
|---|---|
| `server_id` | Identidade persistida da instalação Nexus; não inferida somente do hostname |
| `agent_id` | Identidade canônica já existente; determinada pela credencial autenticada, não pelo JSON |
| Chave canônica do agente | A mesma credencial usada pelo MCP; armazenada no cofre local do Connector quando importada; só enviada ao servidor autorizado |
| `connector_id` | Identificador técnico persistente da instalação remota; não autentica nada por si só |
| `executor_id` | Local de execução cadastrado pelo Server; pode ser embutido ou remoto; admite apenas bindings autorizados |
| `binding_id` | Vínculo entre agente, executor, adaptador e intenção de configuração aprovada |
| `workspace_id` | Escopo lógico canônico de colaboração |
| `workspace_binding_id` | Vínculo do workspace com um diretório validado em um executor |
| `session_id` | Sessão governada de runtime; não substituir IDs de sessões históricas sem migração explícita |
| `native_session_id` / `native_turn_id` | Identificadores do protocolo do harness, subordinados à sessão governada |
| Ticket operacional | Segredo curto e limitado, emitido após autenticar a chave do agente; não nova identidade nem chave administrativa |
| Capability de sessão | Autorização limitada para MCP HTTP direto ou ações nativas não MCP; o Server valida agente, sessão e escopo. Nunca a chave canônica dentro do prompt |
| Credencial do provider | Login/API key do Codex/Claude/Pi, diferente da chave Nexus; permanece no host do harness |

O Server inspecionado guarda hash e retorna plaintext apenas ao emitir/rotacionar a chave [R03]. Portanto, **não é possível recuperar a chave atual a partir do hash**. A tela nunca deve rotacioná-la para montar um comando de conexão sem informar e obter a autorização já exigida pela gestão de chaves. Isso quebraria os MCPs que usam a chave anterior.

O fluxo obrigatório aceita importação protegida da chave existente, inclusive de uma entrada MCP explicitamente selecionada pelo operador. Não varrer arquivos, keychains ou históricos procurando segredos. Descobrir instalações/configurações candidatas não equivale a descobrir ou conceder credenciais.

Não tornar par de chaves de máquina, OAuth de usuário, SSO, conta de operador ou segunda chave persistente de agente pré-requisitos desta versão. Tickets de transporte e capabilities de sessão são detalhes internos, renovados enquanto a chave, o agente e o vínculo continuarem válidos. Rotação/revogação da chave canônica exige nova credencial válida: isso não pode ser reparado por uma renovação automática que contorne a revogação.

Preservar as superfícies administrativas existentes, inclusive eventual identidade reservada de operador. Isso não cria entidade de usuário Nexus nem autoriza entregar credencial administrativa aos harnesses.

## A.4. Fluxos normativos de configuração e uso

### A.4.1. Remoto: comando do agente para o Connector

A tela de um agente existente oferece “Conectar harness em outra máquina”. Gera comando adequado ao shell com endereço e hint do agente, sem embutir chave de longa duração em argv:

```bash
okto-nexus-connector connect --server https://nexus.exemplo --agent ag_123
```

O comando é proposto para implementação. A CLI pede a chave em entrada mascarada, aceita `--credential-stdin` para automação segura ou uma entrada MCP escolhida explicitamente. Se a interface possui a chave em memória no momento legítimo de emissão/importação, pode oferecer transferência protegida; isso não pode exigir persistir plaintext no Server nem simular recuperação do hash. O caminho garantido é chave existente por entrada protegida; não é obrigatório construir um broker de transferência de segredos nesta entrega.

O Connector valida TLS/origem, resolve `/me` pelo mecanismo de chave existente e compara a identidade retornada com o hint. Em divergência, aborta: não muda o alvo nem registra novo agente. Não baixa uma lista global de agentes. Descobre binários/configurações locais, pede seleção quando houver ambiguidade, resolve o projeto atual e apresenta **uma confirmação agregada** de agente + harness + host + projeto + permissões. Configuração técnica, perfil e endpoint são gerados de modo idempotente.

A chave é guardada como referência em cofre local, namespaced por `server_id + agent_id`; aliases são apenas nomes locais. Várias identidades podem apontar para a mesma instalação de Codex, mantendo sessões/ambientes isolados. O mesmo agente pode ter bindings distintos aprovados; isso não habilita broadcast de trabalho a todos.

`connect` pode iniciar o daemon automaticamente. Não inicia um harness apenas por descobrir ou importar a chave. `--start` ou a escolha explícita “conectar e iniciar” acrescenta a intenção de abrir runtime.

Uso recorrente, dentro da pasta do projeto:

```bash
okto-nexus-connector runtime start meu-codex
```

Sem novo cadastro, JSON ou emissão de chave. Uma troca de diretório exige apenas o consentimento definido pela política de roots, não toda a configuração novamente. Em headless, não escolher silenciosamente um agente/harness ambíguo; retornar erro acionável.

### A.4.2. Local nativo

```bash
okto-nexus serve
# Em outro terminal, no projeto:
okto-nexus runtime start ag_123 --harness codex
```

A CLI utiliza o proprietário local existente, autenticado pelo mecanismo administrativo/IPC local já confiável, e o Server produz uma execução **como o agente escolhido**, com verificação de permissões e auditoria. Não pedir que o Server recupere uma chave cujo plaintext não possui. A chave administrativa não é repassada ao runtime; gerar capability de sessão limitada.

Subcomandos de runtime podem reutilizar `--ensure-server` explicitamente aprovado para iniciar o Server quando ausente; não ativar um listener público ou configurar autostart como efeito de uma chamada MCP. Instalar o Core como dependência de `serve` e `serve-lite`; nada de instalar/parear o aplicativo Connector no mesmo host.

### A.4.3. Três intenções diferentes

`tools-only`: o próprio harness acessa diretamente o MCP HTTP do Nexus Server; não exige instalar ou executar Connector/Core. O Connector pode ajudar a escrever essa configuração, mas não participa de suas chamadas. `managed`: iniciar/reutilizar processo e sessão administrados pelo Core. `attach`: conectar-se a uma sessão existente apenas por mecanismo suportado e aprovado. A UI mostra qual ocorreu; configurar MCP HTTP não adota a conversa nativa nem transfere sua memória.

## A.5. Autorização e multiplexação no canal remoto

A autenticação de agente reaproveita o guard existente. Propor rotas HTTP abaixo, adaptando ao prefixo real sem manter serviços duplicados:

| Método/rota proposta | Entrada e efeito |
|---|---|
| `GET /v1/connections/me` | Chave de agente; retorna só identidade autenticada, permissões de conexão e revisões pertinentes |
| `POST /v1/connections/bindings:prepare` | Intenção própria, executor técnico e inventário redigido; proposta sem spawn |
| `POST /v1/connections/bindings:apply` | Proposta, revisão e aprovação autorizada; cria/reutiliza binding/endpoint/perfil em transação |
| `POST /v1/connections/bindings/{id}/ticket` | Chave do mesmo agente; ticket curto limitado ao binding e ao executor |
| `POST /v1/runtime/intents:resolve` | Binding/projeto/intenção; retorna plano de realização e status, sem executar |
| `POST /v1/runtime/operations` | Operação idempotente autorizada; 202 + recibo durável ou resultado já conhecido |
| `GET /v1/runtime/operations/{id}` | Consulta escopada; não reenvia um efeito |
| `GET /v1/runtime/executors/{id}/link` | Upgrade WSS, autenticado com ticket; subprotocolo `nxl.v1` |
| `POST /v1/runtime/approval-decisions` | Decisão autorizada e correlacionada a pedido pendente; CAS |

O Server deriva o `agent_id` da autenticação; campos de payload são hints ou assertions comparadas, nunca substitutos. Criação automática de binding só é permitida para a própria identidade e segundo políticas existentes. Ter a chave identifica o agente, mas não concede sandbox mais amplo nem desativa negações explícitas.

Por instalação/executor e Server, manter preferencialmente um WSS com lanes lógicas por binding. O primeiro ticket autentica a abertura; `binding.attach` adiciona outra lane **somente com ticket obtido pela chave daquele agente**. O canal inicialmente não conhece outros agentes. Revogar A remove autoridade de A sem conceder B nem derrubar suas sessões por acidente. Tickets não aparecem em URLs, logs, snapshots, métricas ou journal; a mensagem de attach é classificada como sensível e redigida.

Um daemon pode atender múltiplos Servers, com canais, cofres, namespaces, quotas e processos isolados por `server_id`. A implementação inicial deve suportar ao menos dois perfis de servidor na mesma máquina, sem trocar um pelo outro nem duplicar daemon. Não compartilhar credenciais entre origens; redirects de credenciais para outra origem são recusados.

Persistir `credential_epoch` e `authorization_revision` de cada binding; revogação/rotação cancela emissão e uso de tickets antigos, marca sessões sem autoridade e interrompe novas admissões. No Server online, invalidação síncrona alcança cache, lanes WSS e capacidades delegadas. Na partição, o limite é o lease local, não uma promessa de revogação instantânea.

## A.6. Workspace lógico e realização local

O path físico só é resolvido pelo host que o possui. O vínculo armazena `workspace_id`, `executor_id`, `workspace_binding_id`, `binding_revision`, root canônico local e evidência de validação. Root pode ser mantido no executor e representado por handle no Server; se for exibido, respeitar permissão e redigir nos logs gerais.

Um projeto novo pode criar workspace lógico automaticamente quando a identidade estiver autorizada. Um checkout do mesmo repositório em outro host é candidato de associação, não prova de acesso: pedir seleção/aprovação única ou usar vínculo explícito já aprovado. Git remote, branch, nome de pasta e manifestos não são identidade confiável. Não mesclar dois workspaces porque seus paths ou remotes são iguais.

Preservar IDs legados baseados em path e criar aliases/vínculos aditivos. Não re-hashear histórico nem exigir migração destrutiva para IDs UUID. Novos IDs são opacos e gerados pelo Server. APIs antigas com `project_root` local continuam por adaptador de compatibilidade; paths vindos de um cliente remoto não passam por `realpath()` no Server.

Cwd, symlinks, mudança de root, wrappers, provider homes e executable drift são verificados de novo antes de spawn, não apenas no onboarding. Root aprovado restringe realização; não equivale a sandbox do sistema operacional. Informar honestamente as restrições efetivas do harness e do SO.

## A.7. API pública mínima do Core

A API final deve ser tipada e documentada; esta é a semântica obrigatória, não implementação pronta:

```python
class RuntimeCore(Protocol):
    async def discover(self, request: DiscoveryRequest) -> Inventory: ...
    async def prepare(self, intent: LaunchIntent, context: ExecutionContext) -> PreparedLaunch: ...
    async def open(self, operation: OpenOperation, context: ExecutionContext) -> OperationReceipt: ...
    async def submit(self, operation: TurnOperation, context: ExecutionContext) -> OperationReceipt: ...
    async def control(self, operation: ControlOperation, context: ExecutionContext) -> OperationReceipt: ...
    def events(self, cursor: EventCursor) -> AsyncIterator[RuntimeEvent]: ...
    async def inspect(self, session_id: str) -> RuntimeSnapshot: ...
    async def reconcile(self, request: ReconcileRequest) -> ReconcileReport: ...
    async def close(self, operation: CloseOperation, context: ExecutionContext) -> OperationReceipt: ...
    async def shutdown(self, policy: ShutdownPolicy) -> ShutdownReport: ...
```

`ExecutionContext` é fornecido pelo host confiável após autorização canônica e inclui os limites aplicáveis, binding/revisões/lease e prova de ownership. O Core não recebe `agent_id` livre do modelo como autoridade nem valida a API key canônica. Ele aplica a interseção entre limites recebidos, aprovação local e capacidades reais.

Ports obrigatórias: relógio monotônico/parede, journal transacional, process backend, secret resolver local, event sink, approval/work bridge e resolução de artefatos. Implementações default não dependem das aplicações. `PreparedLaunch` contém argv estruturado e refs de segredo locais; nunca `shell=True` ou código executável vindo do Server. Não expor classes internas de adapter como contrato público.

O Core gera dados de configuração para o cliente MCP HTTP do harness: URL do Server e referência segura de credencial/capability. Não recebe nem encaminha mensagens MCP, não hospeda MCP e não inclui extra de fachada ou dependência MCP para esse fim. Para harness sem cliente MCP HTTP e com extensão nativa comprovada, oferece bridge estruturada **não MCP**, limitada a ações explícitas de domínio. O Server injeta backend canônico local; o Connector pode chamar as APIs HTTPS canônicas para essas ações. Essa exceção não aceita envelopes/catálogos MCP, não se aplica ao caminho de um cliente MCP HTTP e não implementa outro motor de claim/complete/approval.

## A.8. Mensagens NXL e compatibilidade

Negociar `protocol_major=1`, revisão do contrato, versão de Core, tipos de evento, limites e capacidades efetivas. Durante o desenvolvimento pré-release, exigir a revisão exata `nxl-1-agent-centric-http-only-2026-09-25-r3`; o draft anterior r1 não é aceito por possuir o mesmo major. Compatibilidade entre revisões só existe após implementação/fixtures explícitas. Major incompatível falha antes de qualquer efeito. Minor adiciona campos opcionais/tipos negociados; permissões e ações desconhecidas falham fechadas. Não atualizar automaticamente o harness ou baixar schemas de `main` em runtime.

Famílias de frame: `hello`, `welcome`, `binding.attach`, `binding.detach`, `inventory.snapshot`, `inventory.delta`, `operation.submit`, `operation.receipt`, `operation.query`, `event.batch`, `event.ack`, `reconcile.request`, `reconcile.report`, `lease.renew`, `lease.granted`, `approval.request`, `approval.decision`, `heartbeat`, `error`, `goaway`.

Envelope de operação ilustrativo (campos sensíveis não estão neste exemplo):

```json
{
  "protocol_major": 1,
  "type": "operation.submit",
  "server_id": "srv_A",
  "executor_id": "exe_B",
  "binding_id": "bind_codex",
  "agent_id": "ag_123",
  "workspace_id": "ws_456",
  "workspace_binding_id": "wb_789",
  "session_id": "rs_321",
  "operation_id": "op_abc",
  "action": "turn.submit",
  "connection_generation": 7,
  "authorization_revision": 3,
  "configuration_revision": 4,
  "intent_hash": "sha256:<64-hex>",
  "payload": {"text": "Execute o trabalho autorizado", "delivery_id": "dlv_001"}
}
```

Gerar JSON Schemas de todos os frames, intents, eventos, erros, inventário, gates de capacidade e respostas HTTP; publicar fixtures positivas/negativas. Os exemplos não substituem schema executável. Artefatos grandes usam refs autorizadas e limitadas; não interpretar URLs arbitrárias como instruções de fetch sem política de origem, tamanho e workspace.

O WSS é um canal customizado de controle do produto. O MCP HTTP direto existente permanece no SDK/protocolo já utilizado; a consulta de especificações atuais não autoriza migrar implicitamente `mcp>=1,<2` para outro major.

## A.9. Operações, deduplicação e efeito incerto

Cada intenção mutável recebe `operation_id` persistente **antes do primeiro envio**. O Server reserva operação/outbox em transação; só depois despacha. O executor grava recebimento/intenção em seu journal antes do efeito nativo. Nenhuma dessas gravações torna spawn/write nativo atomicamente transacional.

Calcular `intent_hash = SHA-256(JCS(intent))`, por implementação testada de canonicalização. O objeto semântico inclui Server/agente/binding/executor/workspace/sessão/ação/payload/revisão de configuração/turno esperado. Exclui número de tentativa, ticket, prazo de renovação e geração do canal; reconectar não muda a intenção. Não aceitar NaN, infinitos, chaves JSON duplicadas ou campos semanticamente ambíguos. Gerar vetores de hash, inclusive Unicode e ordem de campos.

Mesmo ID e mesmo hash retornam o recibo/estado conhecido, sem repetir o efeito. Mesmo ID com hash distinto gera `OPERATION_CONFLICT`. Receber um erro de rede após admissão leva a consulta/reconciliação pelo mesmo ID, nunca a novo ID automático. Trocar configuração ou pedir nova execução é uma intenção nova explicitamente autorizada.

Estágios mínimos: `RECEIVED_DURABLE`, `PREPARED`, `SUBMISSION_STARTED`, `SUBMITTED`, `ACCEPTED` quando demonstrável, `RUNNING`, `WAITING_INPUT`, e terminais `SUCCEEDED`, `FAILED`, `CANCELLED`. `OUTCOME_UNKNOWN` expressa efeito possível sem comprovação e bloqueia replay automático; não é sinônimo de falha segura para tentar novamente.

ACK WSS, write/drain de pipe, aceitação nativa, final de turno e conclusão de handoff são fatos diferentes. Se um protocolo não emite ACK de aceitação, não inventá-lo. Eventos finais podem provar conclusão mesmo que ACK intermediário tenha sido perdido; reducer aceita essa ordem sem fabricar transições observadas.

Reconciliar pode resolver um estado incerto com evidência nova; não pode declarar “não executou” apenas por timeout. Não prometer exactly-once de efeitos externos. Retry por deduplicação nativa só é permitido quando a versão do protocolo comprovar essa garantia.

## A.10. Eventos duráveis e controle sob carga

Evento recebe identidade estável `(server_id, executor_id, session_id, stream_epoch, sequence)` antes do envio. `sequence` é monotônica e persistida; `stream_epoch` não muda ao reconectar WSS. Duplicatas mantêm identidade/hash. Novo processo/sessão pode criar outro epoch explicitamente; nunca reiniciar sequência fingindo continuidade.

Categorias normalizadas: lifecycle, estado de turno, text delta/snapshot, atividade de ferramenta, approval/input request, métricas de uso disponíveis, aviso de sistema/rate limit, erro e evento nativo desconhecido. Preservar `native_type` e metadados limitados, sem rotular tudo como atividade de ferramenta. Não inventar conteúdo interno de raciocínio ausente; não armazenar segredos por diagnóstico.

ACK de eventos significa commit no ingresso durável do Server, não entrega à UI. Watermark somente contíguo; gaps pedem replay ou são explicitamente registrados quando irrecuperáveis. Projeção canônica posterior é idempotente. Após revogação, eventos antigos autenticáveis podem ser tratados como evidência histórica conforme política restrita, nunca como nova autoridade ou início de outra entrega.

Filas possuem limites por bytes e itens; fairness por binding. Reservar orçamento para interrupt, revoke, approval, ACK e terminais. Fragmentar batches de texto para que controle não aguarde frames enormes. Saturação/disk full fecha novas admissões e produz estado honesto; não descartar silenciosamente finais. Deltas podem ser compactados em snapshot com marcação explícita; dados críticos não são tratados como descartáveis.

## A.11. Ciclos de vida: daemon, runtime e turno

CLI canônica do Connector: `daemon start`, `daemon run` (foreground), `daemon status`, `daemon stop`, `service install`, `service uninstall`, `runtime start`, `runtime interrupt`, `runtime stop`, `runtime status`, `runtime logs`. Não usar `start` sem namespace para dois significados incompatíveis. Aliases de conveniência somente se inequívocos e documentados.

Daemon: `STOPPED → STARTING → RUNNING → DRAINING → STOPPED`, com `FAILED/RECOVERING` quando cabível. Uma instância por conta de SO + diretório de instalação/domínio de confiança; múltiplos agentes/Servers dentro dela. Lock com identidade real do processo e readiness via IPC; não confiar em PID file isolado. CLI e TUI podem terminar sem matar daemon/runtimes independentes. Não existe fachada MCP; uma conversa tools-only com MCP HTTP direto é independente do daemon.

`daemon start` retorna após readiness local; offline do Nexus aparece como degradação, não prova de startup falho do daemon. `daemon run` fica em foreground; Ctrl+C solicita shutdown. `service install` exige consentimento e usa serviço de usuário quando disponível, sem privilégio admin por padrão. Fechar terminal, encerrar login e reiniciar máquina são eventos distintos: sem autostart/serviço de SO qualificado não prometer sobreviver ao logout ou boot.

Não abrir todos os harnesses ao subir daemon/Server. Harness sobe por pedido explícito ou entrega cuja aprovação permita auto-start. `runtime start` reutiliza sessão compatível por padrão; `--new-session` é explícito. Readiness só depois de spawn, handshake/probe, credencial de provider e bridge necessários. Um runtime headless não implica abrir a TUI nativa em outra janela.

Interromper turno preserva runtime quando o adapter permite. Parar runtime impede novas entregas e termina recursos próprios. Parar daemon/Server entra em drain por prazo limitado, depois interrompe e encerra árvores próprias. Emitir relatório por sessão com graceful/forced/unknown; não alegar sucesso total se ownership/terminação não puder ser demonstrado. Attach externo faz detach; não matar o alvo de terceiros.

Default de shutdown explícito: aguardar até 30 s de drain, solicitar interrupção, aguardar 15 s, terminar/forçar conforme backend qualificado. Essas janelas são configuração interna validada, não requisito de um wizard. O encerramento não desfaz efeitos externos, mudanças em arquivos nem remove identidade/chave/histórico.

Política padrão de ociosidade: manter runtime reutilizável enquanto host estiver ativo, sob quota. Encerramento por idle só quando configurado e suportado; não perder silenciosamente conversa não retomável. Reiniciar processo não reenvia último trabalho. Restart com turno em andamento ou desconhecido requer política/decisão explícita.

## A.12. Partições, leases, ownership e restart

Desconexão do canal não prova morte do harness. Suspender novas operações remotas; um turno em execução pode continuar até expirar a autorização local. Heartbeat não renova lease de trabalho. O prazo local é monotônico, calculado conservadoramente a partir de validade/RTT; alteração do relógio ou replay do frame não o estende. Reboot exige revalidação antes de qualquer novo efeito.

Reconnect: TLS/autenticação, negociação, reconciliação, aplicação de revogações, renovação autorizada e só depois novas admissões. Uma operação de estado desconhecido não é realocada para outro host. O Server continua com único owner de despacho conforme a base; o projeto não implementa HA multiwriter.

`connection_generation` impede comandos de canal antigo; `session_owner_generation` protege ownership da execução. Um novo WSS não autoriza novo processo para a mesma sessão. Definir CAS para reconexão da mesma instalação; duas instâncias ativas conflitantes não alternam posse silenciosamente. Nova posse após crash exige reconciliação; clone de disco/chave não é distinguido por alegação de hostname. Não prometer atestação de hardware.

Ownership inclui PID, criação/birth record, nonce e contenção/guard por SO; não matar por PID sem comprovar origem. Qualificar Job Objects no Windows e grupos/guards/cgroups onde disponíveis nos Unix. Process groups sozinhos não garantem cleanup após SIGKILL do pai. Core deve registrar os limites efetivos e usar guard adequado ou marcar risco/indisponibilidade, nunca prometer ausência de órfãos sem teste.

Na falha do host supervisor, o default é impedir execução indefinida sem controle usando contenção/guard de processos próprios. Reabrir pipes de um processo antigo não é pressuposto. Resume de conversa nativa pode criar novo processo/sessão sob nova intenção, sem replay automático de trabalho incerto. Processo externo anexado permanece fora da política de kill.

## A.13. Capacidades reais e ferramentas Nexus no harness

Descritor do adapter informa protocolos, versões qualificadas, plataformas e semântica de controle. Capacidade efetiva é a interseção de implementação × versão detectada × SO × perfil × política × prova de readiness. Flags estáticas são metadados, não comprovação de suporte naquela instalação.

Qualificar Codex app-server, Pi RPC, Claude stream e o attach existente. Completar os modos gerenciados para multi-turn, streaming, cancelamento, erros, input/aprovações e término real. Capacidades não suportadas retornam `CAPABILITY_UNSUPPORTED` antes do efeito; não converter steering imediato em interrupt+reprompt sem sinalizar e autorizar essa semântica.

Runtimes com cliente **MCP HTTP** recebem automaticamente configuração de acesso direto ao Nexus Server, sem passar pelo Connector ou por helper MCP do Core. No mesmo host, usar o endpoint HTTP de serve; em outro host, usar a origem HTTPS aprovada e alcançável a partir do processo/sandbox do harness. Não criar entrada MCP do tipo `command/args` apontando para Nexus/Connector nem subprocesso MCP. Runtimes sem cliente MCP HTTP só usam integração nativa não MCP quando implementada e qualificada — por exemplo, uma extensão estruturada do Pi — limitada a ações dos casos de uso canônicos. Não exigir cURL manual nem interpretar texto livre como `claim`, `complete` ou autorização.

O Nexus Server emite/valida capabilities de sessão consumíveis diretamente pelo endpoint MCP HTTP; o Connector solicita a capability sob a identidade importada e o Core apenas realiza a configuração segura do cliente. Um handle IPC local não serve como substituto que obrigue proxy de chamadas. No modo local o Server não precisa recuperar plaintext de sua key armazenada como hash: emite a capability da sessão autorizada. Não confundir autenticação de lane NXL com autenticação MCP. Providers/harnesses só recebem credenciais próprias e capabilities estritamente necessárias ao processo, nunca chave administrativa ou coleção de chaves de agentes. Segredos ficam fora de prompts, logs, URLs e argv; usar referência/env/config isolada com ACL conforme capacidade real do cliente.

Modo sem bridge de trabalho comprovada pode ser qualificado como conversacional, mas não como `execute_work`. Essa limitação não pode ser ocultada para marcar adaptação completa. Para attach baseado em substrato não suportado, preservar e diagnosticar o caminho existente, qualificar versões delimitadas; não inventar compatibilidade universal.

## A.13.1. Topologia MCP HTTP, credenciais diretas e remoção do stdio

A correção de arquitetura é normativa e substitui qualquer instrução de preservar stdio, criar fachada ou oferecer MCP dentro do Connector/Core nas revisões anteriores. Ela não é uma opção de implantação.

```text
Ferramentas — harness com MCP HTTP, local ou remoto:
  Harness (cliente MCP HTTP) ── HTTP(S) direto ──> Nexus Server / endpoint MCP

Controle remoto de runtime:
  Nexus Server <── WSS / NXL ──> Connector + Core <── protocolo nativo ──> Harness

Controle local de runtime:
  Nexus Server + Core <── protocolo nativo ──> Harness
```

O caminho de ferramentas não atravessa o processo Connector/Core. O caminho de runtime não é MCP, ainda que o adapter nativo use stdin/stdout. O Server precisa estar rodando para servir MCP HTTP; o harness não inicia uma instância Nexus por configuração MCP `command`. IPC privado da CLI/daemon permanece permitido para gestão, nunca como endpoint MCP. HTTP(S) de gestão no Server e o cliente HTTPS do Connector continuam válidos; não confundir remoção de MCP no Connector com proibição de todo uso de HTTP.

**Autoconfiguração e independência.** Preservar conexões MCP HTTP já existentes e a identidade do agente. Para novas configurações, gerar URL alcançável no contexto real do harness — não reutilizar loopback do Server para um host remoto, e considerar container/sandbox e allowlists de rede aprovadas. Preferir configuração por sessão; persistente usa consentimento, diff, backup e CAS. Não instalar MCP server/proxy local, não expor porta MCP no Connector, não injetar catálogo MCP em NXL e não introduzir callbacks para o daemon a cada tool call. Um cenário tools-only configurado com a credencial atual funciona sem instalar Connector, e continua funcionando após desligá-lo se ele foi usado apenas para configurar; isso não promete manter vivo um runtime que o daemon possui quando ele é desligado.

**Autorização direta.** O endpoint MCP HTTP resolve o agente pela credencial/capability autenticada e aplica permissões, claims e deduplicação comuns. Managed sessions recebem capability limitada por agente/workspace/binding/sessão/ações e validade; a identidade permanece o mesmo agente, não nasce outra credencial canônica. Requisições MCP diretas não podem contornar lease, geração, grants ou revogação das operações que pertencem à sessão gerenciada. A credencial canônica de um cenário tools-only independente mantém a política própria já existente; possuir WSS não é requisito para usar MCP HTTP legítimo.

**Validade sem proxy.** Tickets NXL e capabilities MCP têm finalidade e audiência distintas. Especificar e testar emissão, expiração, renovação e revogação das capabilities no Server. Preferir segredo de sessão validado contra estado/validade renovável no Server, sem reinjetar segredo a cada chamada; não pressupor hot-reload de credenciais em todos os harnesses. Qualificar o mecanismo seguro por adapter, inclusive expiração durante turno. Não trocar uma operação incerta por restart/retry para renovar credenciais. O modo tools-only independente não depende do Connector para renovar sua credencial; revogação real exige o fluxo autorizado. Nunca aceitar automaticamente um ticket NXL como bearer MCP com escopo ampliado.

**Partições independentes.** Testar WSS indisponível com HTTP alcançável e o inverso. A perda do WSS não redireciona MCP via túnel nem autoriza novos efeitos de sessão após lease/revogação. A perda do MCP HTTP não é reparada com MCP stdio; reportar disponibilidade separada de processo, controle e ferramentas. `execute_work` exige caminho de ferramentas e autorização comprovados; runtime conversacional não deve ser anunciado como execução governada completa. Capability expirada não vira falha segura para repetir uma chamada mutável.

**Migração obrigatória.** Inventariar e remover entrypoints, subcomandos, inicialização automática por harness, módulos, distribuição, exemplos, wizard, documentação e testes que implementam ou mantêm MCP stdio no Nexus. Comando antigo deve falhar de forma prescritiva ou deixar de existir, sem abrir transporte MCP nem preservar um shim stdio→HTTP. Converter somente configurações Nexus explicitamente selecionadas e sob ownership/consentimento; manter identidade/key e entradas de outros servidores MCP. O rollback deste trabalho não reativa MCP stdio como fallback operacional; uma volta manual a artefato histórico é um downgrade fora do contrato r3, nunca suporte aceito. Remover a implementação não autoriza apagar projetos/histórico ou adapters nativos de stdio.

**Capacidade não suportada.** Harness com MCP apenas stdio não atende ao caminho MCP desta arquitetura. Retornar diagnóstico de transporte não suportado, sem instalar proxy de terceiros nem extensão genérica que apenas esconda o mesmo proxy. Uma integração nativa não MCP é uma feature separada, limitada, estruturada e qualificada; não recebe envelopes MCP, não publica MCP e não substitui o caminho direto de harness compatível.

## A.14. Inbox, respostas, handoffs e HITL

Uma entrega lógica disputa o mesmo mecanismo de consumo exclusivo entre MCP pull, runtime local e runtime remoto. A outbox registra tentativas de transporte; não vira segunda tarefa. Duplicar conexão não duplica grant, turno ou resposta. Observadores recebem eventos sem execução; não enviar prompt de trabalho a um “observador” que o executará.

Turno concluído não significa handoff concluído. O resultado governado passa pelos casos de uso de claim/grant/complete existentes. Preservar reply target, root/parent/correlation, dedupe, orçamento causal/relay e evidências. Reconnect não reinicia orçamento de mensagens nem permite loop de agentes.

Pedido HITL correlaciona binding, sessão, turno, request nativo, geração, revisão e expiry. Server autoriza/registra decisão única com CAS; UI e CLI são canais de apresentação, não identidades humanas novas nem autoridades paralelas. A chave do próprio agente não basta para aprovar uma escalada que exige operador. CLI deve encaminhar ao mecanismo já autorizado ou reportar pendência; jamais autoaprovar por ser local.

Negação, timeout, saída da UI ou rede caída não viram aprovação. Resposta tardia não é aplicada a outro turno. Estado de solicitação e decisão precisa sobreviver ao reconnect dentro das capacidades reais do harness.

## A.15. Segurança, isolamento e configuração

Cofre local para chaves; configuração/journal guarda refs. Fallback restrito com ACL é explícito e explicado, sem alegar criptografia que não existe. Host comprometido ou processo malicioso com a mesma conta do SO pode exceder a proteção da aplicação; sandbox de provider não equivale a isolamento de contas. Redigir secrets em stdout/stderr, tracing, dump e exports; evitar dumps de memória por padrão.

Não procurar credenciais automaticamente. Detectar métodos locais de autenticação, disponibilidade de login e arquivos candidatos; ler/importar só o que foi selecionado. Provider homes podem carregar hooks/plugins: consentimento de usar login não é autorização de executar todo hook do home. Resolver isolamento/config suportado por versão e informar quando não puder separar.

Alterações em configuração de harness são plan/apply com backup, comparação de revisão e merge estrutural. Preferir configuração efêmera por execução. Não sobrescrever arquivos globais ou apagar entradas de outros MCPs. `disconnect/unbind` remove somente campos sob ownership do produto; evidência de operações não é apagada como efeito de remover configuração.

Não aceitar executable, argv arbitrário, env livre, plugin dinâmico ou root enviado pela rede como autorização. O Server envia intenção/template; Core resolve realização local aprovada. Wrapper Windows deve ter implementação especializada/qualificada, não comando de shell montado por concatenação.

TLS obrigatório fora de desenvolvimento loopback explícito, sem fallback silencioso `verify=False`. Allowlist da origem, proteção de proxy reverso, limites de frame, redaction de headers e timeout de upgrade. Não expor IPC à rede externa. Unix sockets/named pipes com ACL; fallback loopback só autenticado e com proteção de origem/CSRF onde aplicável.

## A.16. Defaults, escalabilidade e observabilidade

Defaults são hipóteses de engenharia a testar, não resultados de benchmark:

| Parâmetro | Default inicial |
|---|---|
| Ticket de binding | 10 min, renovação a 70% com jitter e single-flight |
| Heartbeat / detecção de canal ausente | 15 s / 45 s; ausência não prova término do runtime |
| Lease de sessão / renovação | 120 s / 30 s; renovação condicionada à autorização |
| Grace após lease | 15 s antes de escalada de terminação própria |
| Startup de runtime | Até 90 s; API pode responder 202 imediatamente |
| Frame máximo / chunk de evento | 1 MiB / 64 KiB, medidos em bytes |
| Intenção inline | 64 KiB; recursos maiores por referência autorizada |
| Operações em trânsito / runtimes por instalação | 32 / 8, respeitando política e quotas existentes |
| Journal técnico por executor | 256 MiB, 16 MiB reservados para controle/críticos |
| Backoff de rede | 0,5–30 s exponencial com jitter; não é retry de efeito |
| Drain explícito | 30 s + 15 s de grace; resultado por recurso |

Ao atingir 80% do journal sem recuperação saudável, bloquear novas admissões e compactar somente dados elegíveis. Quotas por agente/host/Server contam todos os endpoints; fairness impede um stream monopolizar o daemon.

Cenário de pelo menos **100 mil identidades cadastradas**: onboarding autentica uma chave por índice e retorna somente vínculos pertinentes; descoberta é local. Não enumerar todas as identidades, abrir WSS para agentes offline ou alocar uma thread por agente cadastrado. Paginação por cursor, índices e caches limitados. Volume de identidades não é promessa de 100 mil runtimes concorrentes; medir concorrência separadamente.

Métricas: tempo de resolve/start/ready, reconexão, backlog em bytes, leases, gaps, operações incertas, duração de approval, recursos por adapter. Não usar `agent_id/session_id` como labels ilimitados de métricas; IDs completos ficam em logs/traces escopados e redigidos. WSS ativo, daemon vivo e runtime pronto são estados distintos.

## A.17. Erros, compatibilidade e publicação

Erros mínimos: `AGENT_AUTH_REQUIRED`, `AGENT_ID_MISMATCH`, `AGENT_REVOKED`, `CREDENTIAL_REPLACEMENT_REQUIRED`, `SERVER_ID_CHANGED`, `BINDING_NOT_AUTHORIZED`, `AMBIGUOUS_BINDING`, `APPROVAL_REQUIRED`, `PROVIDER_AUTH_REQUIRED`, `BINARY_NOT_FOUND`, `NATIVE_VERSION_UNQUALIFIED`, `PROFILE_DRIFT`, `WORKSPACE_UNAVAILABLE`, `EXECUTOR_OFFLINE`, `STALE_GENERATION`, `STALE_TURN`, `CAPABILITY_UNSUPPORTED`, `OPERATION_CONFLICT`, `OUTCOME_UNKNOWN`, `JOURNAL_FULL`, `EVENT_GAP`, `CAPACITY_EXCEEDED`, `VERSION_INCOMPATIBLE`.

Resposta inclui código, estágio, possível efeito, segurança do retry, operation_id consultável e ação corretiva. Não ecoar segredo, traceback ou root sem necessidade. Erro de rede depois de efeito possível não pode receber `retry_safe=true`.

Core usa SemVer da biblioteca, separado do wire major NXL. Antes de 1.0, fixar minor compatível e testar upgrade explicitamente. Aplicações consomem wheel imutável + hash; nada de instalar `main` ou importar pasta irmã como distribuição final. Schemas e fixtures fazem parte do wheel e sdist. Publicação PyPI é etapa técnica preparada, mas só executada com autorização e credenciais apropriadas; build local não depende disso.

Não alterar a licença dos arquivos extraídos sem decisão do titular. Preservar avisos/proveniência e registrar necessidades de packaging/licença sem transformar este plano em parecer jurídico.

## A.18. Ordem dos três projetos e evidência

O Core é dono dos contratos; N00, C00 e K00 podem começar em paralelo. K01 publica bundle de desenvolvimento a partir deste anexo. K02/K03/K04 liberam wheel incremental utilizável antes de terminar todos os adapters. Nexus e Connector integram esse wheel; não esperam conclusão total do outro projeto.

```text
N00 ───────────────┐
C00 ───────────────┤
K00 → K01 ────────┼→ N01/N02 e C01/C02
       → K02 → K03/K04 → N03/N04 e C03/C04/C05
                         → K05/K06/K07/K08 → K09 → bridges nas aplicações
N05/N06 + C04 → primeiro WSS ponta a ponta
N12 + C10 + K11 → mesma campanha conjunta N13/C11
```

Dependências cruzadas de testes são **gates de integração**, não exigência de um projeto se declarar DONE antes do outro rodar a mesma campanha. Cada build registra SHA de Server/Connector/Core e hash do bundle.

Nenhuma evidência de teste executado durante a escrita destes planos é presumida. Tarefas começam `PENDING`; testes `NOT_RUN`. Codex pode usar fakes para unitários/contratos, mas testes de provider real, múltiplos hosts e processo/SO precisam executar nesses ambientes. Sem credencial/host autorizado, registrar bloqueio externo e continuar tarefas independentes; não declarar o produto completo com mocks.

Cada evidência inclui ID, ambiente, versões, comandos, resultado, logs redigidos, duração/recursos quando relevantes e distinção entre falha anterior e regressão. Não sobrescrever evidência histórica. Implementar, testar e registrar resultados; não encerrar entregando outro planejamento.

---

# Anexo B — Aceite conjunto dos três projetos

Os casos abaixo são os mesmos nos três planos. Todos iniciam **NOT_RUN**. Registrar owner de execução, ambiente e blockers. Um cenário de contrato com peer sintético não fecha o cenário homônimo que exige provider/host/SO real.

**Topologia mínima:** A com Nexus Server sem binários/provider keys/projetos remotos; B/C com Connector e o mesmo wheel Core; uma instalação Nexus local separada sem aplicativo Connector. Para heterogeneidade, incluir Windows e um Unix em infraestrutura autorizada. Não simular SO só mudando uma string `platform`.

| ID | Cenário | Preparação/ação | Resultado exigido |
|---|---|---|---|
| J01 | Identidade canônica | Criar agente previamente no Nexus e configurar MCP com sua key; importar a mesma no Connector. | Nenhum segundo Agent/user; MCP continua válido, mesmo agent_id em mensagens e sessões. |
| J02 | Não rotacionar para conectar | Usar Server que guarda somente hash; gerar comando para identidade existente. | Comando pede/importa key existente de forma protegida; não chama issue_key silenciosamente. |
| J03 | Autenticação errada | Key A com hint B; depois ticket A tenta abrir lane de B. | Negação antes de configuração/spawn; nenhuma atribuição por payload. |
| J04 | Nexus local puro | Instalar Nexus/Core, sem app Connector, e abrir/gerir cada runtime gerenciado qualificado. | Core embutido em serve, sem daemon Connector, pareamento ou WSS local obrigatório. |
| J05 | Servidor realmente remoto | A sem executáveis/providers/projetos; B/C com Connector/Core e harnesses. | Operações e streams funcionam; Server não faz realpath/spawn de B/C. |
| J06 | Rede outbound | Firewall de B/C nega conexões entrantes e permite WSS/HTTPS ao Server; testar também o processo/sandbox do harness. | Controle via Connector e MCP HTTP direto via harness funcionam com tráfego de saída; nenhuma porta MCP local exigida. |
| J07 | Paths heterogêneos | A Linux, B Windows e C Unix; roots diferentes do mesmo e de outros repositórios. | Vínculos lógicos autorizados, validação física local e nenhuma fusão por path/Git. |
| J08 | 100 mil agentes | Sem providers, popular 100 mil identidades no Server e importar uma key em B. | Queries indexadas e escopadas; não listar tudo nem criar recursos por agente offline. |
| J09 | Um daemon, várias identidades | B importa agentes A1/A2 que usam o mesmo binário Codex e um agente Pi. | Sessões/credentials/lanes isoladas, sem daemon por agente ou prompt broadcast indevido. |
| J10 | Dois Servers | B conecta dois Servers distintos e remove/revoga binding em apenas um. | Namespaces, processos, cofre e tickets do outro preservados. |
| J11 | First-use/second-use | Configurar por comando da tela e executar runtime start repetidamente. | Setup agregado uma vez; depois sem JSON/argv/endpoint/profile/grant/chave manual recorrente. |
| J12 | Daemon automático | Connect e runtime start concorrentes com daemon parado. | Uma instância; daemon ready não abre todos os harnesses descobertos. |
| J13 | Terminal independente | Fechar CLI de start, TUI/log follower e encerrar cliente MCP HTTP tools-only independente. | Daemon e runtimes independentes continuam; fechar cliente HTTP não encerra supervisor nem outra sessão. |
| J14 | Turno versus processo | Enviar trabalho, interrupt, novo turno e runtime stop. | Cancelamento preserva runtime quando suportado; stop encerra recursos próprios, não identidade/histórico. |
| J15 | Shutdown/crash | Stop daemon/serve; depois SIGKILL do supervisor em cenário controlado. | Drain/containment e relatório real; sem kill alheio ou trabalho indefinido não declarado. |
| J16 | MCP HTTP tools-only direto | Usar conversa não gerenciada com MCP HTTP antes/depois de instalar e desligar Connector; também testar sem Connector instalado. | Harness chama somente o Server; identidade preservada, nenhum proxy/subprocesso MCP e nenhuma adoção de conversa pelo runtime. |
| J17 | Work bridge nativa não MCP | Pi sem MCP HTTP recebe tarefa e chama contexto/claim/complete via extensão qualificada. | Mesmo domínio canônico; nenhum servidor/proxy/envelope MCP no Core/Connector; texto livre não completa handoff. |
| J18 | Consumo exclusivo | Uma entrega com MCP pull e dois runtimes disponíveis ao mesmo agente. | Um consumidor lógico; sem duplo turno/grant/resposta por multiplicidade de conexão. |
| J19 | HITL concorrente | Provider pede aprovação; CLI/UI respondem, agente tenta autoaprovar e decisão chega tarde. | Autoridade existente, CAS, geração/turno corretos; timeout/deny não aprovam. |
| J20 | Recebido não é concluído | Observar ACK WSS, aceitação nativa, fim de turno e handoff em cada adapter. | Estados separados, nenhum marco antecipa outro sem evidência. |
| J21 | Resposta perdida | Perder ACK após submit nativo possível, reconectar e repetir consulta/ID. | Sem replay automático; recibo/unknown/reconciliação pelo mesmo intent. |
| J22 | Partição e revogação | Revogar key com WSS online e depois testar partição prolongada/relógio alterado. | Online invalida derivados; offline limita por lease, sem prometer revogação instantânea. |
| J23 | Gerações e takeover | Canal antigo e novo/daemon clonado disputam mesma sessão. | Um owner, fencing/reconciliação; sem failover silencioso ou processo duplicado. |
| J24 | Eventos/retention | Interromper ACK, enviar duplicatas/out-of-order e ultrapassar retenção. | Ingressão durável idempotente e watermark contíguo; gap explícito, terminais não ocultos. |
| J25 | Saturação | Flood texto, disco cheio e consumidor lento enquanto chega interrupt. | Memória/journal/queues finitos, faixa crítica preservada e novas admissões bloqueadas quando necessário. |
| J26 | Drift/config segura | Trocar binary/root/symlink/home/hook e editar MCP existente durante setup. | Reprepare/approval/CAS; sem execução ampliada, JSON destruído ou secrets centrais. |
| J27 | Quatro adapters | Qualificar Codex/Pi/Claude managed e attach existente por versão/SO delimitados. | Mesma implementação Core local/remota e capabilities comprovadas, sem sucesso simulado. |
| J28 | Upgrade/rollback | Migrar Nexus legado, atualizar Connector/Core com journal pendente e instalar wheels limpos. | Histórico/IDs/negações preservados, drain/reconcile e dependências acíclicas. |
| J29 | Segredos/ameaças | Inspecionar argv/log/config/export/metric; tentar handle/agent/server spoof e shell injection. | Sem chave canônica/admin de terceiros/provider secrets vazados ou efeito fora do escopo. |
| J30 | Release coordenado | Conferir resultados/SHAs/wheel hashes e gates provider/SO/multi-host em três relatórios. | Mesma evidência, bloqueios explícitos e nenhuma alegação de produto completo baseada só em fakes. |
| J31 | Remoção MCP stdio | Instalação limpa e upgrade do Nexus v0.2.0 com configuração MCP stdio; inspecionar entrypoints, módulos, exemplos e migração selecionada. | MCP stdio ausente, sem shim/fallback; MCP HTTP direto mantém agente/key/histórico e outras entradas não são alteradas. |
| J32 | MCP HTTP local nativo | Somente Nexus/Core no host; abrir runtime gerenciado com MCP HTTP para o próprio Server. | Sem Connector ou fachada; cliente chama endpoint HTTP de serve com capability válida e o Core só configura o cliente. |
| J33 | Fronteira protocolo/transporte | Exercitar stdio nativo dos adapters e inspecionar artefatos/portas de Connector/Core; testar harness com MCP apenas stdio. | Protocolos nativos funcionam; nenhum serviço/extra/CLI MCP/proxy; cliente incompatível recebe diagnóstico sem fallback stdio. |
| J34 | Dois canais e autorização | Em runtime remoto com MCP HTTP direto, derrubar apenas WSS e depois apenas HTTP; expirar/revogar capability e manter outra conversa tools-only independente. | Sessão gerenciada respeita lease/grants/revogação sem bypass, replay ou túnel; canal HTTP tools-only legítimo mantém política própria sem exigir daemon. |

## Registro de evidência obrigatório

Cada caso registra: `test_id`, `status`, `blocked_reason`, SHAs de Server/Connector/Core, hashes dos wheels/bundle, versões dos harnesses, SO/arquitetura, topologia e política, comandos exatos, data/hora, resultado observado, artefatos redigidos e limitações. Status permitido: NOT_RUN/PASS/FAIL. Não usar SKIP como sinônimo de PASS.

Segregar credenciais/contas e projetos de teste. Não executar faturamento/provider/hosts de produção sem autorização. Quando indisponíveis, construir fixtures e registrar contrato PASS se executado, mantendo o caso real NOT_RUN. Um relatório de CI verde com testes reais não executados não autoriza alegar suporte nesses ambientes.

A campanha N13/C11 pode começar assim que N12, C10 e K11 entregarem seus artefatos. Nenhuma fase aguarda que a outra conclua a mesma campanha. Reutilizar a mesma evidência, não dois resultados conflitantes com SHAs diferentes.

---

# Anexo C — Fontes, baseline e natureza das decisões

## C.1. Referências históricas da revisão 2

**[R01]** GitHub, branch `feature/v0.2.0`: HEAD `7ed52c22865a92c3768bc32508ed9e35dc5efdc3`, registrado como confirmado na revisão 2 em 25/09/2026. A revisão 3 não consultou novamente o repositório; o Codex deve conferir o HEAD efetivo.
https://api.github.com/repos/OktoLabsAI/okto-nexus/branches/feature/v0.2.0

**[R02]** `pyproject.toml` no SHA de referência: Python >=3.11; dependência MCP `>=1.0,<2`; extras serve/serve-lite; CLI e metadados do projeto. Não mudar SDK major ou licença como efeito da extração.
https://github.com/OktoLabsAI/okto-nexus/blob/7ed52c22865a92c3768bc32508ed9e35dc5efdc3/pyproject.toml

**[R03]** `application/auth.py`: AgentKeyAuthService, emissão/rotação, hash, retorno de plaintext uma vez, resolvedor e invalidação. Importação de chave existente e tickets derivados são decisões de projeto; não supor que já estejam implementados.
https://github.com/OktoLabsAI/okto-nexus/blob/7ed52c22865a92c3768bc32508ed9e35dc5efdc3/src/okto_nexus/application/auth.py

**[R04]** `application/identity.py`: identidade/sessões/workspaces e caminho legado de resolução física do project_root. As demais âncoras de runtime/UI vieram da revisão anterior e precisam ser reproduzidas pelo Codex no HEAD efetivo.
https://github.com/OktoLabsAI/okto-nexus/blob/7ed52c22865a92c3768bc32508ed9e35dc5efdc3/src/okto_nexus/application/identity.py

**[R05]** Materiais anteriores fornecidos nesta conversa e relidos a partir de seus arquivos: `PLANO_01_NEXUS_CONEXOES_NATIVAS_E_DISTRIBUIDAS_CODEX.md` e `PLANO_02_NEXUS_CONNECTOR_CODEX.md`, contrato `nxl-1-draft-2026-09-24-r1`. Foram usados como base/crosswalk, não como autoridade acima das decisões posteriores. Esta revisão substitui o desenho de dois projetos, o runtime package dentro do Connector e onboarding não centrado na credencial do agente.

## C.2. Referências técnicas primárias registradas na revisão 2

**[R06]** Documentação oficial do Codex App Server, consultada em 25/09/2026. O endereço redirecionou para documentação oficial em ChatGPT Learn. Usar a versão instalada e schemas do binário como parte da qualificação; não prometer todos os recursos descritos para versões antigas.
https://developers.openai.com/codex/app-server/
https://learn.chatgpt.com/docs/app-server

**[R07]** Documentação RPC do Pi no repositório oficial atual `earendil-works/pi`; o antigo endereço `badlogic/pi-mono` redireciona. Arquivo consultado pelo GitHub, blob `ad6ff90e80ba89eb2a42f226c1cbd46bc1818a2f`: framing LF, IDs assíncronos, eventos e shutdown. Fixar commit/versão usados nos testes de implementação.
https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/rpc.md

**[R08]** Documentação oficial Claude Code, uso programático e saída stream-json, consultada em 25/09/2026. Controles/approvals devem ser verificados na interface e versão qualificada, não inferidos da saída em streaming.
https://code.claude.com/docs/en/headless

**[R09]** Especificação MCP versionada 2025-11-25, usada para Streamable HTTP. A presença de stdio no padrão não autoriza MCP stdio nestes produtos; r3 o proíbe. Referência deliberadamente versionada para compatibilidade, não afirmação de que é a revisão mais recente. O plano não migra o protocolo/SDK da base implicitamente.
https://modelcontextprotocol.io/specification/2025-11-25/basic/transports

**[R10]** RFC 6455, WebSocket. NXL é decisão de protocolo do produto sobre WSS, não parte do padrão MCP.
https://www.rfc-editor.org/rfc/rfc6455

**[R11]** Decisão explícita do usuário nesta conversa, 25/09/2026: remover MCP stdio do Nexus e qualquer MCP no Connector; clientes MCP acessam diretamente o Nexus Server por HTTP. A revisão 3 corrige documentos e backlogs, sem nova auditoria de código ou consulta às fontes externas. Essa decisão prevalece sobre instruções de compatibilidade stdio/fachada da revisão 2.

## C.3. O que é especificação, não fato implementado

Nomes/rotas novos, comandos CLI, modelos, API do Core, tickets, defaults, budgets, schemas a gerar, fases, testes e métricas são decisões propostas e requisitos de implementação. Nenhum benchmark, teste de provider, execução multi-host, publicação PyPI ou alteração em repositório foi realizado ao escrever estes planos. A validação desta entrega verifica consistência dos documentos/backlogs, não funcionamento do produto.

Se o HEAD mudou, o Codex registra a diferença e aproveita código correto posterior; não restaura a branch ao SHA deste documento. Conflitos com limites reais de protocolo devem produzir ADR, capacidade explícita e teste, não uma promessa inexequível ou remoção silenciosa de requisito.
