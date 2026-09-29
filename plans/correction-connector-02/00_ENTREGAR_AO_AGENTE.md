# Entrega ao executor — CN2

Levar o ZIP completo, com `regressoes/`, relatório, plano, matriz e runner. Não basta copiar Markdown ou reconstruir os casos a partir das mensagens de erro.

**Snapshot:** Connector `28d147625c2a47417ad70f72d3b453cd8bd2e984`, `0.1.0.dev0`, dependência `nexus-connector-core==0.2.10.dev0`. Trabalhar no HEAD real, sem reset. O CN1 permanece referência; CN2 completa suas lacunas, não reinventa a arquitetura.

## Prompt pronto

```text
Leia 01_RELATORIO_REAVALIACAO.md, 02_PLANO_CORRECAO_CN2.md e
03_MATRIZ_ACEITE.md. Implemente as 32 tarefas CN2 sobre o HEAD atual,
preservando alterações posteriores. Use o Core pinado e APIs públicas.

Copie o pacote completo, não apenas os Markdown. Execute o runner antes de
alterar: neste snapshot são 16 FAIL / 4 PASS nos 20 casos CN2, sendo três
controles positivos e uma observação de carga, mais 14 sementes originais
CN1 preservadas com PASS. Não some subconjuntos/repetições. Os 24 casos
complementares da matriz não foram executados pelo revisor e não são
alegações de defeitos adicionais.

Priorize namespace fim a fim, invalidação durante fila, capacidade de
controle, eventos/ACK/replay, reconciliação e WSS seguro. Corrija também
FAILED/CoreError no stop, wiring de aprovação/MCP direto, tickets/lanes
e disponibilidade real do inventário. Faça o teste atravessar o aplicativo,
não só um helper ou a biblioteca Core isoladamente.

Não interpretar catálogo/import de API como integração concluída. O Core
0.2.10 já fornece get_runtime_catalog, evaluate_runtime_availability e
resolve_installation. Consumir seus contratos; não copiar qualificadores,
listas de runtimes ou parsers. MCP somente HTTP direto harness→Server.
Nenhum MCP stdio/proxy/relay no Connector. Identidade permanece no agente.

Não alterar expectativas para encerrar testes. Ajuste fixture somente
quando uma API mudou justificadamente e preserve a condição causal. Um
bloqueio de preflight do sandbox não autoriza remover contenção. Não
substituir rfc8785 por stub. Não presumir que falta de Server real impede
implementar o lado Connector contra um peer de contrato acordado.

Entregue commits/diff, matriz por requisito, comandos/XMLs/hashes,
transições, mudanças públicas e recuperação documentada. Se remote open,
Pi bridge ou aprovação ainda não estiverem ligados, declare a pendência
explicitamente. G0/G1/G2/G3 têm evidências diferentes. Nenhuma publicação,
push ou uso de credenciais reais é autorizado por este plano.
```

## Execução

No ambiente que já possui as dependências declaradas do projeto:

```bash
python executar_verificacao.py --repo /caminho/do/okto-nexus-connector --output /caminho/das/evidencias
```

O runner não instala nada, não edita o produto, não abre providers e não
remove gates. Ele usa `src` do checkout selecionado e verifica o pin do Core.
É esperado um retorno não zero antes das correções. `runner.json` registra
comandos/ambiente; XML/log mostram resultados. A observação de 300 tarefas é
um registro de comportamento, não uma imposição arbitrária de 260 tarefas.
A nova implementação deve expor e comprovar seus próprios limites finitos.

## Escopo do pacote

`artefatos/` contém wheel/sdist do Connector construídos **antes das correções**,
para reprodução — não são uma release aprovada. A dependência foi instalada
a partir de código oficial verificado, mas o wheel do executor não estava
anexado; `CORE_PROVENIENCIA.json` explica a diferença. O código do produto
continua nos repositórios/uploads do usuário, não foi modificado nesta revisão.
