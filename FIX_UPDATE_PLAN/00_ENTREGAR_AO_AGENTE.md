# Instruções ao agente executor — Connector CN1

Entregue o diretório completo, não somente os Markdown. O arquivo `regressoes/test_connector_audit.py` contém 34 casos executados: no snapshot, 30 falham, três controles positivos passam e uma observação de buffer passa. O teste de observação confirma o comportamento histórico, não é um critério de aceitar buffer ilimitado: depois da correção, substituí-lo por prova de limite e replay, conservando a evidência anterior.

## Como começar

Use o ambiente de desenvolvimento do Connector com o Core real. Execute:

```bash
python executar_verificacao.py --repo /caminho/okto-nexus-connector --output /caminho/evidencias/baseline
```

O runner não instala dependências, não altera o produto e não habilita providers falsos. Sem `--repo`, testa o pacote instalado. Para a suíte fornecida pelo projeto:

```bash
python -m pytest -q --ignore=tests/e2e/test_packaging.py --junitxml=/caminho/evidencias/suite.xml
```

O ignore acima documenta a campanha desta auditoria, não uma dispensa permanente do packaging. O agente deve executar os testes de packaging e a instalação limpa com acesso ao wheel exato do Core. Não depende de um clone irmão implicitamente nomeado.

## Prompt pronto

```text
Leia 01_RELATORIO_AUDITORIA.md, 02_PLANO_CORRECAO_CN1.md, achados.json,
backlog.json e a matriz. A referência auditada é Connector 0.1.0.dev0,
commit 8bc50259dc7eb353f349f1eb762be2d33cc393de. Trabalhe no HEAD atual,
sem reset e sem perder mudanças posteriores.

Implemente as correções e funcionalidades pendentes do CN1, não produza
outro plano substituindo a execução. Comece reproduzindo as 34 verificações
e registre o ambiente. Preserve as condições causais: frame com hash válido
mas scope inválido, Core real nas fronteiras necessárias, fila com dois puts,
stop unknown, timeout após POST possivelmente recebido, escolha entre A/B.
Não enfraqueça asserts nem habilite capacidades para fazer fakes passar.

A arquitetura é a do plano R3: agente no centro da identidade; Core único
para runtimes/catálogo/qualificação; Connector hospeda e autentica suas
operações; Server decide autorização e mantém domínio canônico. MCP HTTP
direto entre harness e Server, sem MCP stdio/servidor/proxy no Connector.

Atualize o Core por versão/hash verificável. O último código fornecido ao
revisor foi 0.2.9 e o ajuste C11 ainda era plano; confirme a API efetivamente
entregue antes de consumi-la. Não crie uma identidade paralela da instalação
nem replique a allowlist para contornar lacunas. Corrija perda da arquitetura
do candidato e seleção matches[0].

Priorize autoridade/gerações, namespaces, TLS e incerteza de HTTP, filas e
controle urgente, recovery e lifecycle, wiring de MCP direto/Pi/approvals,
e só então feche UI/CLI/serviços. Não declarar DONE para uma integração
que existe só num teste que importa o Core e não passa pelo Connector.

Entregue commits, diff, testes/saídas, hashes dos artefatos, mudanças de
schema/API, relatório de migração e evidência por tarefa. Separe bugs
reproduzidos, inspeção, provider real, backend do SO e integração entre hosts.
Não publique pacotes nem use credenciais reais sem autorização.
```

## Entregáveis

Versão exata do Connector/Core; planos/matriz com status real; resultados antes/depois; runner e testes novos; codecs/recibos válidos; instruções de migração; caminho público de recuperação; integração vertical com Server quando autorizada. Não compartilhar segredos nos logs.
