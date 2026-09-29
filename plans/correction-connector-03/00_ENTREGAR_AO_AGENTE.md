# Entrega ao agente executor — CN3

Copiar **o ZIP inteiro**, incluindo `regressoes/`, runner e evidências, para uma pasta de trabalho do Connector. O pacote não é uma versão corrigida: os artefatos em `artefatos/` são builds do snapshot auditado, sem patches. Não instalar como atualização corrigida.

Baseline `235439242e4de8edc7ca8940b80a331275be9fb6`, Connector `0.2.0.dev0`, Core `0.2.10.dev0`. Os 127 arquivos originais ficaram intactos. Trabalhar no HEAD real; mudanças posteriores não devem ser apagadas.

## Prompt

```text
Leia 01_RELATORIO_REAVALIACAO.md, 02_PLANO_CORRECAO_CN3.md,
03_MATRIZ_ACEITE.md e os testes deste pacote completo.
Implemente CN3-00 a CN3-07 no HEAD atual, sem reset.

Priorize: preservar gerações/revisões aprovadas até o Core; impedir
reconcile de namespace estrangeiro; recuperar o journal num host
recém-iniciado. Corrija a contabilização de admissão, ACK duplicado
válido e isolamento dos diretórios efêmeros de MCP por namespace.

Mantenha os controles que já passam. Não há bug novo de intent_hash:
o codec real recusa adulteração. Não acrescente validação paralela
como substituto de corrigir o contexto que o host fornece ao Core.

G01/G02 documentam wiring incompleto do CN2. Ligue o inventário técnico
à aplicação, a decisão autorizada ao Core e os verbos/callbacks/tickets
prometidos, ou registre precisamente o bloqueio externo antes de
anunciar capacidade. Um helper sem caller não encerra uma tarefa.

Execute as sementes, preserve causalidade e registre XML/versões/hashes.
Não crie MCP stdio, proxy/relay, usuário Nexus, outra inbox ou cópia
de adapters/qualificadores. Não desligue preflight para passar no
sandbox. Separe teste de peer, backend do SO, provider e dois hosts.

Entregue implementação/diff/artefatos, não outro plano no lugar da
execução. Não publique, faça push ou use credenciais reais sem
instrução específica. Termine com gate/escopo, pendências por owner
e caminhos de recuperação públicos, não apenas contagem verde.
```

## Execução

Com as dependências já instaladas, inclusive Core `0.2.10.dev0`:

```bash
python executar_verificacao.py --repo /caminho/okto-nexus-connector --output /caminho/evidencias --include-executor-cn2
```

Para testar o wheel previamente instalado em vez do checkout:

```bash
python executar_verificacao.py --installed --output /caminho/evidencias-wheel
```

Baseline esperado da campanha nova: **9 FAIL e 3 PASS**. CN2 preservado: **20 PASS**. São campanhas sobrepostas; não somar repetições como cobertura adicional. O runner não instala dependências, não baixa repositórios, não altera fontes, não publica e não executa providers. Usa temporários próprios para fixtures.

`cn2_helpers.py` conserva helpers do pacote anterior; não é coletado como suíte. `original_cn2.txt` é a semente original apenas para rastreabilidade. `evidencias/cn2_fixture_changes.diff` explica a adaptação à API nova. Caso uma assinatura mude legitimamente na correção, adaptar a fixture mantendo a mesma espera, origem, efeito contado e expectativa.

Os resultados de qualificação do executor anterior não são execuções desta auditoria. O Core desta revisão foi construído de fonte oficial verificada, não do mesmo binário de wheel daquele executor. Ver `evidencias/PROVENIENCIA.json`.
