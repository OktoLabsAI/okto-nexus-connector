# Entrega ao executor — CN5

Levar **o ZIP inteiro**, não apenas os Markdown. Verificar a presença de `regressoes/test_cn4_review.py`, `regressoes/test_cn5_review.py`, `regressoes/test_cn3_review.py`, `regressoes/cn2_helpers.py` e `executar_verificacao.py` antes de iniciar. Nesta rodada eles estão realmente incluídos.

## Prompt de execução

```text
Trabalhe no HEAD atual de okto-nexus-connector. O snapshot auditado é
87b8fd2e3e403cb6a70a1ce265618e29c7a6b86c, versão 0.4.0.dev0,
com nexus-connector-core 0.2.10.dev0. Não faça reset nem apague mudanças.

Leia 01_RELATORIO_REAVALIACAO.md e execute executar_verificacao.py antes
 de editar. Leia integralmente 02_PLANO_CORRECAO_CN5.md e siga as
assinaturas, DTOs, estados, transições e critérios definidos ali.
Implemente CN5-00 a CN5-05. Não devolva outro plano no lugar do código.

Prioridade: Q01a e Q01b conjuntamente. Não remova redaction global;
separe proposta operacional de projeção. Não passe session_id isolado
na aplicação nativa e não use a sessão de outro Server para completar
a autorização. Depois, mantenha o produtor da decisão independente
do waiter, reprograme attach após rotação em voo e reinicie publicadores
de eventos após falha transitória/reconexão.

Não substitua o teste que chama _on_remote_approval por preenchimento
direto de _approvals. Não mude os hashes da proposta para obter PASS.
Não solte as barreiras dos testes antes da observação. Registre cada
mudança de fixture com justificativa e preserve o comportamento.

Execute fonte e wheel instalado. Entregue commits/diff, teste/node,
comando, exit code, XML, hashes, matriz e pendências externas. Não
publique/push nem utilize credenciais reais. Não implemente MCP stdio,
proxy ou adapters no Connector. O Core permanece a fonte única.
```

## Execução das reproduções

```bash
python executar_verificacao.py --repo /caminho/do/okto-nexus-connector --output /caminho/evidencias-cn5
```

Sem `--repo`, o runner testa o Connector já instalado no ambiente. Ele não instala dependências, não faz rede, não altera o produto e não remove os gates de contenção. O baseline tem 6 FAIL / 14 PASS no conjunto principal. Um exit code não zero é esperado antes da correção. `--historical` acrescenta uma campanha separada com 12 sementes CN3.

## Sobre a fonte de dependências

Usar o Core real pinado. Os dois módulos `rfc8785` da revisão foram obtidos upstream e verificados por hash; isso não é instrução para criar stub no repositório. O pacote CN5 não inclui fontes de dependências ou fontes de produto para substituir seu checkout.
