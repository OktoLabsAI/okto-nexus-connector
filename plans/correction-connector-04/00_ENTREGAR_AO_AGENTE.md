# Entregar o pacote inteiro ao executor

O ZIP desta revisão contém **testes executáveis**, não apenas Markdown. Extraia preservando `regressoes/` e `executar_verificacao.py`. A última evidência do executor informa novamente que recebeu somente documentos e reconstruiu as sementes; não repetir essa perda de material.

## Prompt

```text
Leia 01_RELATORIO_REAVALIACAO.md, 02_PLANO_CORRECAO_CN4.md e
03_MATRIZ_ACEITE.md deste pacote. Trabalhe no HEAD atual do Connector,
sem reset e sem descartar alterações posteriores ao e930797.

Implemente CN4-00 a CN4-06. Preserve as 12 sementes originais CN3 que
agora passam e as correções anteriores. Priorize namespace/ordem da
aprovação; corrija a tradução do vocabulário no mesmo fluxo, jamais
aplicando no Core antes de confirmação canônica do Server.

Corrija o caller do ACK para o alvo do lote; a identidade sem perda do
diretório MCP; o pipeline de candidato completo e revisão de evidência;
e a rotação/reattach de lanes pelo reload real. Não duplicar catálogo,
qualificação, adaptadores ou protocolos do Core. MCP segue HTTP direto.

Execute o runner recebido antes/depois e duas vezes ao concluir.
Este snapshot apresenta 11 FAIL e 3 controles PASS em CN4, e 12 PASS
nas sementes originais CN3 adaptadas. Não alterar expectativas para
ajustar ao defeito; mudanças de fixture exigem justificativa causal.

Diferencie a sentinela de ordem de aprovação de uma prova no Core:
a implementação real hoje recusa approve/deny por contrato inválido;
não se alegou bypass completo de provider. Feche ambos os problemas,
com teste atravessando a aplicação e o Core após confirmação do Server.

Entregue correções, commits/diff, logs/XML, hashes dos artefatos, matriz
e limites. Não responder só com outro plano. Pendências reais de
Server/provider permanecem BLOCKED por owner, não DONE implícito.
Não publicar, fazer push, usar credenciais reais nem alterar agentes.
```

## Rodar na fonte

```bash
python executar_verificacao.py --repo /caminho/do/okto-nexus-connector --output /caminho/das/evidencias
```

## Rodar no wheel instalado

```bash
python executar_verificacao.py --output /caminho/das/evidencias-wheel
```

Requer as dependências instaladas no ambiente, incluindo Core 0.2.10.dev0 e pytest/pytest-asyncio. O runner não faz instalação, rede, push, edição do produto ou remoção de gates de contenção. Retorno diferente de zero é esperado no snapshot ainda não corrigido.

**Os wheels em artefatos/ são do Connector auditado, NÃO corrigidos.** Não utilizá-los como release de resolução dos achados.
