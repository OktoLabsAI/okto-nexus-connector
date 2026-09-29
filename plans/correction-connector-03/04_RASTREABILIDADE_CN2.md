# Rastreabilidade de fechamento do CN2

| Achado | Situação | Evidência/limite |
|---|---|---|
| N01 | Parcial | Namespace da operação e detach na fila passaram; campos de geração/revisão ainda se perdem (CN3-01). Consultas não produtivas precisam de namespace autenticado (CN3-02). |
| N02 | Parcial | Controles reservados e limite anterior à task funcionam; liberar custo diferente do cobrado deixa quota residual (CN3-03). |
| N03 | Parcial | Reader finito, replay sem ACK e ACK estrangeiro/futuro passaram; duplicata válida bloqueia espera (CN3-05). Startup e aplicação de ACK precisam de recovery completo. |
| N04 | Parcial | Falha explícita de reconcile não gera READY, e binário removido com journal aberto é suportado; host novo ainda ignora história (CN3-04). |
| N05 | Corrigido nos cenários CN2; qualificação pendente | FAILED sem efeito e CoreError não destroem/prendem sessão nos testes adaptados. Saída do daemon com unknown exige política de ownership e teste próprio. |
| N06 | Parcial | URL é renderizada em arquivo e método de decisão tem assinatura correta. Config conflita entre Servers (CN3-06). Aplicação final de decisão/callbacks/open remoto continuam pendentes. |
| N07 | Parcial | Credencial bootstrap e live-add de lane passaram. Expiração/rotação/snapshot de autoridade não são ciclo completo (G02/CN3-01). |
| N08 | Corrigido no cenário CN2 | Override ws remoto é recusado antes do ticket/conexão. Nenhum novo defeito TLS foi alegado nesta rodada. |
| N09 | Parcial | API Core é chamada por helpers novos, mas wrappers não estão conectados ao fluxo Connector. Evidência de publish completo é prematura (G01). |
