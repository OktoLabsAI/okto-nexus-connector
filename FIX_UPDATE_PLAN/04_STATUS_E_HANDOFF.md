# Status CN3 e handoff de integração

| Parte | Parecer desta revisão |
|---|---|
| CN3 gerações/revisões e entrada reconcile | Sementes originais passaram; conservar |
| CN3 quota exata | Passou; reserva passou a conservar custo exato |
| Histórico a frio | Passou após adaptação do double à API ensure_history_journal |
| ACK duplicado no helper | Passou; caller EventBridge não usa target e precisa P02 |
| Namespace de config com IDs curtos | Passou; perda por truncamento em IDs válidos longos exige P03 |
| Aprovação pela aplicação | Novo caller existe; P01 demonstra contrato incompatível, origem errada e ordem a corrigir |
| Catálogo do Core | Centralizado; não criar lista autoritativa nos aplicativos |
| Disponibilidade CLI/IPC | Ligada, mas reconstrução incompleta do candidato exige P04 |
| Snapshot/resolvedor remoto | Contrato/consumo completo ainda não demonstrado; resolve_selection sem caller produtivo |
| Rotação de lane | Teste por atribuição direta funciona; reload real e reattach precisam P05 |
| runtime.open remoto | Explicitamente UNSUPPORTED; contrato do Server pendente |
| Callbacks Pi/resume/approvals opt-in | Composição produtiva ainda declarada pendente |
| Intenção estável e publicação de recibo | Contrato final com Server pendente; flag de timeout não encerra o requisito |
| Saída com unknown | Exit code 1 não comprova transferência de supervisão; qualificar política ou manter drain/recovery |

## Decisão operacional

Desenvolvimento e integração de Server/Connector podem continuar com pin exato. O escopo completo de executor governado não está homologado. Homologar as rotas corrigidas de laboratório somente após as regressões passarem e a matriz declarar as capacidades ainda indisponíveis. Providers/SO/UI/topologia real requerem campanhas próprias.

Aprovação e binding mantêm identidade canônica do agente no Server. O Core continua biblioteca única dos runtimes. O Connector não oferece MCP stdio/HTTP/proxy e não pode corrigir esses gaps copiando codecs, listas de modelos ou regras de qualificação.
