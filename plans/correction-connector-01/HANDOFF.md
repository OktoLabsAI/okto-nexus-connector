# CN1 Handoff — contrato entre os três agentes (CN-00.04)

**Versão exata:** Connector 0.1.0.dev0 (pós-CN1) · Core 0.2.10.dev0
(wheel sha256 `4cde3b9a…50da0`) · Server: a adaptar (r3).

## Owners

| Domínio | Owner | Nota |
|---|---|---|
| Protocolos nativos, qualificação, catálogo, disponibilidade, instalação-refs, journal/kernel | **Core** | Consumido SOMENTE pela API pública (`create_runtime`, `get_runtime_catalog`, `evaluate_runtime_availability`, `resolve_installation`, codecs/reducers). Nenhuma allowlist/paralelo no Connector. |
| Transporte autenticado (WSS/HTTPS), ciclo do host, cofre, IPC, lifecycle, receipts locais | **Connector** | Envelope NXL validado antes de qualquer ExecutionContext; geração do welcome adotada; namespace originário em reconcile. |
| Grants, intenção canônica, decisão de binding, inbox/outbox/handoffs, projeção de UI, MCP HTTP endpoint | **Server** | Pendente de adaptação r3 — gate externo do CN-08.03. |

## Mensagens compartilhadas (schemas reais, não mocks)

- Frames NXL r3 (`contracts/nxl/v1` do Core): os 20 tipos validados pelo
  codec embarcado; `operation.submit` exige `intent_hash` semântico;
  `reconcile.report` com receipts/snapshots tipados.
- Rotas A.5 (`/v1/connections/*`, `/v1/runtime/*`): o peer de contrato do
  Connector (`tests/fakes/http_peer.py`) espelha o contrato do plano;
  divergências reais do Server devem ser acordadas aqui antes do efeito.
- Idempotência HTTP: mutações que perdem a resposta após entrega são
  `OUTCOME_UNKNOWN` (não retry-safe); o Server deve expor consulta por
  `operation_id` (GET /v1/runtime/operations/{id}) para o mesmo intent.

## Dúvidas de contrato resolvidas / pendentes

1. **Confirmacao de attach**: sem frame de ack no r3; o Connector marca a
   lane pronta quando o attach é efetivamente escrito e o Server recusa
   operações de lanes não admitidas (semântica de escopo é do Server).
   *Pendente do Server real confirmar ack explícito em revisão futura.*
2. **Geração de conexão**: o Server autoriza a geração no welcome; o
   contador local NUNCA é autoridade (adotado verbatim).
3. **Remote runtime.open**: fica UNSUPPORTED no Connector até a campanha
   vertical autorizada — o Server não deve enviar antes desse gate.

## Não-negociável

Nenhum MCP no Connector/Core; stdio nativo permitido; nenhuma conta de
usuário Nexus; nenhuma segunda inbox; segredos só por entrada protegida;
autoridade sempre do Server.
