# CN2 Operations Inventory (CN2-00.02) — update over CN1

| Entrada | Origem autenticada | SessionKey | Fila/validação pós-espera | Método Core | operation_id | Receipt | Recovery |
|---|---|---|---|---|---|---|---|
| WSS turn.submit/steer | receiver DTO (envelope completo, lane epoch) | por namespace do wire | revalidação de lane pós-semáforo | submit/control(steer) | do frame | operation.receipt mesmo ID | journal |
| WSS turn.interrupt/close | DTO; classe CONTROLE (pooled reservado) | idem | idem | control/close | do frame | idem | idem |
| WSS runtime.open | — | — | — | — | — | UNSUPPORTED declarado (gate vertical) | — |
| WSS approval.request | lane válida | — | fila HITL bifásica | decide_native_approval (rota completa) | CLI/Server | receipt | Core containment |
| event.ack | conexão autenticada + stream conhecido + watermark ≤ escrito | por stream | validação antes de acknowledge_events | acknowledge (Journal port) | — | — | replay do ACK validado |
| reconcile.request/.report | namespace originário | por namespace | estado pending/failed/complete | Journal.get_receipt/claimed_sessions (sem binário) | do frame/request | tipado codec-válido | journal |
| IPC runtime.* | token IPC + identidade | SessionKey tipado | — | Core público | Server (resolve) | receipt | journal/reconcile |

Frames não-produtivos (heartbeat/negociação/detach): tratados inline no
receiver; nunca geram tasks produtivas. Tasks duráveis (pumps, lease,
attach de lane) pertencem ao manager/transporte e sobrevivem à perda de
um cliente IPC; nada sobrevive à morte do processo exceto journal/state.
