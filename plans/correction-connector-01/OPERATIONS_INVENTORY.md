# CN1 Operations Inventory — anunciado vs implementado (CN-00.03)

Matriz entrada → escopo exigido → operação NXL → método Core → fonte do
operation_id → receipt → recovery. Capacidades sem wiring ficam
expressamente indisponíveis (não anunciadas).

## IPC (CLI → daemon)

| Entrada IPC | Scope exigido | Método Core | operation_id fonte | Receipt | Recovery |
|---|---|---|---|---|---|
| `ping` / `status` | IPC token | — | — | — | — |
| `runtime.start` | alias→binding + identidade + Server (intents:resolve) | prepare→open (create_runtime) | Server (resolve) | open receipt | journal/reconcile |
| `runtime.submit` | idem | submit | Server (resolve) | receipt | idem |
| `runtime.interrupt` | idem | control(interrupt) | Server (resolve) | receipt | idem |
| `runtime.stop` | sessão gerenciada | close | local (StopAttempt) | receipt; unknown mantém sessão | retry/2º lifecycle |
| `runtime.inspect/logs` | sessão | inspect / events | — | — | journal replay |
| `runtime.approval.decide` | sessão + request pendente | decide_native_approval | CLI | receipt | Core containment semantics |
| `approvals.pending/decide` | binding + autoridade Server | HTTP approval-decisions (CAS) | Server | applied | CAS |
| `reconcile` | namespace autenticado | reconcile | — | typed report | journal |
| `state.reload` | IPC token | — | — | — | — |
| `shutdown` | IPC token | shutdown por lifecycle owner | — | relatório honesto | pendências conservadas |

## WSS (Server → daemon)

| Operação NXL | Estado exigido | Método Core | IDs | Receipt | Sem wiring |
|---|---|---|---|---|---|
| `operation.submit: turn.submit` | welcome+reconcile+lane READY + envelope exato (server/executor/agent/binding/geração/revisão) + hash válido + grant autorizado | submit | do frame (preservado) | operation.receipt (mesmo ID) | — |
| `operation.submit: turn.steer` | idem + expected_turn_id quando o adapter exige | control(steer) | do frame | receipt | — |
| `operation.submit: turn.interrupt` | idem (containment permitido além do deadline) | control(interrupt) | do frame | receipt | — |
| `operation.submit: runtime.close` | idem | close_remote | do frame | receipt do MESMO intent | — |
| `operation.submit: runtime.open` | — | — | — | — | **UNSUPPORTED até o gate vertical** (erro tipificado com ação) |
| `reconcile.request` | negociado | reconcile (namespace originário) | — | reconcile.report tipado | — |
| `approval.request` | lane válida | fila HITL (decisão via CAS do Server) | request_id | — | — |
| `event.ack` | namespace/epoch correspondentes | acknowledge_events (pós-ACK durável) | — | — | — |

## Entradas que NÃO existem (proibidas/não anunciadas)

- Qualquer servidor/proxy/relay MCP (qualquer transporte).
- Remote open (até CN-08.03), attach (registered_unqualified no catálogo).
- Login de usuário Nexus; coleção de segredos por varredura.
