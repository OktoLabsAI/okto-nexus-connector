# Verificação de wiring no código da aplicação

Fonte: 235439242e4de8edc7ca8940b80a331275be9fb6. Ocorrências textuais em src/; revisar juntamente com chamadas e composição. Não inclui testes.

## `availability_snapshot`
- `src/okto_nexus_connector/services/discovery_service.py:268` — `def availability_snapshot(candidates) -> dict[str, object]:`

## `evaluate_availability`
- `src/okto_nexus_connector/services/discovery_service.py:261` — `def evaluate_availability(candidates):`
- `src/okto_nexus_connector/services/discovery_service.py:277` — `report = evaluate_availability(candidates)`

## `resolve_selection`
- `src/okto_nexus_connector/services/discovery_service.py:293` — `def resolve_selection(candidates, adapter_id: str, candidate_ref: str):`

## `decide_native_approval`
- `src/okto_nexus_connector/services/runtime_service.py:542` — `async def decide_native_approval(self, session_id: str,`
- `src/okto_nexus_connector/services/runtime_service.py:550` — ```decide_native_approval`` with the pending request projection;`
- `src/okto_nexus_connector/services/runtime_service.py:566` — `receipt = await runtime.decide_native_approval(`

## `inventory_revision=`
- `src/okto_nexus_connector/services/connect_service.py:171` — `inventory_revision=(state.schema_version << 8) | 1,`

## `_expires`
- `src/okto_nexus_connector/daemon/app.py:338` — `ticket, _expires = await http.binding_ticket(key, binding_id)`
- `src/okto_nexus_connector/daemon/app.py:353` — `ticket, _expires = await http.binding_ticket(`

## `pi_native_action`

## `codex_resume`

## `native_approvals_enabled`
