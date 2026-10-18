"""Release only capacity whose original lease refusal proves no native opening."""
import asyncio
import time

from nexus_connector_core import OperationKey, SessionKey, project_r4_bound_receipt

from ..errors import ConnectorError


async def release_denied_opening_capacity(publications, journal, ledger, *,
                                          server_id, executor_id, operation_id,
                                          boot_id, require_current, clock=time.monotonic):
    require_current()
    key = OperationKey(server_id, executor_id, operation_id)
    rows = await asyncio.to_thread(publications.lookup, server_id, executor_id, [operation_id])
    row = rows.get(operation_id)
    if row is None or row['refusal'] is None:
        return False
    refusal = row['refusal']
    denial = refusal.get('denial', {})
    request = denial.get('request', {})
    if (row['metadata']['action'] != 'runtime.open' or
            denial.get('type') != 'lease.denied' or request.get('purpose') != 'initial' or
            request.get('expected_lease_serial') != 0):
        return False
    binding = row['binding']
    if binding is None or refusal['binding'] != binding:
        raise ConnectorError('SCOPE_MISMATCH', 'denied_capacity',
                             'The original lease refusal binding changed.')
    source = binding['source']
    scope = refusal['scope']
    if (source['operation_id'] != operation_id or
            (source['server_id'], source['executor_id']) != (server_id, executor_id) or
            any(source[name] != scope[name] for name in ('server_id', 'executor_id',
                'agent_id', 'binding_id', 'session_id')) or
            source['connection_generation'] != request['connection_generation'] or
            request['scope'] != scope):
        raise ConnectorError('SCOPE_MISMATCH', 'denied_capacity',
                             'The refused opening scope changed.')
    session = SessionKey(server_id, executor_id, source['session_id'])
    fact = await journal.get_receipt(key)
    if (fact is None or fact.stage != 'FAILED' or fact.possible_effect or not fact.retry_safe or
            fact.error_code != 'LEASE_REVALIDATION_REQUIRED' or
            await journal.get_session_claim(session) is not None or
            await journal.get_process_launch(session) is not None):
        raise ConnectorError('RECONCILIATION_REQUIRED', 'denied_capacity',
                             'The opening has no verified native-absence proof.')
    revision = row['receipt']['receipt_revision'] if row['receipt'] is not None else 1
    projected = project_r4_bound_receipt(binding, fact, key=key, receipt_revision=revision)
    if row['receipt'] is not None and projected != row['receipt']:
        raise ConnectorError('OPERATION_CONFLICT', 'denied_capacity',
                             'The retained refusal receipt changed.')
    capacity = await ledger.capacity_for_operation(key, action='runtime.open',
                                                   now=clock(), boot_id=boot_id)
    if capacity is None:
        return False
    if (capacity.request.unit != 'new_session' or capacity.consumption is None or
            capacity.consumption.opening_operation_id != operation_id or
            capacity.consumption.opening_intent_hash != source['intent_hash'] or
            any(getattr(capacity.request.scope, name) != scope[name] for name in (
                'server_id', 'executor_id', 'agent_id', 'binding_id', 'session_id',
                'session_owner_generation')) or
            capacity.request.scope.connection_generation != source['connection_generation']):
        raise ConnectorError('SCOPE_MISMATCH', 'denied_capacity',
                             'The capacity belongs to another opening.')
    require_current()
    released = await ledger.release_capacity_slot(capacity.request, capacity.reservation_id,
                                                  now=clock(), boot_id=boot_id)
    if released.state != 'RELEASED':
        raise ConnectorError('RECONCILIATION_REQUIRED', 'denied_capacity',
                             'The original capacity remains owned.')
    return True


async def recover_denied_opening_capacity(reporter, journal, ledger, request):
    """Bounded replacement-owner scan before the general owned-slot fence."""
    after, high = 0, None
    for _ in range(32):
        page = await ledger.owned_slot_page(after_rowid=after,
            high_water_rowid=high, limit=128)
        high = page.high_water_rowid
        for slot in page.reservations:
            if (slot.key.server_id, slot.key.executor_id) != (reporter.server_id, reporter.executor_id):
                continue
            opening_id = await ledger.capacity_opening_for_slot(slot)
            if opening_id is None:
                continue
            await release_denied_opening_capacity(reporter.publications, journal, ledger,
                server_id=reporter.server_id, executor_id=reporter.executor_id,
                operation_id=opening_id, boot_id=reporter.boot_id, clock=time.monotonic,
                require_current=lambda: reporter.require_authority(request))
        if page.next_after_rowid is None:
            return
        if page.next_after_rowid <= after:
            raise ConnectorError('JOURNAL_UNAVAILABLE', 'denied_capacity',
                                 'The capacity cursor did not advance.')
        after = page.next_after_rowid
    raise ConnectorError('RECONCILIATION_REQUIRED', 'denied_capacity',
                         'The denied-capacity scan exceeded its page budget.')
