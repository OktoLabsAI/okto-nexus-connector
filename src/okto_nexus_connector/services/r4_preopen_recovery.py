"""Recover preopening publications through public Core journal/slot ports."""
import asyncio
import time

from nexus_connector_core import CoreError, OperationKey, SessionKey, R4_PREVIEW_REVISION
from nexus_connector_core.preopen_refusal_r4 import (
    prepare_r4_preopen_refusal, persist_r4_preopen_refusal, validate_r4_preopen_report,
)

from ..errors import ConnectorError


async def recover_preopen_publications(publications, host, request, *, boot_id, clock=time.monotonic):
    journal, ledger = await host.ensure_history_journal(), await host.ensure_ledger()
    reports, after = [], ''
    for _ in range(32):
        page = await asyncio.to_thread(publications.preopen_page, request['server_id'], request['executor_id'],
            generation=request['connection_generation'], after=after)
        if not page:
            return reports
        for row in page:
            metadata = row['metadata']
            after = metadata['operation_id']
            refusal = row['preopen_refusal']
            if refusal is None:
                source = {k: metadata[k] for k in ('server_id', 'executor_id', 'binding_id', 'agent_id',
                    'session_id', 'connection_id', 'connection_generation', 'operation_id', 'intent_hash')}
                source.update(protocol_major=1, contract_revision=R4_PREVIEW_REVISION)
                refusal = prepare_r4_preopen_refusal(source,
                    session_owner_generation=metadata['session_owner_generation'], prior_binding=row['binding'],
                    recovery={k: request[k] for k in ('connection_id', 'connection_generation')})
                await asyncio.to_thread(publications.prepare_preopen, refusal)
            key = OperationKey(metadata['server_id'], metadata['executor_id'], metadata['operation_id'])
            try:
                fact = await persist_r4_preopen_refusal(journal, refusal)
            except CoreError as error:
                if error.code == 'OPERATION_CONFLICT':
                    await asyncio.to_thread(publications.conflict_preopen, refusal)
                raise
            frame = await asyncio.to_thread(publications.fence_preopen, refusal, fact)
            session = SessionKey(key.server_id, key.executor_id, metadata['session_id'])
            if await journal.get_session_claim(session) is not None:
                raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_preopen', 'An opening claim requires its own recovery.')
            capacity = await ledger.capacity_for_operation(key, action='runtime.open', now=clock(), boot_id=boot_id)
            if capacity is not None and (capacity.request.unit != 'new_session'
                    or any(getattr(capacity.request.scope, name) != metadata[name] for name in (
                        'server_id', 'executor_id', 'agent_id', 'binding_id', 'session_id',
                        'connection_generation', 'session_owner_generation'))
                    or capacity.consumption is None
                    or capacity.consumption.opening_operation_id != key.operation_id
                    or capacity.consumption.opening_intent_hash != metadata['intent_hash']):
                raise ConnectorError('SCOPE_MISMATCH', 'r4_preopen', 'The capacity belongs to another opening.')
            # The positive admission fence excludes any delayed native open.
            # Release only slots belonging to this exact opening, never a
            # different operation/session or an already admitted native fact.
            for owner in (journal, ledger):
                slot = await owner.owned_slot_state(session)
                if slot is not None and not slot.released and slot.opening_operation_id == key.operation_id:
                    await owner.release_owned_slot(key, session.session_id)
            if capacity is not None:
                capacity = await ledger.release_capacity_slot(capacity.request, capacity.reservation_id,
                                                              now=clock(), boot_id=boot_id)
                if capacity.state != 'RELEASED':
                    raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_preopen', 'The original capacity has not been released.')
            journal_slot = await journal.owned_slot_state(session)
            if journal_slot is not None and not journal_slot.released:
                raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_preopen', 'Journal ownership remains unresolved.')
            slot = await ledger.owned_slot_state(session)
            slot_fact = None if slot is None else dict(server_id=slot.key.server_id,
                executor_id=slot.key.executor_id, session_id=slot.key.session_id,
                opening_operation_id=slot.opening_operation_id, released=slot.released)
            reports.append(validate_r4_preopen_report(dict(refusal=refusal, receipt=frame,
                capacity=None if capacity is None else capacity.to_dict(), slot=slot_fact)))
    raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_preopen', 'Preopening recovery requires another bounded pass.')
