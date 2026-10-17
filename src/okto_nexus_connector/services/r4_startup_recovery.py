"""Replay effect-free prelease openings through the replacement control owner."""
import asyncio
import secrets
import time

from nexus_connector_core import OperationKey, SessionKey, project_r4_bound_receipt
from nexus_connector_core.startup_refusal_r4 import persist_r4_startup_refusal
from nexus_connector_core.startup_refusal_link import (
    STARTUP_REFUSAL_REVISION, decode_startup_refusal, encode_startup_refusal,
)

from ..errors import ConnectorError
from ..storage.r4_publications import R4PublicationStore


async def recover_startup_refusals(store, host, channel, *, boot_id, clock=time.monotonic):
    publications = R4PublicationStore.for_state(store)
    journal, ledger = await host.ensure_history_journal(), await host.ensure_ledger()
    after = ''
    recovered = set()
    for _ in range(32):
        page = await asyncio.to_thread(publications.startup_recovery_page,
            channel.state.server_id, channel.state.executor_id,
            generation=channel.state.connection_generation, after=after)
        if not page:
            return recovered
        for row in page:
            metadata = row['metadata']
            after = metadata['operation_id']
            refusal = row['refusal']
            if refusal is None:
                raise ConnectorError('RECONCILIATION_REQUIRED', 'startup_recovery',
                    'The original startup association is missing.')
            key = OperationKey(metadata['server_id'], metadata['executor_id'],
                metadata['operation_id'])
            session = SessionKey(key.server_id, key.executor_id, metadata['session_id'])
            if await journal.get_session_claim(session) is not None:
                raise ConnectorError('RECONCILIATION_REQUIRED', 'startup_recovery',
                    'A native opening claim conflicts with startup refusal.')
            fact = await persist_r4_startup_refusal(journal, refusal)
            frame = project_r4_bound_receipt(refusal['binding'], fact,
                key=key, receipt_revision=1)
            if row['receipt'] is not None and row['receipt'] != frame:
                raise ConnectorError('OPERATION_CONFLICT', 'startup_recovery',
                    'The startup fact changed during recovery.')
            for owner in (journal, ledger):
                slot = await owner.owned_slot_state(session)
                if slot is not None and not slot.released and slot.opening_operation_id == key.operation_id:
                    await owner.release_owned_slot(key, session.session_id)
            capacity = await ledger.capacity_for_operation(key,
                action='runtime.open', now=clock(), boot_id=boot_id)
            if capacity is not None:
                if (capacity.request.unit != 'new_session'
                        or capacity.consumption is None
                        or capacity.consumption.opening_operation_id != key.operation_id
                        or capacity.consumption.opening_intent_hash != metadata['intent_hash']):
                    raise ConnectorError('SCOPE_MISMATCH', 'startup_recovery_capacity_fact',
                        'The capacity belongs to another opening.')
                if any(getattr(capacity.request.scope, name) != metadata[name] for name in (
                        'server_id', 'executor_id', 'agent_id', 'binding_id',
                        'session_id', 'connection_generation', 'session_owner_generation')):
                    raise ConnectorError('SCOPE_MISMATCH', 'startup_recovery_capacity_scope',
                        'The capacity belongs to another opening.')
                capacity = await ledger.release_capacity_slot(capacity.request,
                    capacity.reservation_id, now=clock(), boot_id=boot_id)
                if capacity.state != 'RELEASED':
                    raise ConnectorError('RECONCILIATION_REQUIRED', 'startup_recovery',
                        'Original capacity is not released.')
            for owner in (journal, ledger):
                slot = await owner.owned_slot_state(session)
                if slot is not None and not slot.released:
                    raise ConnectorError('RECONCILIATION_REQUIRED', 'startup_recovery',
                        'The physical slot remains owned after startup refusal.')
            old = row['startup_report']
            if old is not None and old['capacity'] != (None if capacity is None else capacity.to_dict()):
                raise ConnectorError('SCOPE_MISMATCH', 'startup_recovery_capacity_proof',
                    'The original capacity release proof changed.')
            report = dict(contract_revision=STARTUP_REFUSAL_REVISION,
                type='startup.refuse', request_id=old['request_id'] if old else
                'startup_' + secrets.token_hex(16),
                server_id=key.server_id, executor_id=key.executor_id,
                connection_id=channel.state.connection_id,
                connection_generation=channel.state.connection_generation,
                refusal=refusal, receipt=frame,
                capacity=None if capacity is None else capacity.to_dict(), native_absent=True)
            report = decode_startup_refusal(encode_startup_refusal(report))
            await asyncio.to_thread(publications.record_startup, report)
            if not await channel.publish_startup_refusal(report):
                raise ConnectorError('RECONCILIATION_REQUIRED', 'startup_recovery',
                    'The Server deferred the retained startup refusal.')
            await asyncio.to_thread(publications.acknowledge, frame)
            recovered.add(session.session_id)
    raise ConnectorError('RECONCILIATION_REQUIRED', 'startup_recovery',
        'The startup recovery page budget was exceeded.')
