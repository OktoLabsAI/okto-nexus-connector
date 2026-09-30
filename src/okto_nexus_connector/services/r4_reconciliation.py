"""Bounded reconciliation from public Core facts and persisted wire bindings."""
import asyncio
import secrets

from nexus_connector_core import EventCursor, OperationKey, project_r4_bound_receipt, project_r4_resource_release

from ..errors import ConnectorError
from ..storage.r4_publications import R4PublicationStore


class R4ReconciliationReporter:
    def __init__(self, store, host, server_id, executor_id):
        self.publications = R4PublicationStore.for_state(store)
        from ..storage.r4_events import R4EventStore
        self.event_store = R4EventStore.for_state(store)
        self.host, self.server_id, self.executor_id = host, server_id, executor_id
        self.cycle = None
        self.cursor = None
        self.after, self.high = 0, None
        self.claims_done = False
        self.blocked = False
        self.requested_sessions, self.seen_sessions = set(), set()

    async def _fact(self, row, journal):
        if row is None or row['binding'] is None:
            return None
        source = row['binding']['source']
        key = OperationKey(self.server_id, self.executor_id, source['operation_id'])
        fact = await journal.get_receipt(key)
        if fact is None:
            return None
        previous = row['receipt']
        revision = previous['receipt_revision'] if previous else 1
        projected = project_r4_bound_receipt(row['binding'], fact, key=key, receipt_revision=revision)
        if projected != previous:
            if previous:
                projected['receipt_revision'] += 1
            await asyncio.to_thread(self.publications.record, projected)
            self.blocked = True  # HTTP must commit this exact revision first.
        return projected

    async def report(self, request):
        if (request['server_id'], request['executor_id']) != (self.server_id, self.executor_id):
            raise ConnectorError('SCOPE_MISMATCH', 'r4_reconcile', 'The reconciliation scope changed.')
        if await asyncio.to_thread(self.publications.pending, self.server_id, self.executor_id):
            raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_reconcile', 'Receipt publication must finish before reconciliation.')
        if request.get('stream_watermarks'):
            raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_reconcile', 'Event stream recovery is required.')
        cycle = tuple(request[k] for k in ('connection_id', 'connection_generation', 'reconcile_id'))
        if request['cursor'] is None:
            self.cycle, self.cursor = cycle, None
            self.after, self.high, self.claims_done, self.blocked = 0, None, False, False
            self.requested_sessions, self.seen_sessions = set(), set()
        elif cycle != self.cycle or request['cursor'] != self.cursor:
            raise ConnectorError('STALE_GENERATION', 'r4_reconcile', 'The reconciliation cursor is stale.')
        self.requested_sessions.update(request['session_ids'])
        if len(self.requested_sessions) > 32768:
            raise ConnectorError('CAPACITY_EXCEEDED', 'r4_reconcile', 'The reconciliation session budget is exhausted.')
        journal = await self.host.ensure_history_journal()
        ledger = await self.host.ensure_ledger()
        # Live/unreleased slots cannot be adopted as stopped resources.
        for source in (journal, ledger):
            after, high = 0, None
            for _ in range(32):
                page = await source.owned_slot_page(after_rowid=after, high_water_rowid=high, limit=128)
                if any((r.key.server_id, r.key.executor_id) == (self.server_id, self.executor_id) for r in page.reservations):
                    raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_reconcile', 'Owned resources require live recovery.')
                high = page.high_water_rowid
                if page.next_after_rowid is None:
                    break
                if page.next_after_rowid <= after:
                    raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_reconcile', 'The ownership cursor did not advance.')
                after = page.next_after_rowid
            else:
                raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_reconcile', 'The ownership scan requires another recovery pass.')
        rows = await asyncio.to_thread(self.publications.lookup, self.server_id, self.executor_id, request['operation_ids'])
        receipts = []
        for operation_id in request['operation_ids']:
            fact = await self._fact(rows.get(operation_id), journal)
            if fact is None:
                raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_reconcile', 'The requested operation has no verified Core receipt.')
            else:
                receipts.append({k: fact[k] for k in ('operation_id', 'intent_hash', 'receipt_revision', 'stage')})
        claims, facts, watermarks = [], [], []
        if not self.claims_done:
            page = await journal.claimed_sessions(self.server_id, self.executor_id,
                after_rowid=self.after, high_water_rowid=self.high, limit=128)
            self.high = page.high_water_rowid
            for claim in page.claims:
                self.seen_sessions.add(claim.key.session_id)
                history = await asyncio.to_thread(self.publications.session_history,
                    self.server_id, self.executor_id, claim.key.session_id)
                generation = claim.opening_owner_generation
                closed = None
                opening = None
                for row in history:
                    metadata = row['metadata']
                    if metadata.get('session_owner_generation') != generation:
                        continue
                    if metadata['operation_id'] == claim.opening_operation_id:
                        opening = row
                    if metadata['action'] == 'runtime.close':
                        observed = await self._fact(row, journal)
                        if observed is not None and observed['stage'] == 'SUCCEEDED':
                            closed = row
                release_fact = None
                if closed is None and opening is not None and opening['binding'] is not None:
                    slot = await ledger.owned_slot_state(claim.key)
                    if slot is not None and slot.released:
                        opening_receipt = await journal.get_receipt(OperationKey(
                            self.server_id, self.executor_id, claim.opening_operation_id))
                        if opening_receipt is not None:
                            release_fact = project_r4_resource_release(
                                opening['binding'], claim, slot, opening_receipt)
                released = (closed is not None or release_fact is not None) and opening is not None and opening['metadata'].get('stream_epoch') is not None
                generation = generation if generation is not None else 0
                claims.append(dict(session_id=claim.key.session_id, owner_generation=generation,
                                   state='RELEASED' if released else 'UNKNOWN'))
                facts.append(dict(session_id=claim.key.session_id, owner_generation=generation,
                                  process_state='EXITED' if released else 'UNKNOWN'))
                if released:
                    facts[-1]['proof_digest'] = (release_fact['proof_digest'] if release_fact is not None
                                                   else closed['binding']['digest'])
                    epoch = opening['metadata']['stream_epoch']
                    sequence = await journal.contiguous_watermark(EventCursor(
                        self.server_id, self.executor_id, claim.key.session_id, epoch))
                    watermarks.append(dict(session_id=claim.key.session_id, stream_epoch=epoch, sequence=sequence))
                    remaining = journal.events(EventCursor(self.server_id,self.executor_id,claim.key.session_id,epoch,sequence))
                    try:
                        async for _ in remaining:
                            self.blocked = True
                            break
                    finally:
                        close = getattr(remaining,'aclose',None)
                        if close is not None:
                            await close()
                    if sequence > 0:
                        progress = await asyncio.to_thread(self.event_store.read,
                            {**opening['metadata'],'stream_epoch':epoch})
                        self.blocked |= progress['remote_acked'] != sequence or progress['core_applied'] != sequence
                else:
                    self.blocked = True
            if page.next_after_rowid is None:
                self.claims_done = True
            elif page.next_after_rowid <= self.after:
                raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_reconcile', 'The claim cursor did not advance.')
            else:
                self.after = page.next_after_rowid
        if self.claims_done and self.requested_sessions - self.seen_sessions:
            raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_reconcile', 'The requested session has no durable Core claim.')
        complete = self.claims_done and len(request['operation_ids']) < 256 and len(request['session_ids']) < 256
        self.cursor = None if complete else 'page_' + secrets.token_hex(16)
        return {k: request[k] for k in ('protocol_major', 'contract_revision', 'server_id', 'executor_id',
            'connection_id', 'connection_generation', 'reconcile_id', 'cursor')} | dict(
                type='reconcile.report', next_cursor=self.cursor, complete=complete,
                receipts=receipts, claims=claims, ownership_facts=facts, stream_watermarks=watermarks)
