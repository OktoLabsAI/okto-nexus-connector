"""Recover interrupted launches under the replacement control-channel owner."""
import asyncio

from nexus_connector_core import OperationKey, project_r4_bound_receipt
from nexus_connector_core.native.process.launch_recovery import recover_process_launch

from ..errors import ConnectorError


async def recover_launches(reporter, journal, ledger, request):
    after, high, changed = 0, None, False
    was_blocked = reporter.blocked
    for _ in range(32):
        page = await journal.claimed_sessions(reporter.server_id, reporter.executor_id,
            after_rowid=after, high_water_rowid=high, limit=128)
        high = page.high_water_rowid
        for claim in page.claims:
            launch = await journal.get_process_launch(claim.key)
            if launch is None:
                continue  # Absence cannot prove a historical process stopped.
            if launch.stop_proof == 'NEVER_CLAIMED':
                reporter._unstarted_sessions.add(claim.key.session_id)
            slot = await ledger.owned_slot_state(claim.key)
            journal_slot = await journal.owned_slot_state(claim.key)
            if slot is not None and slot.released and journal_slot is not None and journal_slot.released:
                continue
            reporter.require_authority(request)
            rows = await asyncio.to_thread(reporter.publications.lookup,
                reporter.server_id, reporter.executor_id, [claim.opening_operation_id])
            opening = rows.get(claim.opening_operation_id)
            if opening is None or opening['binding'] is None:
                raise ConnectorError('RECONCILIATION_REQUIRED', 'launch_recovery',
                                     'The original opening has no durable wire binding.')
            key = OperationKey(claim.key.server_id, claim.key.executor_id, claim.opening_operation_id)
            receipt = await journal.get_receipt(key)
            projected = project_r4_bound_receipt(opening['binding'], receipt, key=key, receipt_revision=1)
            if (projected['session_id'] != claim.key.session_id or any(resource is not None and (
                    resource.key != claim.key or resource.opening_operation_id != claim.opening_operation_id)
                    for resource in (slot, journal_slot))):
                raise ConnectorError('SCOPE_MISMATCH', 'launch_recovery', 'The original resource scope changed.')
            result = await recover_process_launch(journal, claim.key,
                connection_generation=request['connection_generation'],
                validate_authority=lambda: reporter.require_authority(request))
            if result.state != 'STOPPED':
                continue  # Busy producer/uncertain container retains every slot.
            reporter.require_authority(request)
            if result.record.stop_proof == 'NEVER_CLAIMED':
                reporter._unstarted_sessions.add(claim.key.session_id)
                await reporter._fact(opening, journal)
                changed = True
            elif (slot is None or not projected['possible_effect'] or projected['stage'] not in (
                    'SUBMISSION_STARTED', 'SUBMITTED', 'RUNNING', 'OUTCOME_UNKNOWN', 'SUCCEEDED', 'FAILED')):
                raise ConnectorError('RECONCILIATION_REQUIRED', 'launch_recovery',
                                     'The stopped container has no matching opening receipt.')
            reporter.require_authority(request)
            # The updated receipt is already durable in the outbox. Readiness
            # remains blocked until its exact revision is published below.
            if journal_slot is not None and not journal_slot.released:
                await journal.release_owned_slot(key, claim.key.session_id)
            reporter.require_authority(request)
            if slot is not None and not slot.released:
                await ledger.release_owned_slot(key, claim.key.session_id)
        if page.next_after_rowid is None:
            break
        if page.next_after_rowid <= after:
            raise ConnectorError('JOURNAL_UNAVAILABLE', 'launch_recovery', 'The claim cursor did not advance.')
        after = page.next_after_rowid
    else:
        raise ConnectorError('RECONCILIATION_REQUIRED', 'launch_recovery', 'Launch recovery exceeded its page budget.')
    if changed:
        reporter.require_authority(request)
        if reporter.publish_receipts is None:
            raise ConnectorError('RECONCILIATION_REQUIRED', 'launch_recovery',
                                 'Recovered receipts still require durable publication.')
        await reporter.publish_receipts()
        reporter.require_authority(request)
        pending = (reporter.publications.pending_unfenced if request.get('preopen_refusal_supported')
                   else reporter.publications.pending)
        if await asyncio.to_thread(pending, reporter.server_id, reporter.executor_id):
            raise ConnectorError('RECONCILIATION_REQUIRED', 'launch_recovery',
                                 'Recovered receipts have not been acknowledged.')
        reporter.blocked = was_blocked
