"""Common ownership and retirement fences for host-private saved sessions."""

import asyncio
import logging

from nexus_connector_core import (
    CoreError, NativeSessionTarget, OperationKey, SessionKey, project_r4_resource_release,
)

from ..errors import ConnectorError

_LOG = logging.getLogger(__name__)


async def capture_owned_history(host, frame, *, scope, epoch, budget, adapter_id, read):
    """Observe the live session only while sufficient lease time remains to close it."""
    if budget <= 0:
        return None
    try:
        async with asyncio.timeout(min(3, budget)):
            journal = await host.ensure_history_journal()
            key = SessionKey(frame['server_id'], frame['executor_id'], frame['session_id'])
            claim = await journal.get_session_claim(key)
            if (claim is None or claim.opening_owner_generation != frame['session_owner_generation']
                    or claim.opening_connection_generation != frame['connection_generation']):
                return None
            opening = await journal.get_receipt(OperationKey(key.server_id, key.executor_id, claim.opening_operation_id))
            if opening is None or opening.native_id is None:
                return None
            target = NativeSessionTarget(claim.opening_operation_id, adapter_id, opening.native_id, epoch)
            return await read(scope, target, connection_id=frame['connection_id'],
                              connection_generation=frame['connection_generation'])
    except (CoreError, ConnectorError, OSError, TimeoutError):
        _LOG.debug('The live saved history observation was unavailable.')
        return None


async def record_retired_history(host, publications, frame, close_receipt, observation, *, scope, verify, record):
    """Bind verified history to the exact released process, opening and close."""
    proof, history = observation
    key = proof.key
    opening_id = proof.target.opening_operation_id
    try:
        async with asyncio.timeout(3):
            journal = await host.ensure_history_journal()
            claim = await journal.get_session_claim(key)
            birth = await journal.get_process_birth(key)
            if ((key.server_id, key.executor_id, key.session_id) !=
                    (frame['server_id'], frame['executor_id'], frame['session_id'])
                    or (proof.connection_id, proof.connection_generation, proof.owner_generation) != (
                        frame['connection_id'], frame['connection_generation'], frame['session_owner_generation'])
                    or close_receipt.stage != 'SUCCEEDED'
                    or claim is None or claim.opening_operation_id != opening_id
                    or claim.opening_owner_generation != proof.owner_generation
                    or claim.opening_connection_generation != proof.connection_generation
                    or frame['operation_id'] != close_receipt.operation_id or close_receipt.session_id != key.session_id
                    or birth != proof.process):
                return
            opening = await journal.get_receipt(OperationKey(key.server_id, key.executor_id, opening_id))
            published = await asyncio.to_thread(publications.lookup, key.server_id, key.executor_id,
                                                (opening_id, close_receipt.operation_id))
            open_binding = published.get(opening_id, {}).get('binding')
            close_binding = published.get(close_receipt.operation_id, {}).get('binding')
            if opening is None or open_binding is None or close_binding is None:
                return
            ledger = await host.ensure_ledger()
            slot = await ledger.owned_slot_state(key)
            release = project_r4_resource_release(open_binding, claim, slot, opening)
            evidence = await asyncio.to_thread(verify, history)
            if evidence is None:
                return
            await asyncio.to_thread(record, dict(evidence, scope=scope, source_connection_id=proof.connection_id,
                source_connection_generation=proof.connection_generation, stream_epoch=proof.target.stream_epoch,
                opening_operation_id=opening_id, close_operation_id=close_receipt.operation_id,
                opening_binding_digest=open_binding['digest'], close_binding_digest=close_binding['digest'],
                resource_release_digest=release['proof_digest']))
    except (CoreError, ConnectorError, OSError, TimeoutError):
        _LOG.debug('The retired saved history observation was unavailable.')
