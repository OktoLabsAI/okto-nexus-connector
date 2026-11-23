"""Record saved history only for the exact Core-owned Codex source that closed."""

import asyncio
from dataclasses import asdict
import logging
from pathlib import Path

from nexus_connector_core import (
    CoreError, NativeSessionTarget, OperationKey, SessionKey, project_r4_resource_release,
)
from nexus_connector_core.native.codex_rollout import observe_rollout

from ..errors import ConnectorError

_LOG = logging.getLogger(__name__)


async def capture_saved_codex(host, runtime, frame, *, scope, epoch, budget):
    """Observe the owned rollout under the current lease without delaying containment past it."""
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
            target = NativeSessionTarget(claim.opening_operation_id, 'codex_app_server', opening.native_id, epoch)
            return await runtime.read_live_current_codex_rollout(scope, target,
                connection_id=frame['connection_id'], connection_generation=frame['connection_generation'])
    except (CoreError, ConnectorError, OSError, TimeoutError):
        _LOG.debug('The live Codex history observation was unavailable.')
        return None


async def record_retired_codex(host, publications, releases, frame, close_receipt, observation, *, scope, workspace):
    """Use closed process and slot facts plus unchanged bytes to preserve a local candidate."""
    proof, rollout = observation
    key = proof.key
    opening_id = proof.target.opening_operation_id
    try:
        async with asyncio.timeout(3):
            journal = await host.ensure_history_journal()
            claim = await journal.get_session_claim(key)
            birth = await journal.get_process_birth(key)
            if ((key.server_id, key.executor_id, key.session_id) !=
                    (frame['server_id'], frame['executor_id'], frame['session_id'])
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
            # The exact released slot is Core's stop proof for a managed close.
            release = project_r4_resource_release(open_binding, claim, slot, opening)
            path = Path(rollout.state_home).joinpath(*rollout.relative_path)
            current = await asyncio.to_thread(observe_rollout, rollout.state_home, str(path), rollout.thread_id, workspace)
            if current != rollout:
                return
            await asyncio.to_thread(releases.record_saved_codex_context, {
                'scope': scope, 'source_connection_id': proof.connection_id,
                'source_connection_generation': proof.connection_generation, 'stream_epoch': proof.target.stream_epoch,
                'opening_operation_id': opening_id, 'close_operation_id': close_receipt.operation_id,
                'opening_binding_digest': open_binding['digest'], 'close_binding_digest': close_binding['digest'],
                'resource_release_digest': release['proof_digest'], 'workspace_root': workspace,
                'rollout': asdict(rollout),
            })
    except (CoreError, ConnectorError, OSError, TimeoutError):
        _LOG.debug('The retired Codex history observation was unavailable.')
