"""Record saved history only for the exact Core-owned Codex source that closed."""

import asyncio
from dataclasses import asdict
import logging
from pathlib import Path
import sys

from nexus_connector_core import (
    CodexResumeGrant, CodexRolloutEvidence, CoreError, NativeSessionTarget, OperationKey, SessionKey,
    project_r4_resource_release,
)
from nexus_connector_core.native.codex_rollout import (
    CODEX_SAVED_HISTORY_BUILD, observe_rollout, publish_rollout, saved_launch_configuration,
)

from ..errors import ConnectorError

_LOG = logging.getLogger(__name__)


def _unchanged(rollout, workspace):
    path = Path(rollout.state_home).joinpath(*rollout.relative_path)
    return observe_rollout(rollout.state_home, str(path), rollout.thread_id, workspace) == rollout


def probe_saved_codex(releases, frame, *, require_opening_profile=False):
    """Re-read the exact host record and return only whether its rollout is still intact."""
    witness = releases.saved_codex_context(frame['scope'], frame['source'])
    if witness is None or (require_opening_profile and not witness.get('opening_profile')):
        return False
    return _unchanged(_rollout(witness), witness['workspace_root'])


def _rollout(witness):
    saved = witness['rollout']
    return CodexRolloutEvidence(**(saved | {key: tuple(saved[key]) for key in (
        'relative_path', 'home_identity', 'file_identity')}))


class SavedCodexOpening:
    """Authorize the exact continuation in the host's already approved environment."""

    def __init__(self, releases, frame, source, scope, *, require_current):
        self.releases, self.frame, self.require_current = releases, frame, require_current
        self.witness = releases.saved_codex_context(scope, source)
        if self.witness is None or not self.witness.get('opening_profile'):
            raise CoreError('CAPABILITY_UNSUPPORTED', 'saved_codex_resume', retry_safe=True)
        self.rollout = _rollout(self.witness)
        self.thread_id = self.rollout.thread_id

    def _check(self, prepared):
        current = saved_launch_configuration(prepared)
        prior = self.witness['opening_profile']
        candidate = prepared.candidate
        if (candidate.adapter_id != 'codex_app_server'
                or (candidate.version, sys.platform, candidate.architecture, candidate.fingerprint)
                    != CODEX_SAVED_HISTORY_BUILD
                or Path(prepared.cwd) != Path(self.witness['workspace_root'])
                or any(current[key] != prior.get(key) for key in ('root_fingerprint', 'resume_configuration_sha256'))
                or not _unchanged(self.rollout, prepared.cwd)):
            raise CoreError('PROFILE_DRIFT', 'saved_codex_resume', retry_safe=True)

    async def grant(self, prepared, session_id, context):
        identity = ('server_id', 'executor_id', 'binding_id', 'agent_id', 'workspace_id',
                    'session_owner_generation', 'authorization_revision', 'configuration_revision')
        if (session_id != self.frame['session_id']
                or any(getattr(context, field) != self.frame[field] for field in identity)):
            raise CoreError('STALE_GENERATION', 'saved_codex_resume', retry_safe=True)
        self.require_current()
        await asyncio.to_thread(self._check, prepared)
        self.require_current()
        try:
            await asyncio.to_thread(self.releases.claim_saved_codex_successor, self.witness,
                                    session_id=session_id, operation_id=self.frame['operation_id'])
        except ConnectorError as error:
            raise CoreError(error.code, 'saved_codex_resume', retry_safe=True) from None
        self.require_current()
        return CodexResumeGrant(self.thread_id, session_id, context.server_id, context.executor_id,
            context.binding_id, context.agent_id, context.workspace_id, context.session_owner_generation,
            prepared.candidate.fingerprint, prepared.root_fingerprint, prepared.profile_fingerprint, True, True, True)

    async def environment(self, resolver, prepared, owned_home):
        environment = await resolver(prepared)
        self.require_current()
        await asyncio.to_thread(self._stage, prepared, environment.get('CODEX_HOME'), owned_home)
        self.require_current()
        return environment

    def _stage(self, prepared, state_home, owned_home):
        self._check(prepared)
        if type(state_home) is not str or not Path(state_home).is_absolute():
            raise CoreError('PROFILE_DRIFT', 'saved_codex_resume', retry_safe=True)
        target = Path(state_home)
        native_home = Path(self.rollout.state_home)
        if target == native_home:
            return
        if owned_home is None or target != owned_home.home:
            raise CoreError('PROFILE_DRIFT', 'saved_codex_resume', retry_safe=True)
        try:
            owned_home.require_current()
            source = native_home.joinpath(*self.rollout.relative_path)
            destination = target.joinpath(*self.rollout.relative_path)
            publish_rollout(source, destination, target, self.rollout.sha256)
            published = observe_rollout(str(target), str(destination), self.thread_id, prepared.cwd)
            if published is None or published.sha256 != self.rollout.sha256:
                raise CoreError('PROFILE_DRIFT', 'saved_codex_resume', retry_safe=True)
            owned_home.require_current()
        except (OSError, ValueError, KeyError, ConnectorError):
            raise CoreError('PROFILE_DRIFT', 'saved_codex_resume', retry_safe=True) from None


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


async def record_retired_codex(host, publications, releases, frame, close_receipt, observation, *, scope, workspace,
                               opening_profile=None):
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
            if not await asyncio.to_thread(_unchanged, rollout, workspace):
                return
            await asyncio.to_thread(releases.record_saved_codex_context, {
                'scope': scope, 'source_connection_id': proof.connection_id,
                'source_connection_generation': proof.connection_generation, 'stream_epoch': proof.target.stream_epoch,
                'opening_operation_id': opening_id, 'close_operation_id': close_receipt.operation_id,
                'opening_binding_digest': open_binding['digest'], 'close_binding_digest': close_binding['digest'],
                'resource_release_digest': release['proof_digest'], 'workspace_root': workspace,
                'rollout': asdict(rollout),
                **({'opening_profile': opening_profile} if opening_profile is not None else {}),
            })
    except (CoreError, ConnectorError, OSError, TimeoutError):
        _LOG.debug('The retired Codex history observation was unavailable.')
