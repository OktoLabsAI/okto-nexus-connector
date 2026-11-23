"""Record saved history only for the exact Core-owned Codex source that closed."""

import asyncio
from dataclasses import asdict
from pathlib import Path
import sys

from nexus_connector_core import CodexResumeGrant, CodexRolloutEvidence, CoreError
from nexus_connector_core.native.codex_rollout import (
    CODEX_SAVED_HISTORY_BUILD, observe_rollout, publish_rollout, saved_launch_configuration,
)

from ..errors import ConnectorError
from .r4_saved_history import capture_owned_history, record_retired_history


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
    return await capture_owned_history(host, frame, scope=scope, epoch=epoch, budget=budget,
        adapter_id='codex_app_server', read=runtime.read_live_current_codex_rollout)


async def record_retired_codex(host, publications, releases, frame, close_receipt, observation, *, scope, workspace,
                               opening_profile=None):
    def verify(rollout):
        if not _unchanged(rollout, workspace):
            return None
        return dict(workspace_root=workspace, rollout=asdict(rollout),
                    **({'opening_profile': opening_profile} if opening_profile is not None else {}))

    await record_retired_history(host, publications, frame, close_receipt, observation, scope=scope,
                                 verify=verify, record=releases.record_saved_codex_context)
