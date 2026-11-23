"""Retain the retired Pi file and authorize its exact successor."""

import asyncio
from dataclasses import asdict
from functools import partial
from pathlib import Path

from nexus_connector_core import CoreError
from nexus_connector_core.native.codex_rollout import saved_launch_configuration
from nexus_connector_core.native.pi_saved_history import PiSavedHistory, PiSavedResumeGrant, pi_saved_history_current

from ..errors import ConnectorError

from .r4_saved_history import capture_owned_history, record_retired_history


def _history(witness):
    saved = witness['history']
    return PiSavedHistory(**(saved | {field: tuple(saved[field]) for field in (
        'agent_dir_identity', 'workspace_identity', 'file_identity')}))


def probe_saved_pi(releases, frame):
    witness = releases.saved_context('pi_rpc', frame['scope'], frame['source'])
    return (witness is not None and bool(witness.get('opening_profile'))
            and pi_saved_history_current(_history(witness)))


class SavedPiOpening:
    """Reserve a single successor for a proven, retired native branch."""

    def __init__(self, releases, frame, source, scope, *, require_current):
        self.releases, self.frame, self.require_current = releases, frame, require_current
        self.witness = releases.saved_context('pi_rpc', scope, source)
        if self.witness is None or not self.witness.get('opening_profile'):
            raise CoreError('CAPABILITY_UNSUPPORTED', 'saved_pi_resume', retry_safe=True)
        self.history = _history(self.witness)

    async def grant(self, prepared, session_id, context, environment, *, require_profile_current):
        fields = ('server_id', 'executor_id', 'binding_id', 'agent_id', 'workspace_id',
                  'session_owner_generation', 'authorization_revision', 'configuration_revision')
        if session_id != self.frame['session_id'] or any(
                getattr(context, field) != self.frame[field] for field in fields):
            raise CoreError('STALE_GENERATION', 'saved_pi_resume', retry_safe=True)
        def require_current():
            self.require_current()
            require_profile_current()
            self.require_current()

        current = saved_launch_configuration(prepared)
        prior = self.witness['opening_profile']
        home = environment.get('PI_CODING_AGENT_DIR')
        if (not home or Path(home) != Path(self.history.agent_dir)
                or Path(prepared.cwd) != Path(self.history.workspace)
                or any(current[field] != prior.get(field) for field in ('root_fingerprint', 'resume_configuration_sha256'))
                or not await asyncio.to_thread(pi_saved_history_current, self.history)):
            raise CoreError('PROFILE_DRIFT', 'saved_pi_resume', retry_safe=True)
        await asyncio.to_thread(require_current)
        try:
            await asyncio.to_thread(self.releases.claim_saved_successor, 'pi_rpc', self.witness,
                                    session_id=session_id, operation_id=self.frame['operation_id'])
        except ConnectorError as error:
            raise CoreError(error.code, 'saved_pi_resume', retry_safe=True) from None
        self.require_current()
        return PiSavedResumeGrant(self.history, session_id, *(getattr(context, field) for field in fields),
                                 prepared.profile_fingerprint, prepared.root_fingerprint, require_current)


async def capture_saved_pi(host, runtime, frame, *, scope, epoch, budget):
    return await capture_owned_history(host, frame, scope=scope, epoch=epoch, budget=budget,
        adapter_id='pi_rpc', read=runtime.read_live_current_pi_saved_history)


async def record_retired_pi(host, publications, releases, frame, close_receipt, observation, *, scope,
                           opening_profile):
    def verify(history):
        if pi_saved_history_current(history):
            return dict(history=asdict(history), opening_profile=opening_profile)
        return None

    await record_retired_history(host, publications, frame, close_receipt, observation, scope=scope,
        verify=verify, record=partial(releases.record_saved_context, 'pi_rpc'))
