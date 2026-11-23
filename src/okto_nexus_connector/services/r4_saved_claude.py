"""Retain and revalidate exact Claude dialogue on the owning Connector."""

import asyncio
from dataclasses import asdict
from functools import partial
from pathlib import Path

from nexus_connector_core import CoreError
from nexus_connector_core.native.claude_saved_history import ClaudeSavedHistory, observe_claude_saved_history
from nexus_connector_core.native.claude_terminal_resume import (
    ClaudeTerminalResumeGrant, reviewed_claude_terminal_candidate,
)
from nexus_connector_core.native.codex_rollout import saved_launch_configuration
from nexus_connector_core.native.claude_terminal_profile import observe_claude_resume_profile

from ..errors import ConnectorError
from .r4_saved_history import capture_owned_history, record_retired_history


def _history(witness):
    saved = witness['history']
    return ClaudeSavedHistory(**(saved | {field: tuple(saved[field]) for field in (
        'home_identity', 'workspace_identity', 'file_identity')}))


def _current_history(history, profile):
    return (profile is not None
            and observe_claude_resume_profile(history.state_home, history.workspace) == profile
            and observe_claude_saved_history(history.state_home, history.session_uuid, history.workspace) == history)


def probe_saved_claude(releases, frame):
    """Report only whether the exact closed history and approved profile still match."""
    witness = releases.saved_context('claude_stream', frame['scope'], frame['source'])
    return witness is not None and _current_history(
        _history(witness), witness['opening_profile'].get('claude_resume_profile'))


class SavedClaudeOpening:
    """Reserve one exact continuation before granting a new terminal owner."""

    def __init__(self, releases, frame, source, scope, *, require_current):
        self.releases, self.frame, self.require_current = releases, frame, require_current
        self.witness = releases.saved_context('claude_stream', scope, source)
        if self.witness is None or not self.witness.get('opening_profile', {}).get('claude_resume_profile'):
            raise CoreError('CAPABILITY_UNSUPPORTED', 'saved_claude_resume', retry_safe=True)
        self.history = _history(self.witness)
        self.profile = self.witness['opening_profile']['claude_resume_profile']

    async def grant(self, prepared, session_id, context, *, profile, require_profile_current):
        fields = ('server_id', 'executor_id', 'binding_id', 'agent_id', 'workspace_id',
                  'session_owner_generation', 'authorization_revision', 'configuration_revision')
        if session_id != self.frame['session_id'] or any(
                getattr(context, field) != self.frame[field] for field in fields):
            raise CoreError('STALE_GENERATION', 'saved_claude_resume', retry_safe=True)
        self.require_current()
        prior = self.witness['opening_profile']
        launch = saved_launch_configuration(prepared)
        if (profile != self.profile or not reviewed_claude_terminal_candidate(prepared.candidate)
                or Path(prepared.cwd) != Path(self.history.workspace)
                or any(launch[field] != prior.get(field) for field in (
                    'root_fingerprint', 'resume_configuration_sha256'))
                or not await asyncio.to_thread(_current_history, self.history, profile)):
            raise CoreError('PROFILE_DRIFT', 'saved_claude_resume', retry_safe=True)
        self.require_current()
        try:
            await asyncio.to_thread(self.releases.claim_saved_successor, 'claude_stream', self.witness,
                                    session_id=session_id, operation_id=self.frame['operation_id'])
        except ConnectorError as error:
            raise CoreError(error.code, 'saved_claude_resume', retry_safe=True) from None
        self.require_current()
        return ClaudeTerminalResumeGrant(self.history, session_id, *(getattr(context, field) for field in fields),
            prepared.profile_fingerprint, prepared.root_fingerprint, require_profile_current,
            question_only=profile.get('question_only') is True, read_approval=profile.get('read_approval') is True)


async def capture_saved_claude(host, runtime, frame, *, scope, epoch, budget):
    return await capture_owned_history(host, frame, scope=scope, epoch=epoch, budget=budget,
        adapter_id='claude_stream', read=runtime.read_live_current_claude_saved_history)


async def record_retired_claude(host, publications, releases, frame, close_receipt, observation, *, scope,
                               opening_profile):
    def verify(history):
        expected_profile = opening_profile['claude_resume_profile']
        if observe_claude_resume_profile(history.state_home, history.workspace) != expected_profile:
            return None
        # The reviewed build can append bounded cost metadata on shutdown.
        # Changed dialogue, identity or workspace invalidates the proof.
        final = observe_claude_saved_history(history.state_home, history.session_uuid, history.workspace,
                                             before_close=history)
        if final is None:
            return None
        return dict(history=asdict(final), opening_profile=opening_profile)

    await record_retired_history(host, publications, frame, close_receipt, observation, scope=scope,
        verify=verify, record=partial(releases.record_saved_context, 'claude_stream'))
