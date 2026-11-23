"""Retain exact Claude dialogue only after its managed process closes."""

from dataclasses import asdict
from functools import partial

from nexus_connector_core.native.claude_saved_history import observe_claude_saved_history
from nexus_connector_core.native.claude_terminal_profile import observe_claude_resume_profile

from .r4_saved_history import capture_owned_history, record_retired_history


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
