"""Retain a Pi session file proof after its hosted owner has been released."""

from dataclasses import asdict
from functools import partial

from nexus_connector_core.native.pi_saved_history import pi_saved_history_current

from .r4_saved_history import capture_owned_history, record_retired_history


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
