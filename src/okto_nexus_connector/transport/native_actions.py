"""Canonical HTTP backend for the Core's public native domain bridge."""

from dataclasses import replace
import math
from types import MappingProxyType

from nexus_connector_core import CoreError
from nexus_connector_core.native_action_bridge import (
    NativeActionGrant, ScopedNativeActionBridge, native_action_scope,
)
from ..errors import ConnectorError

_ACTIONS = frozenset({'handoff.get', 'handoff.claim', 'handoff.complete'})


def native_capability_snapshot(capability):
    """Freeze mutable response metadata before any asynchronous operation."""
    from .https_client import R4SessionCapability
    if not isinstance(capability, R4SessionCapability):
        raise ConnectorError('VALIDATION_ERROR', 'native_action', 'A typed native capability is required.')
    try:
        scope = native_action_scope(capability.scope)
    except CoreError:
        raise ConnectorError('VALIDATION_ERROR', 'native_action', 'The native capability scope is invalid.') from None
    if (capability.audience != 'nexus-native-session'
            or type(capability.capability_id) is not str or not 1 <= len(capability.capability_id) <= 160
            or capability.capability_ref != 'native-cap:' + capability.capability_id
            or type(capability.capability) is not str or not capability.capability.startswith('nxc4_')
            or not 32 <= len(capability.capability) <= 4096
            or type(capability.actions) is not tuple or any(type(a) is not str for a in capability.actions)
            or not set(capability.actions) <= _ACTIONS
            or len(set(capability.actions)) != len(capability.actions)
            or type(capability.deadline_monotonic) not in (int, float)
            or not math.isfinite(capability.deadline_monotonic)
            or capability.mcp_url is not None):
        raise ConnectorError('VALIDATION_ERROR', 'native_action', 'The native capability is invalid.')
    return replace(capability, scope=MappingProxyType(scope))


class NexusNativeActions:
    """The backend owns no handoff state and never changes request identity."""

    def __init__(self, http, capability):
        self._http = http
        self._capability = native_capability_snapshot(capability)

    async def _invoke(self, request):
        try:
            return await self._http.native_action(self._capability, request=request)
        except ConnectorError as error:
            raise CoreError(error.code, 'native_action', possible_effect=error.possible_effect,
                retry_safe=error.retry_safe, operation_id=request.operation_id,
                message=error.message) from None

    async def get_context(self, request, context):
        return await self._invoke(request)

    async def claim_handoff(self, request, context):
        return await self._invoke(request)

    async def complete_handoff(self, request, context):
        return await self._invoke(request)


def native_action_bridge(http, capability, runtime, *, connection_id, connection_generation, clock=None):
    """Bind a Server-issued capability to this Core incarnation's installed lease."""
    cap = native_capability_snapshot(capability)
    scope = cap.scope
    grant = NativeActionGrant(cap.capability_ref, scope['server_id'], scope['executor_id'],
        scope['binding_id'], scope['agent_id'], scope['workspace_id'], scope['session_id'],
        connection_generation, scope['authorization_revision'], scope['configuration_revision'],
        cap.deadline_monotonic, frozenset(cap.actions), scope, connection_id)
    return ScopedNativeActionBridge(NexusNativeActions(http, cap), grant, clock=clock, r4_runtime=runtime)


def native_action_owner_factory(http, capability, *, connection_id, connection_generation, clock=None):
    """Freeze one approved capability before the host composes its runtime."""
    from nexus_connector_core.native_action_socket import PiNativeActionOwner
    cap = native_capability_snapshot(capability)
    def build(runtime):
        bridge = native_action_bridge(http, cap, runtime, connection_id=connection_id,
                                      connection_generation=connection_generation, clock=clock)
        def context():
            return runtime.r4_native_action_context(cap.scope, connection_id=connection_id,
                                                     connection_generation=connection_generation)
        return PiNativeActionOwner(bridge, context, capability_ref=cap.capability_ref,
                                   session_id=cap.scope["session_id"])
    return build
