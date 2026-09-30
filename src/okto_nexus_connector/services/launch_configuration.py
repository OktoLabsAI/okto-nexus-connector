"""Resolve digest-bound, executor-local launch configuration through Core APIs."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, replace
import hashlib
from pathlib import Path
import re
import time
from types import MappingProxyType

from nexus_connector_core import decode_r4_frame, encode_r4_frame
from nexus_connector_core.protocol import canonical_json

from ..errors import ConnectorError
from ..storage.state_store import LaunchConfigurationRecord
from .execution_selection import resolve_execution_selection


def _digest(record):
    body = asdict(record)
    body.pop('configuration_digest')
    return 'sha256:' + hashlib.sha256(canonical_json(body)).hexdigest()


def _home(path):
    if path is None:
        return None, None, None
    try:
        home = Path(path)
        if not home.is_absolute() or not home.is_dir():
            raise ValueError()
        resolved = home.resolve(strict=True)
        stat = resolved.stat()
        return str(resolved), str(stat.st_dev), str(stat.st_ino)
    except (OSError, ValueError, TypeError):
        raise ConnectorError('PROFILE_DRIFT', 'launch_configuration',
                             'The approved provider home is unavailable.') from None


def stage_launch_configuration(store, *, server_id, executor_id, agent_id,
        local_consent_id, adapter_id, profile_revision, secret_bindings=None, provider_home=None):
    """Stage host consent before publishing the realization's configuration digest.

    This does not grant runtime authority. The normal realization/binding
    approval must adopt this digest before an opening can use it.
    """
    for value in (server_id, executor_id, agent_id, local_consent_id, adapter_id):
        if type(value) is not str or not 1 <= len(value) <= 160:
            raise ConnectorError('VALIDATION_ERROR', 'launch_configuration',
                                 'Configuration scope and local consent are required.')
    if type(profile_revision) is not int or profile_revision < 1:
        raise ConnectorError('VALIDATION_ERROR', 'launch_configuration', 'Invalid profile revision.')
    if secret_bindings is not None and type(secret_bindings) is not dict:
        raise ConnectorError('VALIDATION_ERROR', 'launch_configuration',
                             'Secret bindings must be an object of protected references.')
    bindings = dict(secret_bindings or {})
    if len(bindings) > 64 or any(
            type(name) is not str or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,127}', name)
            or type(ref) is not str or not 1 <= len(ref) <= 256
            or not ref.startswith(('vault:', 'provider:'))
            or not ref.partition(':')[2].strip()
            or any(char in ref for char in '\r\n\x00')
            or any(prefix in ref for prefix in ('nxs_', 'nxsept_', 'nxc4_', 'nxt4_'))
            for name, ref in bindings.items()):
        raise ConnectorError('VALIDATION_ERROR', 'launch_configuration',
                             'Configuration may contain protected secret references only.')
    home, device, inode = _home(provider_home)
    record = LaunchConfigurationRecord(server_id, executor_id, agent_id, local_consent_id,
        adapter_id, profile_revision, bindings, home, device, inode, '')
    record.configuration_digest = _digest(record)
    def stage(state):
        matches = [r for r in state.launch_configurations
                   if r.configuration_digest == record.configuration_digest]
        if matches:
            if len(matches) != 1 or matches[0] != record:
                raise ConnectorError('OPERATION_CONFLICT', 'launch_configuration',
                                     'The configuration digest has conflicting content.')
            return
        if len(state.launch_configurations) >= 256:
            raise ConnectorError('CAPACITY_EXCEEDED', 'launch_configuration',
                                 'The local configuration capacity is exhausted.')
        state.launch_configurations.append(record)
    store.update(stage)
    return replace(record, secret_bindings=dict(bindings))


def _resolve(store, frame, candidates):
    selection = resolve_execution_selection(store, frame=frame, candidates=candidates)
    state = store.load()
    matches = [r for r in state.launch_configurations
               if r.configuration_digest == selection.configuration_digest]
    if len(matches) != 1:
        raise ConnectorError('BINDING_NOT_AUTHORIZED', 'launch_configuration',
                             'The approved launch configuration is missing or ambiguous.')
    record = matches[0]
    if (record.configuration_digest != _digest(record)
            or any(getattr(record, name) != getattr(selection, name)
                   for name in ('server_id', 'executor_id', 'agent_id', 'local_consent_id'))
            or record.adapter_id != selection.candidate.adapter_id
            or record.profile_revision != frame['payload']['profile_revision']
            or (record.provider_home, record.home_device, record.home_inode) != _home(record.provider_home)):
        raise ConnectorError('PROFILE_DRIFT', 'launch_configuration',
                             'The approved launch configuration or provider home changed.')
    return selection, record


async def approved_launch_setup(store, vault, *, frame, candidates, capability=None, http=None, tool_root=None,
                               capability_metadata=None):
    """Compose the standard R4 launch port without raw wire environment or argv."""
    from .core_host import LaunchOverlay, LaunchSecretResolver, make_environment
    from .r4_execution import R4LaunchSetup
    if capability is not None:
        from ..transport.https_client import R4SessionCapability
        from nexus_connector_core.native_action_bridge import native_action_scope
        if not isinstance(capability, R4SessionCapability):
            raise ConnectorError('VALIDATION_ERROR', 'launch_configuration',
                                 'A typed session capability is required.')
        capability = replace(capability, scope=MappingProxyType(native_action_scope(capability.scope)))
    encoded = encode_r4_frame(frame)
    candidates = tuple(candidates)
    async def current():
        return await asyncio.to_thread(_resolve, store, decode_r4_frame(encoded), candidates)
    expected = await current()
    _, record = expected
    native_factory = None
    templates, material = (), {}
    home = record.provider_home
    session_home = None
    auth_refs = set(record.secret_bindings.values())
    if capability is not None:
        required_scope = {name: frame[name] for name in (
            'server_id', 'executor_id', 'binding_id', 'agent_id', 'workspace_id',
            'workspace_binding_id', 'session_id', 'session_owner_generation',
            'authorization_revision', 'configuration_revision', 'binding_revision',
            'credential_epoch')}
        if dict(capability.scope) != required_scope:
            raise ConnectorError('BINDING_NOT_AUTHORIZED', 'launch_configuration',
                                 'The tool capability differs from the approved session.')
        auth_refs.add(capability.capability_ref)
        if record.adapter_id == 'pi_rpc' and http is not None:
            from ..transport.native_actions import native_action_owner_factory
            native_factory = native_action_owner_factory(http, capability,
                connection_id=frame['connection_id'],
                connection_generation=frame['connection_generation'], metadata_provider=capability_metadata)
        elif record.adapter_id in ('codex_app_server', 'claude_stream') and http is not None and tool_root is not None:
            from .mcp_launch import mcp_template, session_mcp_home
            if record.provider_home is not None and not record.secret_bindings:
                raise ConnectorError('PROVIDER_AUTH_REQUIRED', 'launch_configuration',
                                     'Import provider credentials before using an isolated MCP session home.')
            template = mcp_template(capability, adapter_id=record.adapter_id, approved_origin=http.origin)
            session_home = await asyncio.to_thread(session_mcp_home, tool_root, frame=frame,
                configuration_digest=record.configuration_digest, template=template)
            home = str(session_home.home)
            templates = (template,)
            material = {capability.capability_ref: capability.capability}
        else:
            raise ConnectorError('CAPABILITY_UNSUPPORTED', 'launch_configuration',
                                 'The selected adapter has no approved tool configuration.')
    # Local references are passed to Core prepare. Core remains responsible
    # for environment name/value policy and resolving only prepared refs.
    render = make_environment(LaunchSecretResolver(vault, material), LaunchOverlay(
        secret_bindings=dict(record.secret_bindings), provider_home=home,
        trusted_home=home is not None, http_templates=templates))
    async def tools_current():
        if capability is not None and time.monotonic() >= capability.deadline_monotonic:
            raise ConnectorError('AUTH_EXPIRED', 'launch_configuration',
                                 'The session tool capability has expired.')
        if session_home is not None:
            await asyncio.to_thread(session_home.require_current)
    async def environment(prepared):
        if await current() != expected:
            raise ConnectorError('PROFILE_DRIFT', 'launch_configuration',
                                 'The approved launch configuration changed.')
        if prepared.intent.adapter_id != record.adapter_id:
            raise ConnectorError('BINDING_NOT_AUTHORIZED', 'launch_configuration',
                                 'The prepared adapter differs from the approved configuration.')
        await tools_current()
        result = await render(prepared)
        await tools_current()
        if await current() != expected:
            raise ConnectorError('PROFILE_DRIFT', 'launch_configuration',
                                 'The approved launch configuration changed.')
        return result
    await tools_current()
    return R4LaunchSetup(environment, auth_refs=tuple(sorted(auth_refs)),
                         native_action_factory=native_factory)
