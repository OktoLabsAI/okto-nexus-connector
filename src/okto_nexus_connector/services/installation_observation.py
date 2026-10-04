"""Explicit version probes and byte-bound, local-only observation reuse."""
from __future__ import annotations

import asyncio
from dataclasses import asdict, replace
import sys

from nexus_connector_core import __version__ as CORE_VERSION

from ..errors import ConnectorError
from .discovery_service import availability_snapshot, probe_version, resolve_executor_installation, resolve_selection


def _error(message, code='STALE_GENERATION'):
    return ConnectorError(code, 'installation_probe', message)


def apply_observations(candidates, observations):
    if not observations:
        return list(candidates)
    if not isinstance(observations, list) or len(observations) > 64:
        raise _error('The installation observations are invalid.', 'VALIDATION_ERROR')
    result = []
    for candidate in candidates:
        matches = []
        for entry in observations:
            if not isinstance(entry, dict) or set(entry) != {'core_version', 'platform', 'source', 'version'}:
                raise _error('The installation observation is invalid.', 'VALIDATION_ERROR')
            if (entry['platform'] == sys.platform
                    and entry['source'] == asdict(candidate)):
                if not isinstance(entry['version'], str) or not 1 <= len(entry['version']) <= 160:
                    raise _error('The observed version is invalid.', 'VALIDATION_ERROR')
                matches.append(entry['version'])
        if len(matches) > 1:
            raise _error('The installation observation is ambiguous.', 'VALIDATION_ERROR')
        result.append(replace(candidate, version=matches[0]) if matches else candidate)
    return result


def _registration(state, server_id):
    rows = [r for r in state.execution_executors if r.server_id == server_id]
    if (len(rows) != 1 or rows[0].state != 'REGISTERED' or not rows[0].executor_id
            or rows[0].connector_id != state.connector_id):
        raise _error('Register this Server executor before probing.', 'BINDING_NOT_AUTHORIZED')
    return replace(rows[0], inventory_publication_sequence=0)


async def observe_installation(store, *, server_id, adapter_id, candidate_ref, inventory_revision):
    from .discovery_configuration import configured_candidates, configuration_arguments
    scope = _registration(await asyncio.to_thread(store.load), server_id)
    raw = await configured_candidates(scope.discovery_configuration)
    effective = apply_observations(raw, scope.installation_observations)
    selected = resolve_executor_installation(effective, adapter_id=adapter_id,
        candidate_ref=candidate_ref, expected_inventory_revision=inventory_revision)
    source = resolve_selection(raw, adapter_id, candidate_ref)
    if _registration(await asyncio.to_thread(store.load), server_id) != scope:
        raise _error('The executor changed before the version probe.')
    # This is the explicit effect requested by the local operator. Core owns
    # containment, bounded observation, sealed environment and byte checks.
    observed = await probe_version(selected, strict=True)
    if not observed.version or replace(observed, version=source.version) != source:
        raise _error('The selected installation could not be observed consistently.', 'NATIVE_VERSION_UNQUALIFIED')
    current = await configured_candidates(scope.discovery_configuration)
    if resolve_selection(current, adapter_id, candidate_ref) != source:
        raise _error('The installation changed during the version probe.', 'PROFILE_DRIFT')
    entry = dict(core_version=CORE_VERSION, platform=sys.platform, source=asdict(source), version=observed.version)
    retained = [item for item in scope.installation_observations if item.get('source') != asdict(source)]
    if len(retained) >= 64:
        raise _error('At most 64 observations are retained; reconfigure discovery to clear them.', 'VALIDATION_ERROR')
    entries = [*retained, entry]
    result = availability_snapshot(apply_observations(current, entries))
    def commit(state):
        if _registration(state, server_id) != scope:
            raise _error('The executor changed during the version probe.')
        configuration_arguments(scope.discovery_configuration)
        next(r for r in state.execution_executors if r.server_id == server_id).installation_observations = entries
    await asyncio.to_thread(store.update, commit)
    return dict(server_id=server_id, executor_id=scope.executor_id, availability=result,
                publication_pending=True, runtime_authorized=False)
