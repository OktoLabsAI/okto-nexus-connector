"""Join approved R4 references to host-owned physical evidence without effects."""

from dataclasses import asdict, dataclass
import hashlib
from pathlib import Path

from nexus_connector_core import CoreError, InstallationCandidate, decode_r4_frame, encode_r4_frame
from nexus_connector_core.build_identity import executable_build_identity, pi_build_identity
from nexus_connector_core.discovery import binary_architecture, selected_fingerprint
from nexus_connector_core.protocol import canonical_json

from ..errors import ConnectorError
from ..storage.state_store import ExecutionBindingRecord, StateStore
from ..transport.https_client import R4BindingView
from .discovery_service import resolve_executor_installation
from .realization_service import _root_digest


def _digest(value):
    return 'sha256:' + hashlib.sha256(canonical_json(asdict(value))).hexdigest()


def _unique(items, message):
    items = list(items)
    if len(items) != 1:
        raise ConnectorError('BINDING_NOT_AUTHORIZED', 'execution_selection', message)
    return items[0]


def _local_for(state, binding):
    return _unique((r for r in state.realizations if r.server_id == binding.server_id and
        r.executor_id == binding.executor_id and r.agent_id == binding.agent_id and
        r.realization_ref == binding.realization_ref), 'The approved local realization is missing or ambiguous.')


def _matches(local, binding):
    return (local.canonical_workspace_id == binding.workspace_id and all(
        getattr(local, key) == getattr(binding, key) for key in ('workspace_binding_id', 'adapter_id',
            'candidate_ref', 'inventory_revision', 'realization_revision')))


def acknowledge_execution_binding(store: StateStore, *, binding: R4BindingView) -> ExecutionBindingRecord:
    """Persist an authenticated apply result against the exact local evidence.

    This records administrative consent, never an execution grant or lease.
    A changed response cannot overwrite an already accepted binding silently.
    """
    if not isinstance(binding, R4BindingView) or binding.state != 'APPROVED':
        raise ConnectorError('BINDING_NOT_AUTHORIZED', 'execution_selection', 'An approved binding response is required.')
    result = None
    def record(state):
        nonlocal result
        local = _local_for(state, binding)
        if local.status not in ('PENDING_APPROVAL', 'BOUND') or not _matches(local, binding):
            raise ConnectorError('SCOPE_MISMATCH', 'execution_selection', 'The binding differs from the published realization.')
        if _root_digest(local, local.root_proof_nonce) != local.local_root_proof_digest:
            raise ConnectorError('PROFILE_DRIFT', 'execution_selection', 'The approved workspace root has changed.')
        local.status = 'BOUND'
        wanted = ExecutionBindingRecord(**asdict(binding), realization_snapshot_digest=_digest(local))
        existing = [r for r in state.execution_bindings if r.server_id == binding.server_id and
                    r.executor_id == binding.executor_id and r.binding_id == binding.binding_id]
        if existing:
            if len(existing) != 1 or existing[0] != wanted:
                raise ConnectorError('OPERATION_CONFLICT', 'execution_selection', 'The approved binding mapping changed.')
            result = existing[0]
        else:
            state.execution_bindings.append(wanted)
            result = wanted
    store.update(record)
    return result


@dataclass(frozen=True, slots=True)
class ExecutionSelection:
    server_id: str
    executor_id: str
    binding_id: str
    session_id: str
    agent_id: str
    workspace_id: str
    workspace_root: str
    candidate: InstallationCandidate
    binding_digest: str
    realization_digest: str
    opening_intent_hash: str


def resolve_execution_selection(store: StateStore, *, frame: dict, candidates) -> ExecutionSelection:
    """Resolve only opaque references from a schema-validated opening envelope.

    Full candidates come from this host's inventory, never from wire paths.
    Qualification, current lane and Core lease are separate admission gates.
    """
    parsed = decode_r4_frame(encode_r4_frame(frame))
    if parsed['type'] != 'operation.submit' or parsed['action'] != 'runtime.open':
        raise ConnectorError('VALIDATION_ERROR', 'execution_selection', 'An opening operation is required.')
    state = store.load()
    binding = _unique((b for b in state.execution_bindings if b.server_id == parsed['server_id'] and
        b.executor_id == parsed['executor_id'] and b.binding_id == parsed['binding_id']),
        'The approved execution binding is missing or ambiguous.')
    if binding.state != 'APPROVED' or any(parsed[key] != getattr(binding, key) for key in (
            'agent_id', 'workspace_id', 'workspace_binding_id', 'binding_revision',
            'authorization_revision', 'configuration_revision')):
        raise ConnectorError('STALE_GENERATION', 'execution_selection', 'The operation differs from the approved binding.')
    payload = parsed['payload']
    if any(payload[key] != getattr(binding, key) for key in (
            'adapter_id', 'candidate_ref', 'inventory_revision', 'realization_ref', 'realization_revision')):
        raise ConnectorError('STALE_GENERATION', 'execution_selection', 'The opening selection differs from the approved binding.')
    local = _local_for(state, binding)
    if local.status != 'BOUND' or not _matches(local, binding) or _digest(local) != binding.realization_snapshot_digest:
        raise ConnectorError('PROFILE_DRIFT', 'execution_selection', 'The approved local realization changed.')
    selected = resolve_executor_installation(candidates, adapter_id=binding.adapter_id,
        candidate_ref=binding.candidate_ref, expected_inventory_revision=binding.inventory_revision)
    try:
        physical_executable = str(Path(selected.executable).resolve(strict=True))
        physical_root = str(Path(local.workspace_root).resolve(strict=True))
        observed_fingerprint = selected_fingerprint(selected)
        observed_build = None
        if selected.build_identity is not None:
            observed_build = (pi_build_identity(Path(physical_executable), Path(selected.launch_script).parents[2])
                if selected.adapter_id == 'pi_rpc' and selected.launch_script else
                executable_build_identity(Path(physical_executable)))
        observed_architecture = binary_architecture(Path(physical_executable))
    except (OSError, ValueError, IndexError, CoreError) as error:
        raise ConnectorError('PROFILE_DRIFT', 'execution_selection', 'The approved local resources are unavailable.') from error
    if (physical_executable != local.candidate_executable or physical_root != local.workspace_root or
            observed_fingerprint != local.candidate_fingerprint or any(
                getattr(selected, field) != getattr(local, 'candidate_' + field) for field in
                ('fingerprint', 'source', 'trust', 'launch_script', 'build_identity', 'version', 'architecture')) or
            (selected.build_identity is not None and observed_build != selected.build_identity) or
            (observed_architecture is not None and selected.architecture is not None and
             observed_architecture != selected.architecture) or
            _root_digest(local, local.root_proof_nonce) != local.local_root_proof_digest):
        raise ConnectorError('PROFILE_DRIFT', 'execution_selection', 'The approved root or installation has changed.')
    return ExecutionSelection(binding.server_id, binding.executor_id, binding.binding_id, parsed['session_id'],
        binding.agent_id, binding.workspace_id, physical_root, selected, _digest(binding),
        binding.realization_snapshot_digest, parsed['intent_hash'])
