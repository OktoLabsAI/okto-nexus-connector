"""Executor-owned R4 realization evidence and durable local root mapping.

The Nexus receives opaque references and digests. This file is the only place
that joins them to a physical root and selected installation on this host.
No provider is started while staging or publishing a realization.
"""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
import re
import secrets
from typing import Iterable, TYPE_CHECKING

from nexus_connector_core import InstallationCandidate
from nexus_connector_core.discovery import selected_fingerprint
from nexus_connector_core.protocol import canonical_json

from ..errors import ConnectorError
from ..storage.state_store import LocalRealizationRecord, StateStore
from .discovery_service import resolve_executor_installation

if TYPE_CHECKING:
    from ..transport.https_client import NexusHTTPClient, R4Realization


_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


def _root_digest(record: LocalRealizationRecord, nonce: str) -> str:
    root = Path(record.workspace_root)
    try:
        stat = root.stat()
    except OSError as exc:
        raise ConnectorError("PROFILE_DRIFT", "realization",
                             "The approved workspace root is unavailable") from exc
    if not root.is_dir():
        raise ConnectorError("PROFILE_DRIFT", "realization",
                             "The approved workspace root is not a directory")
    evidence = {
        "server_id": record.server_id, "executor_id": record.executor_id,
        "agent_id": record.agent_id, "root": record.workspace_root,
        "device": str(stat.st_dev), "inode": str(stat.st_ino),
        "nonce": nonce,
    }
    return "sha256:" + hashlib.sha256(canonical_json(evidence)).hexdigest()


def stage_local_realization(
    store: StateStore, *, server_id: str, executor_id: str, agent_id: str,
    client_intent_id: str, candidates: Iterable[InstallationCandidate],
    adapter_id: str, candidate_ref: str, inventory_revision: str,
    workspace_root: Path, workspace_id: str | None,
    workspace_label: str, configuration_digest: str,
    local_consent_id: str,
) -> LocalRealizationRecord:
    """Check local evidence and persist a recoverable publication intent."""
    for name, value in (
        ("server_id", server_id), ("executor_id", executor_id),
        ("agent_id", agent_id), ("client_intent_id", client_intent_id),
        ("adapter_id", adapter_id), ("candidate_ref", candidate_ref),
        ("local_consent_id", local_consent_id),
    ):
        if not isinstance(value, str) or not 1 <= len(value) <= 160:
            raise ConnectorError("VALIDATION_ERROR", "realization",
                                 f"Invalid {name}")
    if (not _DIGEST.fullmatch(inventory_revision) or
            not _DIGEST.fullmatch(configuration_digest) or
            not isinstance(workspace_label, str) or
            len(workspace_label) > 160 or
            any(char in workspace_label for char in ("/", "\\", ":")) or
            (workspace_id is not None and
             (not isinstance(workspace_id, str) or
              not 1 <= len(workspace_id) <= 160))):
        raise ConnectorError("VALIDATION_ERROR", "realization",
                             "Invalid realization evidence or workspace label")
    try:
        root = workspace_root.resolve(strict=True)
    except OSError as exc:
        raise ConnectorError("PROFILE_DRIFT", "realization",
                             "The approved workspace root is unavailable") from exc
    if not root.is_dir():
        raise ConnectorError("PROFILE_DRIFT", "realization",
                             "The approved workspace root is not a directory")
    selected = resolve_executor_installation(
        candidates, adapter_id=adapter_id, candidate_ref=candidate_ref,
        expected_inventory_revision=inventory_revision,
    )
    try:
        observed_fingerprint = selected_fingerprint(selected)
    except (OSError, ValueError) as exc:
        raise ConnectorError("PROFILE_DRIFT", "realization",
                             "The selected installation is unavailable") from exc
    if observed_fingerprint != selected.fingerprint:
        raise ConnectorError("PROFILE_DRIFT", "realization",
                             "The selected installation has changed")
    selected_executable = str(Path(selected.executable).resolve(strict=True))
    staged: LocalRealizationRecord | None = None

    def _record(state):
        nonlocal staged
        current = next((item for item in state.realizations
                        if item.server_id == server_id and
                        item.executor_id == executor_id and
                        item.agent_id == agent_id and
                        item.client_intent_id == client_intent_id), None)
        if current is not None:
            if (current.workspace_root != str(root) or
                    current.workspace_id != workspace_id or
                    current.workspace_label != workspace_label or
                    current.adapter_id != adapter_id or
                    current.candidate_ref != candidate_ref or
                    current.candidate_executable != selected_executable or
                    current.candidate_fingerprint != observed_fingerprint or
                    current.candidate_source != selected.source or
                    current.candidate_trust != selected.trust or
                    current.candidate_launch_script != selected.launch_script or
                    current.candidate_build_identity != selected.build_identity or
                    current.candidate_version != selected.version or
                    current.candidate_architecture != selected.architecture or
                    current.inventory_revision != inventory_revision or
                    current.configuration_digest != configuration_digest or
                    current.local_consent_id != local_consent_id):
                raise ConnectorError("OPERATION_CONFLICT", "realization",
                                     "The local intent ID has different content")
            if _root_digest(current, current.root_proof_nonce) != current.local_root_proof_digest:
                raise ConnectorError("PROFILE_DRIFT", "realization",
                                     "The approved workspace root has changed")
            staged = current
            return
        nonce = secrets.token_urlsafe(24)
        record = LocalRealizationRecord(
            client_intent_id=client_intent_id, server_id=server_id,
            executor_id=executor_id, agent_id=agent_id,
            local_realization_ref="root_" + secrets.token_hex(16),
            realization_revision=1, workspace_id=workspace_id,
            workspace_label=workspace_label, workspace_root=str(root),
            adapter_id=adapter_id, candidate_ref=candidate_ref,
            candidate_executable=selected_executable,
            candidate_fingerprint=observed_fingerprint,
            candidate_source=selected.source,
            candidate_trust=selected.trust,
            candidate_launch_script=selected.launch_script,
            candidate_build_identity=selected.build_identity,
            candidate_version=selected.version,
            candidate_architecture=selected.architecture,
            inventory_revision=inventory_revision,
            local_root_proof_digest="", root_proof_nonce=nonce,
            configuration_digest=configuration_digest,
            local_consent_id=local_consent_id,
        )
        record.local_root_proof_digest = _root_digest(record, nonce)
        state.realizations.append(record)
        staged = record

    store.update(_record)
    assert staged is not None
    return staged


def publication_body(record: LocalRealizationRecord) -> dict[str, object]:
    """Project local state without executable, root path or secret material."""
    return {
        "client_intent_id": record.client_intent_id,
        "agent_id": record.agent_id,
        "local_realization_ref": record.local_realization_ref,
        "realization_revision": record.realization_revision,
        "workspace_id": record.workspace_id,
        "workspace_label": record.workspace_label,
        "adapter_id": record.adapter_id,
        "candidate_ref": record.candidate_ref,
        "inventory_revision": record.inventory_revision,
        "local_root_proof_digest": record.local_root_proof_digest,
        "configuration_digest": record.configuration_digest,
        "local_consent_id": record.local_consent_id,
    }


def acknowledge_local_realization(
    store: StateStore, *, record: LocalRealizationRecord,
    published: R4Realization,
) -> LocalRealizationRecord:
    """Bind the server ref to the exact locally staged intent under CAS."""
    if (published.server_id != record.server_id or
            published.executor_id != record.executor_id or
            published.agent_id != record.agent_id or
            published.local_realization_ref != record.local_realization_ref or
            published.realization_revision != record.realization_revision or
            published.inventory_revision != record.inventory_revision or
            published.configuration_digest != record.configuration_digest or
            (record.workspace_id is not None and
             published.workspace_id != record.workspace_id)):
        raise ConnectorError("SCOPE_MISMATCH", "realization",
                             "The published realization changed scope")
    acknowledged: LocalRealizationRecord | None = None

    def _record(state):
        nonlocal acknowledged
        current = next((item for item in state.realizations
                        if item.server_id == record.server_id and
                        item.executor_id == record.executor_id and
                        item.agent_id == record.agent_id and
                        item.client_intent_id == record.client_intent_id), None)
        if (current is None or
                publication_body(current) != publication_body(record) or
                current.local_realization_ref != record.local_realization_ref or
                current.local_root_proof_digest != record.local_root_proof_digest or
                (current.realization_ref and
                 current.realization_ref != published.realization_ref) or
                (current.workspace_binding_id and
                 current.workspace_binding_id != published.workspace_binding_id) or
                (current.canonical_workspace_id and
                 current.canonical_workspace_id != published.workspace_id)):
            raise ConnectorError("OPERATION_CONFLICT", "realization",
                                 "The local realization mapping changed")
        current.realization_ref = published.realization_ref
        current.workspace_binding_id = published.workspace_binding_id
        current.canonical_workspace_id = published.workspace_id
        if current.status != "BOUND":
            current.status = "PENDING_APPROVAL"
        acknowledged = current

    store.update(_record)
    assert acknowledged is not None
    return acknowledged


async def publish_local_realization(
    store: StateStore, client: NexusHTTPClient, ticket: str, *,
    server_id: str, executor_id: str, agent_id: str,
    client_intent_id: str, candidates: Iterable[InstallationCandidate],
    adapter_id: str, candidate_ref: str, inventory_revision: str,
    workspace_root: Path, workspace_id: str | None,
    workspace_label: str, configuration_digest: str,
    local_consent_id: str,
) -> R4Realization:
    """Stage locally, publish over HTTPS, then durably bind the server ref."""
    record = await asyncio.to_thread(
        stage_local_realization, store, server_id=server_id,
        executor_id=executor_id, agent_id=agent_id,
        client_intent_id=client_intent_id, candidates=tuple(candidates),
        adapter_id=adapter_id, candidate_ref=candidate_ref,
        inventory_revision=inventory_revision, workspace_root=workspace_root,
        workspace_id=workspace_id, workspace_label=workspace_label,
        configuration_digest=configuration_digest,
        local_consent_id=local_consent_id,
    )
    published = await client.publish_r4_realization(
        ticket, executor_id=executor_id, request=publication_body(record))
    await asyncio.to_thread(
        acknowledge_local_realization, store, record=record,
        published=published)
    return published
