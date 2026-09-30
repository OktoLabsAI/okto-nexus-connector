"""Non-secret persistent state: server profiles, identities, bindings.

Secrets never live here; identity records reference vault handles. Writes
are atomic (temp file + os.replace) and guarded by the same advisory
cross-process file lock used by the daemon, so CLI and daemon never corrupt
each other. A schema version supports explicit migrations (C09.4).
"""

from __future__ import annotations

import json
import os
import stat
import tempfile
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterator

from ..errors import ConnectorError

SCHEMA_VERSION = 8
_LOCK_POLL_SECONDS = 0.05


@dataclass(slots=True)
class ServerProfileRecord:
    server_id: str
    base_url: str
    origin: str
    added_at: str
    display_name: str = ""
    link_url_override: str = ""


@dataclass(slots=True)
class IdentityRecord:
    alias: str
    server_id: str
    agent_id: str
    secret_handle: str
    credential_epoch: int
    added_at: str
    display_name: str = ""
    revoked: bool = False

    @property
    def namespace(self) -> str:
        return f"{self.server_id}/{self.agent_id}"


@dataclass(slots=True)
class BindingRecord:
    binding_id: str
    alias: str
    server_id: str
    agent_id: str
    adapter_id: str
    executor_id: str
    workspace_id: str
    workspace_root: str
    endpoint_id: str = ""
    profile_id: str = ""
    authorization_revision: int = 1
    configuration_revision: int = 1
    candidate_executable: str = ""
    candidate_fingerprint: str = ""
    candidate_version: str = ""
    # Core 0.2.0 (PC09/PC12): portable build content identity and the Pi
    # composite launch script; both travel with the selected candidate.
    candidate_build_identity: str = ""
    candidate_launch_script: str = ""
    # CN1/A01 + Core C10/C11 (schema v2): the COMPLETE observed candidate
    # evidence. ``candidate_architecture`` is indispensable to the Core's
    # exact-build qualification and was previously lost between discovery
    # and runtime. ``installation_ref`` is the Core's opaque SELECTABLE
    # LOCAL INSTALLATION identity (byte-identical copies in distinct
    # locations stay distinct); ``inventory_revision`` versions the
    # selection evidence. Legacy records (schema v1) without these keep
    # working but are flagged ``needs_rediscovery`` — never guessed ready.
    candidate_architecture: str = ""
    installation_ref: str = ""
    #: CN3-05.03: evidence-derived inventory revision (opaque string
    #: from the availability snapshot; legacy ints load unchanged).
    inventory_revision: str | int = ""
    needs_rediscovery: bool = False
    created_at: str = ""
    mcp_entry_name: str = "nexus"
    tools_only: bool = False


@dataclass(slots=True)
class LocalRealizationRecord:
    """Executor-only root and candidate mapping; never sent as a whole."""

    client_intent_id: str
    server_id: str
    executor_id: str
    agent_id: str
    local_realization_ref: str
    realization_revision: int
    workspace_id: str | None
    workspace_label: str
    workspace_root: str
    adapter_id: str
    candidate_ref: str
    candidate_executable: str
    candidate_fingerprint: str
    candidate_source: str
    candidate_trust: str
    candidate_launch_script: str | None
    candidate_build_identity: str | None
    candidate_version: str | None
    candidate_architecture: str | None
    inventory_revision: str
    local_root_proof_digest: str
    root_proof_nonce: str
    configuration_digest: str
    local_consent_id: str
    canonical_workspace_id: str = ""
    realization_ref: str = ""
    workspace_binding_id: str = ""
    status: str = "LOCAL_VALIDATED"


@dataclass(slots=True)
class ExecutionBindingRecord:
    """Approved R4 mapping, separate from legacy bindings and runtime grants."""

    binding_id: str
    server_id: str
    executor_id: str
    agent_id: str
    endpoint_id: str
    workspace_id: str
    workspace_binding_id: str
    adapter_id: str
    candidate_ref: str
    inventory_revision: str
    realization_ref: str
    realization_revision: int
    binding_revision: int
    authorization_revision: int
    configuration_revision: int
    state: str
    realization_snapshot_digest: str


@dataclass(slots=True)
class ExecutionExecutorRecord:
    """Durable R4 registration intent and result; never a stored ticket."""

    server_id: str
    connector_id: str
    registration_agent_id: str
    client_intent_id: str
    label: str
    control_capabilities: tuple[str, ...] = ()
    executor_id: str = ""
    state: str = "REGISTRATION_PENDING"
    # Reserved before publishing; gaps after failure are valid, reuse is not.
    inventory_publication_sequence: int = 0


@dataclass(slots=True)
class SessionCapabilityRecord:
    """Durable issuance intent and vault reference; never credential material."""

    reservation_id: str
    request_id: str
    request_digest: str
    server_id: str
    executor_id: str
    session_id: str
    audience: str
    secret_handle: str
    status: str = "REQUESTED"
    capability_id: str = ""
    capability_ref: str = ""
    recovery_allowed: bool = False


@dataclass(slots=True)
class LaunchConfigurationRecord:
    """Executor-local approved configuration; the Server receives only its digest."""

    server_id: str
    executor_id: str
    agent_id: str
    local_consent_id: str
    adapter_id: str
    profile_revision: int
    secret_bindings: dict[str, str]
    provider_home: str | None
    home_device: str | None
    home_inode: str | None
    configuration_digest: str


@dataclass(slots=True)
class ConnectorState:
    schema_version: int = SCHEMA_VERSION
    connector_id: str = ""
    servers: dict[str, ServerProfileRecord] = field(default_factory=dict)
    identities: list[IdentityRecord] = field(default_factory=list)
    bindings: list[BindingRecord] = field(default_factory=list)
    realizations: list[LocalRealizationRecord] = field(default_factory=list)
    execution_bindings: list[ExecutionBindingRecord] = field(default_factory=list)
    execution_executors: list[ExecutionExecutorRecord] = field(default_factory=list)
    session_capabilities: list[SessionCapabilityRecord] = field(default_factory=list)
    launch_configurations: list[LaunchConfigurationRecord] = field(default_factory=list)
    preferences: dict[str, Any] = field(default_factory=dict)

    # -- lookups ---------------------------------------------------------

    def identity_by_alias(self, alias: str) -> IdentityRecord | None:
        for record in self.identities:
            if record.alias == alias:
                return record
        return None

    def identity_for(self, server_id: str, agent_id: str) -> IdentityRecord | None:
        for record in self.identities:
            if record.server_id == server_id and record.agent_id == agent_id:
                return record
        return None

    def binding_by_alias(self, alias: str) -> BindingRecord | None:
        matches = [b for b in self.bindings if b.alias == alias]
        return matches[0] if len(matches) == 1 else None

    def bindings_for_agent(self, server_id: str, agent_id: str) -> list[BindingRecord]:
        return [b for b in self.bindings
                if b.server_id == server_id and b.agent_id == agent_id]


class StateStore:
    """Atomic JSON state with a cross-process advisory lock."""

    def __init__(self, path: Path):
        self.path = path
        self.lock_path = path.with_suffix(".lock")

    def load(self) -> ConnectorState:
        # Windows readers must not open the snapshot during os.replace.
        # Use the same cross-process lock as writers, including across
        # separate StateStore instances used by CLI and daemon IPC.
        with self.locked():
            return _load_unlocked(self.path)

    def save(self, state: ConnectorState) -> None:
        """Replace the whole state snapshot atomically under the lock.

        ``save`` is a full-snapshot replace (last writer wins) for
        callers that own the state exclusively; ``update`` remains the
        CAS-style load-mutate-persist path for concurrent writers. Both
        share the same atomic writer and cross-process advisory lock
        (CN1/A15: the previous public ``save`` called a method that did
        not exist and raised ``AttributeError``).
        """
        with self.locked():
            _write_unlocked(self.path, state)

    @contextmanager
    def locked(self) -> Iterator[None]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_BINARY", 0)
        fd = os.open(self.lock_path, flags, 0o600)
        try:
            deadline = time.monotonic() + 10
            while True:
                try:
                    if os.name == "nt":
                        import msvcrt
                        os.lseek(fd, 0, os.SEEK_SET)
                        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise ConnectorError("CONFIG_LOCK_BUSY", "state_lock",
                                             retry_safe=True)
                    time.sleep(_LOCK_POLL_SECONDS)
            try:
                yield
            finally:
                if os.name == "nt":
                    import msvcrt
                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def update(self, mutate) -> ConnectorState:
        """Load, mutate and persist atomically under the cross-process lock."""
        with self.locked():
            state = _load_unlocked(self.path)
            mutate(state)
            _write_unlocked(self.path, state)
            return state


def state_from_json(payload: dict[str, Any]) -> ConnectorState:
    version = int(payload.get("schema_version", 0))
    if version > SCHEMA_VERSION:
        raise ConnectorError(
            "VERSION_INCOMPATIBLE", "state_load",
            f"state schema {version} is newer than supported "
            f"{SCHEMA_VERSION}; upgrade the connector first")
    state = ConnectorState()
    state.schema_version = SCHEMA_VERSION
    state.connector_id = str(payload.get("connector_id", ""))
    for item in payload.get("servers", {}).values():
        record = ServerProfileRecord(**item)
        state.servers[record.server_id] = record
    for item in payload.get("identities", []):
        state.identities.append(IdentityRecord(**item))
    for item in payload.get("bindings", []):
        record = BindingRecord(**item)
        # CN1/A01 migration (additive, schema v1 → v2): a legacy record
        # without complete candidate evidence keeps everything it had —
        # agent, key references, history — but is explicitly marked as
        # needing rediscovery instead of silently guessing architecture
        # or an installation ref. UNKNOWN is never converted to READY.
        if not record.candidate_architecture:
            record.needs_rediscovery = True
        state.bindings.append(record)
    for item in payload.get("realizations", []):
        state.realizations.append(LocalRealizationRecord(**item))
    for item in payload.get("execution_bindings", []):
        state.execution_bindings.append(ExecutionBindingRecord(**item))
    for item in payload.get("execution_executors", []):
        record = dict(item)
        record["control_capabilities"] = tuple(record.get("control_capabilities", ()))
        state.execution_executors.append(ExecutionExecutorRecord(**record))
    for item in payload.get("session_capabilities", []):
        state.session_capabilities.append(SessionCapabilityRecord(**item))
    for item in payload.get("launch_configurations", []):
        state.launch_configurations.append(LaunchConfigurationRecord(**item))
    preferences = payload.get("preferences", {})
    if isinstance(preferences, dict):
        state.preferences = preferences
    return state


def state_to_json(state: ConnectorState) -> dict[str, Any]:
    return {
        "schema_version": state.schema_version,
        "connector_id": state.connector_id,
        "servers": {key: asdict(value)
                    for key, value in state.servers.items()},
        "identities": [asdict(value) for value in state.identities],
        "bindings": [asdict(value) for value in state.bindings],
        "realizations": [asdict(value) for value in state.realizations],
        "execution_bindings": [asdict(value) for value in state.execution_bindings],
        "execution_executors": [asdict(value) for value in state.execution_executors],
        "session_capabilities": [asdict(value) for value in state.session_capabilities],
        "launch_configurations": [asdict(value) for value in state.launch_configurations],
        "preferences": state.preferences,
    }


def _load_unlocked(path: Path) -> ConnectorState:
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return ConnectorState()
    if len(raw) > 4 * 1024 * 1024:
        raise ConnectorError("CAPACITY_EXCEEDED", "state_load")
    return state_from_json(json.loads(raw.decode("utf-8")))


def _write_unlocked(path: Path, state: ConnectorState) -> None:
    _write_unlocked_bytes(path, json.dumps(
        state_to_json(state), indent=1, sort_keys=True).encode("utf-8"))


def _write_unlocked_bytes(path: Path, data: bytes) -> None:
    fd, temp = tempfile.mkstemp(dir=str(path.parent), prefix=".state-",
                                suffix=".tmp")
    try:
        if os.name != "nt":
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        temp = None
    finally:
        if temp is not None:
            try:
                os.unlink(temp)
            except OSError:
                pass
    mode = stat.S_IMODE(path.stat().st_mode)
    if os.name != "nt" and mode != 0o600:
        os.chmod(path, 0o600)
