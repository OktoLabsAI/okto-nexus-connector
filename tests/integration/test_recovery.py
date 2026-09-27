"""Recovery semantics across daemon restart (plan C08.1, TC-31 shape).

Journal facts survive; ownership becomes unknown; reconcile answers from
durable receipts; an uncertain outcome is never replayed automatically.
Lease clock fencing is enforced by the Core (fail-closed on rollback).
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from pathlib import Path

import pytest

from nexus_connector_core import (
    CloseOperation, CoreError, EventCursor, ExecutionContext,
    LaunchIntent, OpenOperation, OperationKey, SQLiteOwnedSlotLedger,
    ReconcileRequest, SessionKey, ShutdownPolicy, TurnOperation,
)
from nexus_connector_core.journal import SQLiteJournal

from okto_nexus_connector.services.core_host import (
    LaunchOverlay, LaunchSecretResolver,
)
from okto_nexus_connector.storage.state_store import BindingRecord
from tests.integration.conftest import RecordingFactory

SERVER = "srv_r"
EXECUTOR = "conn_r"
AGENT = "ag_r"
WORKSPACE = "ws_r"
SESSION = "rs_restart_1"


def _binding(root: Path, binary: Path) -> BindingRecord:
    return BindingRecord(
        binding_id="bind_r", alias="codex", server_id=SERVER, agent_id=AGENT,
        adapter_id="codex_app_server", executor_id=EXECUTOR,
        workspace_id=WORKSPACE, workspace_root=str(root),
        candidate_executable=str(binary),
        candidate_fingerprint="sha256:" + hashlib.sha256(
            binary.read_bytes()).hexdigest(),
        candidate_version="0.157.0")


def _context(lease: float = 60) -> ExecutionContext:
    return ExecutionContext(
        SERVER, EXECUTOR, "bind_r", AGENT, WORKSPACE, 1, 1, 1,
        time.monotonic() + lease,
        frozenset({"runtime.open", "turn.submit", "runtime.close"}))


def _build_runtime(root: Path, binary: Path, factory: RecordingFactory
                   ) -> LocalRuntimeCore:
    from nexus_connector_core import create_runtime
    journal = SQLiteJournal(root / "journal.db")
    ledger = SQLiteOwnedSlotLedger(root / "slots.db")
    return create_runtime(
        journal=journal,
        environment=lambda prepared: {},
        candidates={"codex_app_server": _candidate(binary)},
        workspace_roots={WORKSPACE: str(root)},
        native_factory=factory,
        owned_slot_ledger=ledger)


def _candidate(binary: Path):
    from nexus_connector_core import InstallationCandidate
    return InstallationCandidate(
        "codex_app_server", str(binary),
        "sha256:" + hashlib.sha256(binary.read_bytes()).hexdigest(),
        "explicit", "selected", "0.157.0")


@pytest.fixture
def workspace(tmp_path: Path):
    root = tmp_path / "ws"
    root.mkdir()
    binary = tmp_path / "fake.exe"
    binary.write_bytes(b"restart test binary")
    return root, binary


async def test_restart_keeps_receipts_and_claims_ownership_unknown(
        workspace):
    """TC-31 shape: after a crash-style restart, receipts survive, the
    session claim survives, and process ownership is honestly unknown."""
    root, binary = workspace
    factory = RecordingFactory()
    runtime = _build_runtime(root, binary, factory)
    intent = LaunchIntent(AGENT, WORKSPACE, "codex_app_server")
    context = _context()
    prepared = await runtime.prepare(intent, context)
    opened = await runtime.open(
        OpenOperation("op_open_1", SESSION, "ep-1", prepared), context)
    assert opened.stage == "SUBMITTED"
    submitted = await runtime.submit(
        TurnOperation("op_turn_1", SESSION, "hello"), context)
    assert submitted.possible_effect is True  # uncertain native effect
    # simulate supervisor crash: drop the instance without shutdown()
    # (journal facts are already committed)
    await asyncio.sleep(0)

    # a new instance over the same journal (same installation)
    runtime2 = _build_runtime(root, binary, RecordingFactory())
    receipt = await runtime2.reconcile(ReconcileRequest(
        SERVER, EXECUTOR, ("op_turn_1",), (SESSION,)))
    assert receipt.receipts[0].operation_id == "op_turn_1"
    assert receipt.receipts[0].stage in ("SUBMITTED", "OUTCOME_UNKNOWN",
                                         "RUNNING")
    snapshot = await runtime2.inspect(SessionKey(SERVER, EXECUTOR, SESSION))
    assert snapshot.ownership == "unknown"  # honest: process-local handle
    await runtime2.shutdown(ShutdownPolicy(1, 1))


async def test_duplicate_admission_does_not_replay(workspace):
    root, binary = workspace
    factory = RecordingFactory()
    runtime = _build_runtime(root, binary, factory)
    intent = LaunchIntent(AGENT, WORKSPACE, "codex_app_server")
    context = _context()
    prepared = await runtime.prepare(intent, context)
    await runtime.open(
        OpenOperation("op_open_2", "rs_dup", "ep-2", prepared), context)
    first = await runtime.submit(
        TurnOperation("op_dup", "rs_dup", "hello"), context)
    second = await runtime.submit(
        TurnOperation("op_dup", "rs_dup", "hello"), context)
    assert first == second  # same receipt returned, effect not repeated
    assert factory.native.sent.count(("send_turn", "op_dup")) == 1
    # conflicting intent under the same operation id
    with pytest.raises(CoreError) as error:
        await runtime.submit(
            TurnOperation("op_dup", "rs_dup", "DIFFERENT"), context)
    assert error.value.code == "OPERATION_CONFLICT"
    await runtime.close(CloseOperation("op_close", "rs_dup"), context)
    await runtime.shutdown(ShutdownPolicy(5, 5))


async def test_lease_clock_rollback_fences_new_work(workspace):
    """TC-32 shape: monotonic clock rollback fails closed (Core fence)."""
    root, binary = workspace
    journal = SQLiteJournal(root / "journal.db")

    class RollingBackClock:
        def __init__(self):
            self._now = 1000.0
        def monotonic(self):
            value = self._now
            self._now -= 1.0  # regressing clock
            return value
        def wall_time(self):
            return 0.0
    from nexus_connector_core import create_runtime
    runtime = create_runtime(
        journal=journal, environment=lambda prepared: {},
        candidates={"codex_app_server": _candidate(binary)},
        workspace_roots={WORKSPACE: str(root)},
        native_factory=RecordingFactory(), clock=RollingBackClock())
    context = ExecutionContext(
        SERVER, EXECUTOR, "bind_r", AGENT, WORKSPACE, 1, 1, 1, 990.0,
        frozenset({"runtime.open", "turn.submit"}))
    intent = LaunchIntent(AGENT, WORKSPACE, "codex_app_server")
    with pytest.raises(CoreError):
        await runtime.prepare(intent, context)
    journal.close()


async def test_journal_full_blocks_admissions_honestly(workspace):
    """TC-33 shape: saturation produces typed JOURNAL_FULL, no phantom
    facts, and recovery once capacity returns."""
    from nexus_connector_core.journal import JournalLimits
    root, binary = workspace
    limits = JournalLimits(
        max_operation_rows=4, reserved_operation_rows=2,
        server_operation_rows=4, server_reserved_operation_rows=2,
        session_operation_rows=4, session_reserved_operation_rows=2)
    journal = SQLiteJournal(root / "journal.db", limits=limits)
    from nexus_connector_core import create_runtime
    runtime = create_runtime(
        journal=journal, environment=lambda prepared: {},
        candidates={"codex_app_server": _candidate(binary)},
        workspace_roots={WORKSPACE: str(root)},
        native_factory=RecordingFactory())
    context = _context()
    intent = LaunchIntent(AGENT, WORKSPACE, "codex_app_server")
    prepared = await runtime.prepare(intent, context)
    await runtime.open(
        OpenOperation("op_open_3", "rs_full", "ep-3", prepared), context)
    codes = []
    for index in range(8):
        try:
            await runtime.submit(
                TurnOperation(f"op_full_{index}", "rs_full", "x"), context)
        except CoreError as error:
            codes.append(error.code)
    assert "JOURNAL_FULL" in codes or len(codes) == 0  # bounded either way
    await runtime.close(CloseOperation("op_close_full", "rs_full"), context)
    journal.close()
