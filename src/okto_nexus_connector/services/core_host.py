"""Composition of ``nexus_connector_core`` for one connector installation.

The Connector never duplicates native Popen/parsers (TC-20): every managed
launch goes through the Core's **public** composition API
(``create_runtime``, C1/PC06) — no private bridge imports. One technical
journal and one installation-wide owned-slot ledger are created once and
shared by all binding runtimes (both own an off-loop worker since Core
0.2.0); each binding gets its own runtime instance with its selected
candidate and workspace root.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from nexus_connector_core import (
    InstallationCandidate, LocalRuntimeCore, SQLiteOwnedSlotLedger,
    create_runtime,
)
from nexus_connector_core.environment import child_environment
from nexus_connector_core.harness_config import HarnessHTTPTemplate
from nexus_connector_core.installation import (
    REF_AMBIGUOUS, REF_NOT_FOUND, effective_installation_ref,
    resolve_installation,
)
from nexus_connector_core.journal import SQLiteJournal, open_journal
from nexus_connector_core.ports import SecretResolver

from ..errors import ConnectorError
from ..platform import paths
from ..storage.state_store import BindingRecord


@dataclass(frozen=True, slots=True, order=True)
class BindingKey:
    """CN1/A03: a binding is scoped by (server, binding id).

    Two independent Server installations may mint the SAME textual
    binding id; a bare ``binding_id`` key silently shares the wrong
    runtime. Every host-side registry uses this typed key.
    """

    server_id: str
    binding_id: str


@dataclass(frozen=True, slots=True)
class ExecutionRuntimeKey:
    server_id: str
    executor_id: str
    binding_id: str
    session_id: str


class LaunchSecretResolver:
    """Resolves vault handles and per-launch ephemeral capability refs."""

    def __init__(self, vault, capabilities: Mapping[str, str] | None = None):
        self._vault = vault
        self._capabilities = dict(capabilities or {})

    def with_capabilities(self, capabilities: Mapping[str, str]
                          ) -> "LaunchSecretResolver":
        return LaunchSecretResolver(self._vault, capabilities)

    async def resolve(self, reference: str) -> str:
        if reference in self._capabilities:
            return self._capabilities[reference]
        if reference.startswith("vault:"):
            return await asyncio.to_thread(self._vault.resolve, reference)
        if reference.startswith("provider:"):
            return await asyncio.to_thread(self._vault.resolve, f"vault:{reference}")
        raise ConnectorError("PROVIDER_AUTH_REQUIRED", "secret_resolver",
                             f"unknown secret reference kind",
                             action="Import the provider credential or use "
                                    "the harness's own local login.")


@dataclass(slots=True)
class LaunchOverlay:
    """Per-launch environment decisions owned by the trusted host."""

    secret_bindings: dict[str, str] = field(default_factory=dict)
    http_templates: tuple[HarnessHTTPTemplate, ...] = ()
    provider_home: str | None = None
    trusted_home: bool = False
    public_overrides: dict[str, str] = field(default_factory=dict)


def make_environment(resolver: LaunchSecretResolver, overlay: LaunchOverlay):
    async def environment(prepared) -> Mapping[str, str]:
        return await child_environment(
            prepared, resolver,
            secret_bindings=overlay.secret_bindings,
            http_templates=overlay.http_templates,
            provider_home=overlay.provider_home,
            trusted_home=overlay.trusted_home,
            public_overrides=overlay.public_overrides)
    return environment


class CoreRuntimeHost:
    """Builds and owns Core runtime instances for the daemon."""

    def __init__(self, root: Path, vault):
        self.root = root
        self._vault = vault
        self._journal_path = paths.journal_path(root)
        self._ledger_path = paths.owned_slot_ledger_path(root)
        # CN1/A03: keyed by the typed BindingKey — two Servers with the
        # same textual binding id never share an instance.
        self._runtimes: dict[BindingKey | ExecutionRuntimeKey, LocalRuntimeCore] = {}
        self._execution_selections = {}
        self._native_action_owners = {}
        self._native_action_factories = {}
        self._journal: SQLiteJournal | None = None
        self._ledger: SQLiteOwnedSlotLedger | None = None
        # CN1/CN-05.05: single-flight initialization for the shared
        # stores under concurrent starts.
        self._journal_gate: asyncio.Lock | None = None
        self._ledger_gate: asyncio.Lock | None = None

    # -- shared installation stores -------------------------------------

    @property
    def journal(self) -> SQLiteJournal:
        """Synchronous view of the shared journal (diagnostics/tests).

        The daemon's event-loop paths construct through ``ensure_journal``
        (the Core's async ``open_journal`` entry, C2/PC01.06) so the
        blocking schema setup never runs on the loop.
        """
        if self._journal is None:
            self._journal = SQLiteJournal(self._journal_path)
        return self._journal

    async def ensure_journal(self) -> SQLiteJournal:
        """Open (once) the shared journal off the event loop.

        CN1/CN-05.05: creation is single-flight — two concurrent starts
        share exactly one store, never orphan workers.
        """
        if self._journal is not None:
            return self._journal
        if self._journal_gate is None:
            self._journal_gate = asyncio.Lock()
        async with self._journal_gate:
            if self._journal is None:
                self._journal = await open_journal(self._journal_path)
        return self._journal

    async def ensure_ledger(self) -> SQLiteOwnedSlotLedger:
        if self._ledger is not None:
            return self._ledger
        if self._ledger_gate is None:
            self._ledger_gate = asyncio.Lock()
        async with self._ledger_gate:
            if self._ledger is None:
                self._ledger = await asyncio.to_thread(
                    SQLiteOwnedSlotLedger, self._ledger_path)
        return self._ledger

    @property
    def ledger(self) -> SQLiteOwnedSlotLedger:
        if self._ledger is None:
            self._ledger = SQLiteOwnedSlotLedger(self._ledger_path)
        return self._ledger

    def journal_if_open(self):
        """CN2/N04 (b10): the shared JOURNAL PORT (public) when already
        open — durable history queries never require the binary."""
        return self._journal

    async def ensure_history_journal(self):
        """CN3-04 (D04): OPEN the technical journal explicitly for
        history — a cold host (no runtime ever composed) recovers the
        durable store before answering reconcile/replay queries. Never
        composes a runtime, never validates a binary; single-flight; a
        failure to open is an error, not an empty success."""
        return await self.ensure_journal()

    # -- candidates -------------------------------------------------------

    def candidate_for(self, binding: BindingRecord) -> InstallationCandidate:
        """Revalidate the selected installation before every launch (A.6).

        CN1/A01: the COMPLETE observed candidate evidence survives the
        persistence round-trip — architecture included (indispensable to
        the Core's exact-build qualification), plus the Core C11
        installation ref when recorded. Checks the path-bound fingerprint
        AND, when recorded, the portable ``build_identity``; moving the
        installation still invalidates the local binding, while content
        drift is caught even when the path stays. Legacy records without
        architecture evidence are NEEDS_REDISCOVERY — the host never
        guesses the host's own architecture to make an allowlist pass.
        """
        from nexus_connector_core.discovery import fingerprint

        if binding.needs_rediscovery or not binding.candidate_architecture:
            raise ConnectorError(
                "NEEDS_REDISCOVERY", "candidate",
                "this binding predates complete candidate evidence "
                "(no recorded architecture)",
                action="Re-run 'okto-nexus-connector connect' (or "
                       "'bind create') from the project to rediscover "
                       "and reselect the installation; the agent, key "
                       "and history are preserved.")
        executable = Path(binding.candidate_executable)
        try:
            info = executable.stat()
        except OSError as exc:
            raise ConnectorError(
                "BINARY_NOT_FOUND", "candidate",
                f"selected binary is unavailable: {executable}",
                action="Re-run 'okto-nexus-connector discover' and re-bind "
                       "with a valid installation.") from exc
        if not info.st_mode & 0o111 and executable.suffix != ".js":
            raise ConnectorError("BINARY_NOT_FOUND", "candidate",
                                 "selected candidate is not executable")
        launch_script = binding.candidate_launch_script or None
        current = _composite_fingerprint(executable, launch_script)
        if current != binding.candidate_fingerprint:
            raise ConnectorError(
                "PROFILE_DRIFT", "candidate",
                "the selected binary changed since the binding was created",
                action="Re-run 'okto-nexus-connector bind create' after "
                       "verifying the update, or reinstall the previous "
                       "version.")
        build_identity = binding.candidate_build_identity or None
        if build_identity:
            current_identity = _current_build_identity(
                binding.adapter_id, executable, launch_script)
            if current_identity is not None and current_identity != \
                    build_identity:
                raise ConnectorError(
                    "PROFILE_DRIFT", "candidate",
                    "the build content changed since the binding was "
                    "created (portable build identity mismatch)",
                    action="Re-qualify the build explicitly; identical "
                           "bytes in another directory are still accepted.")
        architecture = binding.candidate_architecture or None
        # CN1/A01: when the binary exposes a parsable architecture it must
        # match the recorded evidence — never adopt the host's own.
        from nexus_connector_core.discovery import binary_architecture
        observed = binary_architecture(executable)
        if observed is not None and architecture is not None \
                and observed != architecture:
            raise ConnectorError(
                "PROFILE_DRIFT", "candidate",
                f"recorded architecture {architecture} but the binary "
                f"parses as {observed}",
                action="Rediscover and reselect the installation; the "
                       "recorded evidence no longer matches this host.")
        installation_ref = binding.installation_ref or None
        if installation_ref:
            from nexus_connector_core.installation import installation_ref \
                as derive_ref
            current_ref = derive_ref(binding.adapter_id, str(executable),
                                     launch_script)
            if current_ref != installation_ref:
                raise ConnectorError(
                    REF_NOT_FOUND, "candidate",
                    "the selected installation moved since the binding "
                    "was created (installation ref is stale)",
                    action="Reselect the installation explicitly; the old "
                           "reference no longer resolves to this target.")
        return InstallationCandidate(
            adapter_id=binding.adapter_id,
            executable=str(executable),
            fingerprint=binding.candidate_fingerprint,
            source="explicit",
            trust="selected",
            version=binding.candidate_version or None,
            architecture=architecture,
            launch_script=launch_script,
            build_identity=build_identity,
            installation_ref=installation_ref,
        )

    # -- runtime composition ------------------------------------------------

    async def approved_launch(self, store, *, frame, candidates, capability=None, http=None):
        from .launch_configuration import approved_launch_setup
        return await approved_launch_setup(store, self._vault, frame=frame, candidates=candidates,
                                           capability=capability, http=http, tool_root=self.root / 'runtime' / 'r4-mcp')

    async def build_r4(self, store, *, frame, candidates, environment, factory=None, native_action_factory=None):
        """Compose an R4 runtime only from the approved host realization.

        Composition is effect-free and does not install execution authority.
        The connection owner must install the canonical lease before prepare.
        Revalidate after shared-store waits and around the environment callback.
        """
        from .execution_selection import resolve_execution_selection
        from nexus_connector_core.native_action_socket import PiNativeActionOwner
        from nexus_connector_core import decode_r4_frame, encode_r4_frame
        # Freeze caller-owned input before yielding; no mutable wire dict or
        # candidate generator may retarget a build that is already waiting.
        encoded = encode_r4_frame(frame)
        candidates = tuple(candidates)
        async def resolve():
            return await asyncio.to_thread(resolve_execution_selection, store,
                frame=decode_r4_frame(encoded), candidates=candidates)
        selection = await resolve()
        if native_action_factory is not None and (
                not callable(native_action_factory) or selection.candidate.adapter_id != "pi_rpc"):
            raise ConnectorError("VALIDATION_ERROR", "native_action_launch",
                                 "A native action factory is only valid for an approved Pi installation.")
        key = ExecutionRuntimeKey(selection.server_id, selection.executor_id,
                                  selection.binding_id, selection.session_id)
        journal = await self.ensure_journal()
        ledger = await self.ensure_ledger()
        if await resolve() != selection:
            raise ConnectorError('PROFILE_DRIFT', 'runtime_composition', 'The approved execution selection changed.')
        existing = self._runtimes.get(key)
        if existing is not None:
            if (self._execution_selections.get(key) != selection
                    or self._native_action_factories.get(key) is not native_action_factory):
                raise ConnectorError('OPERATION_CONFLICT', 'runtime_composition', 'The runtime has a different opening selection.')
            return existing
        async def checked_environment(prepared):
            if await resolve() != selection:
                raise ConnectorError('PROFILE_DRIFT', 'runtime_environment', 'The approved execution selection changed.')
            value = await environment(prepared)
            if await resolve() != selection:
                raise ConnectorError('PROFILE_DRIFT', 'runtime_environment', 'The approved execution selection changed.')
            return value
        native_owner = None
        async def native_launch(prepared, session_id, context):
            if native_owner is None:
                raise ConnectorError("BINDING_NOT_AUTHORIZED", "native_action_launch")
            return await native_owner.launch(prepared, session_id, context)
        runtime = create_runtime(journal=journal, environment=checked_environment,
            candidates={selection.candidate.adapter_id: selection.candidate},
            workspace_roots={selection.workspace_id: selection.workspace_root},
            native_factory=factory, owned_slot_ledger=ledger,
            pi_native_action=native_launch if native_action_factory is not None else None)
        if native_action_factory is not None:
            native_owner = native_action_factory(runtime)
            if not isinstance(native_owner, PiNativeActionOwner):
                raise ConnectorError("VALIDATION_ERROR", "native_action_launch",
                                     "The native action factory must return an owned Pi ingress.")
            self._native_action_owners[key] = native_owner
            self._native_action_factories[key] = native_action_factory
        self._runtimes[key] = runtime
        self._execution_selections[key] = selection
        return runtime

    async def build(self, binding: BindingRecord, *, environment,
                    factory=None, session_id: str | None = None
                    ) -> LocalRuntimeCore:
        """Build (or reuse) the runtime instance for one binding.

        Composed through the Core's public ``create_runtime`` (PC06),
        with the journal opened off the loop (C2 async entry). Keyed by
        the typed ``BindingKey`` (CN1/A03): a second Server with the
        same textual binding id gets its OWN instance and full
        candidate revalidation. ``session_id`` scopes the cache to ONE
        session so per-session credentials (CN1/A10, MCP capabilities)
        are never shared across launches of the same binding.
        ``factory`` is the documented trusted-host injection seam for
        contract tests; production always gets the real copied-adapter
        factory inside the Core.
        """
        key = BindingKey(binding.server_id, binding.binding_id)
        if session_id is not None:
            key = BindingKey(key.server_id,
                             f"{key.binding_id}#{session_id}")
        existing = self._runtimes.get(key)
        if existing is not None:
            return existing
        candidate = self.candidate_for(binding)
        journal = await self.ensure_journal()
        ledger = await self.ensure_ledger()
        runtime = create_runtime(
            journal=journal,
            environment=environment,
            candidates={binding.adapter_id: candidate},
            workspace_roots={binding.workspace_id: binding.workspace_root},
            native_factory=factory,
            owned_slot_ledger=ledger)
        self._runtimes[key] = runtime
        return runtime

    def get(self, binding_id: str,
            server_id: str | None = None) -> LocalRuntimeCore | None:
        """Fetch one runtime by binding id (optionally server-qualified).

        CN1/A03: without ``server_id`` a UNIQUE textual id resolves;
        ambiguity between Servers returns None rather than the wrong
        namespace's instance. Session-scoped entries never match.
        """
        if server_id is not None:
            return self._runtimes.get(BindingKey(server_id, binding_id))
        matches = [runtime for key, runtime in self._runtimes.items()
                   if isinstance(key, BindingKey) and key.binding_id == binding_id]
        return matches[0] if len(matches) == 1 else None

    @property
    def _journals(self) -> list[SQLiteJournal]:
        # Compatibility view for diagnostics that still close stores.
        return [self._journal] if self._journal is not None else []

    async def close_native_actions(self, key: ExecutionRuntimeKey, *, timeout_seconds=0):
        owner = self._native_action_owners.get(key)
        return owner is None or await owner.close(timeout_seconds=timeout_seconds)

    async def wait_executor_leases(self, *, server_id, executor_id, stop_event):
        """Retain disconnected runtimes until Core observes lease expiry.

        This is observation only: no lease is renewed and no runtime is
        recreated. An explicit daemon stop can proceed to normal shutdown.
        Storage/inspection failures propagate while ownership stays retained.
        """
        from nexus_connector_core import SessionKey

        while not stop_event.is_set():
            selected = [(key, runtime) for key, runtime in self._runtimes.items()
                        if isinstance(key, ExecutionRuntimeKey) and
                        (key.server_id, key.executor_id) == (server_id, executor_id)]
            if not selected:
                return
            snapshots = await asyncio.gather(*[
                runtime.inspect(SessionKey(key.server_id, key.executor_id, key.session_id))
                for key, runtime in selected], return_exceptions=True)
            for snapshot in snapshots:
                if isinstance(snapshot, BaseException):
                    raise snapshot
            if not any(snapshot.lease_state not in ("EXPIRED", "REVOKED", "CLOSED")
                       and snapshot.ownership != "released"
                       for snapshot in snapshots):
                return
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.1)
            except TimeoutError:
                pass

    async def shutdown_executor(self, *, server_id, executor_id):
        return await self.shutdown_all(_scope=(server_id, executor_id))

    async def shutdown_all(self, *, _scope=None) -> list[tuple[str, str]]:
        """Bounded shutdown; unknown outcomes are never discarded.

        CN1/A08+CN-05.03: an instance whose report leaves any session
        with UNKNOWN ownership keeps BOTH the instance and the shared
        stores alive — the next public lifecycle call re-contains the
        same resources (the Core's late-handle machinery converges
        them). Only fully-resolved instances are dropped, and the
        journal/ledger close exclusively when every obligation is
        resolved.
        """
        from nexus_connector_core import ShutdownPolicy

        outcomes: list[tuple[str, str]] = []
        any_pending = False
        for key, runtime in list(self._runtimes.items()):
            if _scope is not None and (not isinstance(key, ExecutionRuntimeKey) or
                    (key.server_id, key.executor_id) != _scope):
                continue
            owner = self._native_action_owners.get(key)
            if owner is not None:
                await owner.close(timeout_seconds=0)
            report = await runtime.shutdown(ShutdownPolicy(30.0, 15.0))
            native_pending = owner is not None and not await owner.close(timeout_seconds=0)
            resolved = not native_pending
            for session_key, outcome in report.session_outcomes.items():
                sid = getattr(session_key, "session_id", None)
                if sid is None and isinstance(session_key, tuple)                         and session_key:
                    sid = session_key[-1]
                if native_pending and session_key == owner.session_key:
                    outcome = "unknown"
                outcomes.append((sid, str(outcome)))
                if str(outcome) not in ("graceful", "already_closed",
                                        "forced"):
                    resolved = False
            if native_pending and owner.session_key not in report.session_outcomes:
                outcomes.append((key.session_id, "unknown"))
            if resolved:
                del self._runtimes[key]
                self._execution_selections.pop(key, None)
                self._native_action_owners.pop(key, None)
                self._native_action_factories.pop(key, None)
            else:
                any_pending = True
        if self._runtimes or not any_pending:
            # Stores stay open while any instance (or its obligations)
            # survives; with everything resolved, close through the
            # Core's public async lifecycle.
            pass
        if not self._runtimes:
            if self._journal is not None:
                await self._journal.aclose()
                self._journal = None
            if self._ledger is not None:
                await self._ledger.aclose()
                self._ledger = None
        return outcomes

    def storage_status(self):
        """Journal-level storage observation (diagnostics only)."""
        from nexus_connector_core.discovery import fingerprint
        marker = self.root / ".user-private"
        if not marker.exists():
            marker.write_text("probe\n", encoding="utf-8")
        candidate = InstallationCandidate(
            "codex_app_server", str(marker), fingerprint(marker),
            "explicit", "selected")
        probe = create_runtime(
            journal=self.journal, environment=_empty_environment,
            candidates={"codex_app_server": candidate},
            workspace_roots={"probe": str(self.root)},
            native_factory=_RejectingFactory())
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(probe.storage_status())
        finally:
            loop.close()


def _composite_fingerprint(executable: Path,
                           launch_script: str | None) -> str:
    from nexus_connector_core.discovery import fingerprint, selected_fingerprint
    from nexus_connector_core.models import InstallationCandidate
    if launch_script is None:
        return fingerprint(executable)
    probe = InstallationCandidate(
        "pi_rpc", str(executable), "", "explicit", "selected",
        launch_script=launch_script)
    return selected_fingerprint(probe)


def _current_build_identity(adapter_id: str, executable: Path,
                            launch_script: str | None) -> str | None:
    from nexus_connector_core.build_identity import (
        executable_build_identity, pi_build_identity,
    )
    try:
        if adapter_id == "pi_rpc" and launch_script is not None:
            # C2/R06: the hashed unit is the pi-coding-agent PACKAGE
            # directory (parents[2]); selection-time identities use the
            # same root, so revalidation stays consistent.
            package_root = Path(launch_script).parents[2]
            return pi_build_identity(executable, package_root)
        return executable_build_identity(executable)
    except (OSError, ValueError):
        return None


async def _empty_environment(prepared) -> Mapping[str, str]:
    return {}


class _RejectingFactory:  # pragma: no cover - diagnostic path only
    async def open(self, *_args, **_kwargs):
        raise ConnectorError("CAPABILITY_UNSUPPORTED", "diagnostic")


def monotonic_now() -> float:
    return time.monotonic()
