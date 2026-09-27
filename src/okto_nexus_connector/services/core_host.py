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
from nexus_connector_core.journal import SQLiteJournal
from nexus_connector_core.ports import SecretResolver

from ..errors import ConnectorError
from ..platform import paths
from ..storage.state_store import BindingRecord


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
            return self._vault.resolve(reference)
        if reference.startswith("provider:"):
            return self._vault.resolve(f"vault:{reference}")
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
        self._runtimes: dict[str, LocalRuntimeCore] = {}
        self._journal: SQLiteJournal | None = None
        self._ledger: SQLiteOwnedSlotLedger | None = None

    # -- shared installation stores -------------------------------------

    @property
    def journal(self) -> SQLiteJournal:
        """One shared journal (single off-loop worker) for the daemon."""
        if self._journal is None:
            self._journal = SQLiteJournal(self._journal_path)
        return self._journal

    @property
    def ledger(self) -> SQLiteOwnedSlotLedger:
        if self._ledger is None:
            self._ledger = SQLiteOwnedSlotLedger(self._ledger_path)
        return self._ledger

    # -- candidates -------------------------------------------------------

    def candidate_for(self, binding: BindingRecord) -> InstallationCandidate:
        """Revalidate the selected binary before every launch (A.6).

        Checks the path-bound fingerprint AND, when recorded, the portable
        ``build_identity`` (Core 0.2.0 / PC09): moving the installation
        still invalidates the local binding, while content drift is caught
        even when the path stays.
        """
        from nexus_connector_core.discovery import fingerprint

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
        return InstallationCandidate(
            adapter_id=binding.adapter_id,
            executable=str(executable),
            fingerprint=binding.candidate_fingerprint,
            source="explicit",
            trust="selected",
            version=binding.candidate_version or None,
            launch_script=launch_script,
            build_identity=build_identity,
        )

    # -- runtime composition ------------------------------------------------

    def build(self, binding: BindingRecord, *, environment,
              factory=None) -> LocalRuntimeCore:
        """Build (or reuse) the runtime instance for one binding.

        Composed through the Core's public ``create_runtime`` (PC06).
        ``factory`` is the documented trusted-host injection seam for
        contract tests; production always gets the real copied-adapter
        factory inside the Core.
        """
        existing = self._runtimes.get(binding.binding_id)
        if existing is not None:
            return existing
        candidate = self.candidate_for(binding)
        runtime = create_runtime(
            journal=self.journal,
            environment=environment,
            candidates={binding.adapter_id: candidate},
            workspace_roots={binding.workspace_id: binding.workspace_root},
            native_factory=factory,
            owned_slot_ledger=self.ledger)
        self._runtimes[binding.binding_id] = runtime
        return runtime

    def get(self, binding_id: str) -> LocalRuntimeCore | None:
        return self._runtimes.get(binding_id)

    @property
    def _journals(self) -> list[SQLiteJournal]:
        # Compatibility view for diagnostics that still close stores.
        return [self._journal] if self._journal is not None else []

    async def shutdown_all(self) -> list[tuple[str, str]]:
        """Bounded shutdown of every runtime this daemon built (C02.5)."""
        from nexus_connector_core import ShutdownPolicy

        outcomes: list[tuple[str, str]] = []
        for binding_id, runtime in list(self._runtimes.items()):
            report = await runtime.shutdown(ShutdownPolicy(30.0, 15.0))
            for key, outcome in report.session_outcomes.items():
                outcomes.append((key.session_id, str(outcome)))
            del self._runtimes[binding_id]
        if self._journal is not None:
            self._journal.close()
            self._journal = None
        if self._ledger is not None:
            self._ledger.close()
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
            package_root = Path(launch_script).parents[3]
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
