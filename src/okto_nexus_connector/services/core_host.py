"""Composition of ``nexus_connector_core`` for one connector installation.

The Connector never duplicates native Popen/parsers (TC-20): every managed
launch goes through ``LocalRuntimeCore`` with the Core's real copied-adapter
factory. One technical journal and one installation-wide owned-slot ledger
are shared by all bindings; each binding gets its own runtime instance with
its selected candidate and workspace root.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from nexus_connector_core import (
    InstallationCandidate, LocalRuntimeCore, SQLiteOwnedSlotLedger,
)
from nexus_connector_core.environment import child_environment
from nexus_connector_core.harness_config import HarnessHTTPTemplate
from nexus_connector_core.journal import SQLiteJournal
from nexus_connector_core.native.runtime_bridge import CopiedAdapterFactory
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
        self._journals: list[SQLiteJournal] = []
        self._ledgers: list[SQLiteOwnedSlotLedger] = []

    def candidate_for(self, binding: BindingRecord) -> InstallationCandidate:
        """Revalidate the selected binary before every launch (A.6)."""
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
        current = fingerprint(executable)
        if current != binding.candidate_fingerprint:
            raise ConnectorError(
                "PROFILE_DRIFT", "candidate",
                "the selected binary changed since the binding was created",
                action="Re-run 'okto-nexus-connector bind create' after "
                       "verifying the update, or reinstall the previous "
                       "version.")
        return InstallationCandidate(
            adapter_id=binding.adapter_id,
            executable=str(executable),
            fingerprint=binding.candidate_fingerprint,
            source="explicit",
            trust="selected",
            version=binding.candidate_version or None,
        )

    def build(self, binding: BindingRecord, *, environment,
              factory=None) -> LocalRuntimeCore:
        """Build (or reuse) the runtime instance for one binding.

        ``factory`` is a trusted-host injection seam for contract tests;
        production always uses the Core's copied-adapter factory.
        """
        existing = self._runtimes.get(binding.binding_id)
        if existing is not None:
            return existing
        candidate = self.candidate_for(binding)
        journal = SQLiteJournal(self._journal_path)
        ledger = SQLiteOwnedSlotLedger(self._ledger_path)
        native = factory if factory is not None \
            else CopiedAdapterFactory(environment)
        runtime = LocalRuntimeCore(
            journal, native,
            candidates={binding.adapter_id: candidate},
            workspace_roots={binding.workspace_id: binding.workspace_root},
            owned_slot_ledger=ledger)
        self._runtimes[binding.binding_id] = runtime
        self._journals.append(journal)
        self._ledgers.append(ledger)
        return runtime

    def get(self, binding_id: str) -> LocalRuntimeCore | None:
        return self._runtimes.get(binding_id)

    async def shutdown_all(self) -> list[tuple[str, str]]:
        """Bounded shutdown of every runtime this daemon built (C02.5)."""
        from nexus_connector_core import ShutdownPolicy

        outcomes: list[tuple[str, str]] = []
        for binding_id, runtime in list(self._runtimes.items()):
            report = await runtime.shutdown(ShutdownPolicy(30.0, 15.0))
            for key, outcome in report.session_outcomes.items():
                outcomes.append((key.session_id, str(outcome)))
            del self._runtimes[binding_id]
        for journal in self._journals:
            journal.close()
        self._journals.clear()
        self._ledgers.clear()
        return outcomes

    def storage_status(self):
        runtime = next(iter(self._runtimes.values()), None)
        if runtime is None:
            journal = SQLiteJournal(self._journal_path)
            runtime = LocalRuntimeCore(
                journal, _rejecting_factory(), candidates={},
                workspace_roots={})
            try:
                import asyncio
                return asyncio.get_event_loop().run_until_complete(
                    runtime.storage_status())
            finally:
                journal.close()
        import asyncio
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not None:
            raise ConnectorError("DAEMON_UNAVAILABLE", "storage_status",
                                 "call from the daemon loop")
        return asyncio.new_event_loop().run_until_complete(
            runtime.storage_status())


def _rejecting_factory():  # pragma: no cover - diagnostic path only
    class _Rejecting:
        async def open(self, *_args, **_kwargs):
            raise ConnectorError("CAPABILITY_UNSUPPORTED", "diagnostic")
    return _Rejecting()


def monotonic_now() -> float:
    return time.monotonic()
