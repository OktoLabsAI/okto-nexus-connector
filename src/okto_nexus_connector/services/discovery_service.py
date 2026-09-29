"""Local harness inventory via the Core, without executing candidates.

Discovery never authorizes anything and never collects secrets (plan 2.3,
TC-11): candidates found on PATH, through passive npm-shim resolution
(Core 0.2.0 / RC-10-03) or from Pi release layouts (PC10) are listed
redacted; selection happens through explicit operator choice at connect
time. The Core's sealed version probes are used only for explicitly
selected candidates and return the portable build identity (PC09).
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path

from nexus_connector_core import (
    CoreError, DiscoveryRequest, Inventory, get_runtime_catalog,
)
from nexus_connector_core.catalog import RuntimeDescriptor
from nexus_connector_core.discovery import (
    candidate as make_candidate,
    discover_path,
)

from ..errors import ConnectorError

# C9/C01 contract: the Core's public catalog is the single source for
# which adapters exist, their display names and whether they admit
# discovery. The connector keeps NO host-side adapter arrays; the only
# local table below is npm-shim naming (an npm packaging detail, not
# adapter registry knowledge).

#: npm command names for Windows shim resolution, keyed by the catalog's
#: ``native_kind`` ("claude_code" ships as the ``claude`` command).
_NPM_COMMAND_BY_KIND = {"codex": "codex", "pi": "pi",
                        "claude_code": "claude"}


def catalog_runtimes(*, discoverable_only: bool = False
                     ) -> tuple[RuntimeDescriptor, ...]:
    """Managed runtimes this Core build knows (public catalog)."""
    return tuple(
        descriptor for descriptor in get_runtime_catalog().runtimes
        if descriptor.connection_mode == "managed"
        and descriptor.support_status == "managed_supported"
        and (not discoverable_only or descriptor.discoverable))


def adapter_ids() -> tuple[str, ...]:
    return tuple(descriptor.adapter_id
                 for descriptor in catalog_runtimes())


def display_name(adapter_id: str) -> str:
    for descriptor in get_runtime_catalog().runtimes:
        if descriptor.adapter_id == adapter_id:
            return descriptor.display_name
    return adapter_id


def known_adapter(adapter_id: str) -> bool:
    return adapter_id in adapter_ids()


# Backwards-compatible module constants (derived from the catalog at
# import; the catalog is sync, side-effect-free and immutable).
ADAPTERS = adapter_ids()
_SHIM_NAMES = {
    "codex_app_server": "codex",
    "pi_rpc": "pi",
    "claude_stream": "claude",
}


@dataclass(slots=True)
class InventoryEntry:
    adapter_id: str
    executable: str
    source: str
    trust: str
    fingerprint: str
    version: str | None
    architecture: str | None
    build_identity: str | None = None
    launch_script: str | None = None

    def to_json(self) -> dict[str, object]:
        return {
            "adapter_id": self.adapter_id,
            "label": display_name(self.adapter_id),
            "executable": self.executable,
            "source": self.source,
            "trust": self.trust,
            "fingerprint": self.fingerprint,
            "version": self.version,
            "architecture": self.architecture,
            "build_identity": self.build_identity,
            "launch_script": self.launch_script,
        }


def _entry_of(found) -> InventoryEntry:
    return InventoryEntry(
        adapter_id=found.adapter_id,
        executable=found.executable,
        source=found.source,
        trust=found.trust,
        fingerprint=found.fingerprint,
        version=found.version,
        architecture=found.architecture,
        build_identity=found.build_identity,
        launch_script=found.launch_script)


async def discover_inventory(adapter_ids=None) -> list[InventoryEntry]:
    """Inventory over the catalog's discoverable managed adapters.

    ``adapter_ids=None`` (the default) asks the catalog — the caller
    never needs its own adapter array (C9/C01).
    """
    return [_entry_of(found) for found in
            await inventory_candidates(adapter_ids)]


async def inventory_candidates(adapter_ids=None, *, extra=()) -> list:
    """CN4-04.01: the FULL Core ``InstallationCandidate`` objects.

    The candidates the Core discovered are preserved EXACTLY as
    produced (executable, launch_script, fingerprint, build_identity,
    version, architecture, trust/source/installation_ref) — consumers
    (CLI and IPC) pass THESE to the availability evaluation instead of
    re-creating candidates from executable paths, which silently
    dropped the Pi pair's CLI identity/version/build. ``extra``
    appends operator-named candidates (e.g. the Pi release pair) to the
    same single effective inventory.
    """
    if adapter_ids is None:
        adapter_ids = tuple(
            descriptor.adapter_id for descriptor in
            catalog_runtimes(discoverable_only=True))
    found: list = []
    for adapter_id in adapter_ids:
        try:
            candidates = await asyncio.to_thread(
                discover_path, adapter_id)
        except CoreError:
            continue
        found.extend(candidates)
    # Passive npm-shim resolution on Windows (Core 0.2.0): .cmd wrappers
    # are parsed, never executed; only the two documented shim shapes
    # yield candidates, everything else is refused honestly.
    if os.name == "nt":
        for adapter_id in adapter_ids:
            for candidate in await asyncio.to_thread(
                    shim_candidates, adapter_id):
                if all(entry.executable != candidate.executable
                       for entry in found):
                    found.append(candidate)
    for candidate in extra:
        if all(entry.executable != candidate.executable
               or entry.launch_script != candidate.launch_script
               for entry in found):
            found.append(candidate)
    return found


def shim_candidates(adapter_id: str, *,
                    path_env: str | None = None
                    ) -> list[object]:
    """Resolve known npm ``.cmd`` shims into real candidates, passively.

    The wrapper itself is never executed (RC-10-03). Refused shapes —
    dynamic expansion, non-.js targets, URLs — simply yield nothing;
    explicit ``--executable`` remains the guaranteed selection path.
    """
    from nexus_connector_core.discovery import resolve_windows_npm_shim
    if os.name != "nt" or not known_adapter(adapter_id):
        return []
    command = _npm_command(adapter_id)
    if command is None:
        return []
    found: dict[str, object] = {}
    for directory in (path_env if path_env is not None else
                      os.environ.get("PATH", "")).split(os.pathsep):
        if not directory:
            continue
        shim = Path(directory) / f"{command}.cmd"
        if not shim.is_file():
            continue
        try:
            target = resolve_windows_npm_shim(shim)
            candidate = make_candidate(adapter_id, target, explicit=True)
        except (CoreError, OSError):
            continue
        found[candidate.executable] = candidate
    return list(found.values())


def pi_release_candidates(install_root: str | Path, node_path: str | Path,
                          *, trusted_roots: tuple[Path, ...] = ()):
    """Passive Pi release-layout discovery (Core 0.2.0 / PC10)."""
    from nexus_connector_core.discovery import discover_pi_releases
    try:
        return list(discover_pi_releases(install_root, node_path,
                                         trusted_roots=trusted_roots))
    except CoreError as error:
        raise ConnectorError(error.code, "discovery", str(error)) from None


def _npm_command(adapter_id: str) -> str | None:
    """npm command name for one adapter, via the catalog's native kind."""
    for descriptor in catalog_runtimes():
        if descriptor.adapter_id == adapter_id:
            return _NPM_COMMAND_BY_KIND.get(descriptor.native_kind)
    return None


def select_explicit(adapter_id: str, executable: str | Path):
    """Build a selected candidate from an explicit operator choice."""
    try:
        return make_candidate(adapter_id, executable, explicit=True)
    except CoreError as error:
        raise ConnectorError(error.code, "selection",
                             str(error), retry_safe=error.retry_safe) from None


async def probe_version(selected):
    """Sealed, secret-free ``--version`` probe for one selected candidate.

    Returns the Core's updated candidate, carrying the observed version
    and the portable ``build_identity`` when the layout provides one.
    """
    import os
    from nexus_connector_core.discovery import (
        probe_selected_claude, probe_selected_codex, probe_selected_pi,
    )
    # Only whitelisted essentials reach the probe; never credentials.
    essentials = frozenset({"SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT",
                            "PATH", "TEMP", "TMP", "LANG", "LC_ALL",
                            "TERM"})
    env = {key: value for key, value in os.environ.items()
           if key.upper() in essentials}
    try:
        if selected.adapter_id == "codex_app_server":
            probed = await probe_selected_codex(selected, cwd=Path.cwd(),
                                                env=env)
        elif selected.adapter_id == "claude_stream":
            probed = await probe_selected_claude(selected, cwd=Path.cwd(),
                                                 env=env)
        elif selected.adapter_id == "pi_rpc":
            probed = await probe_selected_pi(selected, cwd=Path.cwd(),
                                             env=env)
        else:
            return selected
        return probed
    except CoreError as error:
        raise ConnectorError(error.code, "probe", str(error)) from None
    except Exception:
        # A probe that cannot run or parse simply yields no version;
        # selection and qualification happen through the Core anyway.
        return selected


def pi_candidate(node: str | Path, script: str | Path):
    """Composite Node+CLI selection for Pi (fingerprint binds both)."""
    from nexus_connector_core.discovery import candidate_pi_node_cli
    try:
        return candidate_pi_node_cli(node, script, explicit=True)
    except CoreError as error:
        raise ConnectorError(error.code, "selection", str(error)) from None


def containment_status() -> dict[str, str]:
    """Passive containment preflight (Core 0.2.0 / PC11) for doctor."""
    from nexus_connector_core.native.process import containment_preflight
    return containment_preflight()


def evaluate_availability(candidates):
    """CN2/N09 (CN-07.04): the Core's PUBLIC per-candidate technical
    availability assessment — never a connector-side allowlist."""
    from nexus_connector_core import evaluate_runtime_availability
    return evaluate_runtime_availability(candidates)


def availability_snapshot(candidates) -> dict[str, object]:
    """Local IPC/CLI preview with the Core's complete R4 evidence revision.

    This legacy preview is not the authenticated publication envelope. The
    producing host supplies its real IDs and sequence when publishing R4.
    """
    from nexus_connector_core import calculate_inventory_revision
    report = evaluate_availability(candidates)
    projection = report.to_dict()
    rows = projection["availability"]
    revision = calculate_inventory_revision(candidates, availability=report)
    return {
        "format_version": projection["format_version"],
        "platform": projection["platform"],
        "executor_revision": revision,
        "rows": rows,
    }


def executor_inventory_snapshot(candidates, *, server_id: str, executor_id: str,
                                producer_instance_id: str,
                                publication_sequence: int,
                                observation_age_ms: int = 0) -> dict[str, object]:
    """R4 HTTP publication shape; the caller retains the full candidates."""
    from nexus_connector_core import build_executor_inventory_snapshot
    return build_executor_inventory_snapshot(
        candidates, server_id=server_id, executor_id=executor_id,
        producer_instance_id=producer_instance_id,
        publication_sequence=publication_sequence,
        observation_age_ms=observation_age_ms,
    )


def resolve_executor_installation(candidates, *, adapter_id: str,
                                  candidate_ref: str,
                                  expected_inventory_revision: str):
    """Select only within this executor's current, full Core inventory."""
    from nexus_connector_core import calculate_inventory_revision, resolve_installation

    items = tuple(candidates)
    if calculate_inventory_revision(items) != expected_inventory_revision:
        raise ConnectorError("STALE_GENERATION", "inventory_selection",
                             "The selected inventory revision is stale")
    try:
        return resolve_installation(items, adapter_id, candidate_ref)
    except CoreError as error:
        raise ConnectorError(error.code, "inventory_selection", str(error),
                             retry_safe=error.retry_safe) from None


def resolve_selection(candidates, adapter_id: str, candidate_ref: str):
    """CN2/N09: exact ONE-candidate resolution via the Core's public
    resolver (typed REF_NOT_FOUND / REF_AMBIGUOUS; never index 0)."""
    from nexus_connector_core import resolve_installation
    try:
        return resolve_installation(candidates, adapter_id, candidate_ref)
    except CoreError as error:
        raise ConnectorError(error.code, "selection", error.message or
                             error.code,
                             action="Reselect the installation "
                                    "explicitly; identical copies are "
                                    "ambiguous by construction.") from None
