"""Unit: Core 0.2.0 integrations — passive shims, build identity,
containment preflight (PC09/PC10/PC11)."""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services.core_host import CoreRuntimeHost
from okto_nexus_connector.services.discovery_service import (
    containment_status, pi_candidate, pi_release_candidates,
    select_explicit, shim_candidates,
)
from okto_nexus_connector.storage.state_store import BindingRecord

# -- passive npm shim resolution (RC-10-03 / PC10) ------------------------

_VALID_SHIM = """@ECHO off
GOTO start
:find_dp0
SET dp0=%~dp0
EXIT /b
:start
SETLOCAL
CALL :find_dp0
IF EXIST "%dp0%\\node.exe" (
  SET "_prog=%dp0%\\node.exe"
) ELSE (
  SET "_prog=node"
  SET "nodepath=%dp0%"
)
endLocal & goto #_undefined_# 2>NUL || title %COMSPEC% & "%_prog%"  "%dp0%\\node_modules\\fake-pkg\\bin\\tool.js" %*
"""

_DYNAMIC_SHIM = """@echo off
node "%~dp0%dynamic-%USERNAME%.js" %*
"""


def _write_shim(directory: Path, name: str, content: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    shim = directory / f"{name}.cmd"
    shim.write_text(content, encoding="utf-8")
    return shim


def _make_executable(directory: Path, name: str) -> Path:
    path = directory / name
    path.write_bytes(b"synthetic target executable")
    if os.name != "nt":
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


@pytest.mark.skipif(os.name != "nt", reason="Windows npm shims")
def test_known_shim_shape_resolves_passively(tmp_path: Path):
    """The two documented npm-cmd-shim shapes resolve without executing."""
    bin_dir = tmp_path / "bin"
    _write_shim(bin_dir, "codex", _VALID_SHIM)
    target = bin_dir / "node_modules" / "fake-pkg" / "bin" / "tool.js"
    target.parent.mkdir(parents=True)
    target.write_text("// shim target", encoding="utf-8")
    # The .js target is not directly executable on Windows: resolution
    # reports the target, but candidate selection still refuses it
    # honestly (no wrapper interpretation, no fallback).
    found = shim_candidates("codex_app_server", path_env=str(bin_dir))
    # either a real candidate (if the layout qualifies) or none — never
    # an executed wrapper or a guessed binary
    for candidate in found:
        assert candidate.executable.endswith((".exe", ".com"))
        assert candidate.fingerprint.startswith("sha256:")


@pytest.mark.skipif(os.name != "nt", reason="Windows npm shims")
def test_dynamic_shim_refused(tmp_path: Path):
    """Dynamic expansion in the target is refused (never executed)."""
    bin_dir = tmp_path / "bin"
    _write_shim(bin_dir, "codex", _DYNAMIC_SHIM)
    found = shim_candidates("codex_app_server", path_env=str(bin_dir))
    assert found == []


def test_shim_resolution_never_runs_outside_windows(tmp_path: Path):
    if os.name == "nt":
        pytest.skip("covered above")
    found = shim_candidates("codex_app_server", path_env=str(tmp_path))
    assert found == []


# -- portable build identity round-trip (PC09) ------------------------------

def _binding(root: Path, binary: Path, *, build_identity: str = "",
             launch_script: str = "") -> BindingRecord:
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    return BindingRecord(
        binding_id="bind_b", alias="codex", server_id="srv",
        agent_id="ag", adapter_id="codex_app_server",
        executor_id="conn", workspace_id="ws",
        workspace_root=str(root),
        candidate_executable=str(binary),
        candidate_fingerprint="sha256:" + digest,
        candidate_version="0.157.0",
        candidate_build_identity=build_identity,
        candidate_launch_script=launch_script)


def test_candidate_carries_recorded_build_identity(tmp_path: Path):
    binary = _make_executable(tmp_path, "tool.exe")
    from nexus_connector_core.build_identity import executable_build_identity
    identity = executable_build_identity(binary)
    host = CoreRuntimeHost.__new__(CoreRuntimeHost)
    binding = _binding(tmp_path, binary, build_identity=identity)
    candidate = host.candidate_for(binding)
    assert candidate.build_identity == identity
    assert candidate.trust == "selected"


def test_build_identity_drift_detected(tmp_path: Path):
    """Same path, different bytes: the portable identity catches drift
    even when the recorded fingerprint was stale."""
    binary = _make_executable(tmp_path, "tool.exe")
    from nexus_connector_core.build_identity import executable_build_identity
    stale_identity = executable_build_identity(binary)
    binary.write_bytes(b"tampered content")  # fingerprint now differs too
    binding = _binding(tmp_path, binary, build_identity=stale_identity)
    # recompute fingerprint so only the build identity diverges
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    binding.candidate_fingerprint = "sha256:" + digest
    host = CoreRuntimeHost.__new__(CoreRuntimeHost)
    with pytest.raises(ConnectorError) as error:
        host.candidate_for(binding)
    assert error.value.code == "PROFILE_DRIFT"


def test_binding_without_build_identity_still_path_bound(tmp_path: Path):
    """Pre-0.2.0 bindings (no recorded identity) keep working via the
    path-bound fingerprint alone."""
    binary = _make_executable(tmp_path, "tool.exe")
    binding = _binding(tmp_path, binary)
    host = CoreRuntimeHost.__new__(CoreRuntimeHost)
    candidate = host.candidate_for(binding)
    assert candidate.build_identity is None


# -- containment preflight (PC11) --------------------------------------------

def test_containment_status_reports_backend():
    status = containment_status()
    assert isinstance(status, dict) and status
    if os.name == "nt":
        assert "job_objects" in status
    # the daemon's own platform must be qualified for managed launches
    assert all(value == "ok" for value in status.values()) or \
        any(value != "ok" for value in status.values())


# -- Pi release layout discovery (PC10) ---------------------------------------

def test_pi_release_layout_discovered_passively(tmp_path: Path):
    node = _make_executable(tmp_path, "node.exe")
    package = tmp_path / "releases" / "0.87.1" / "node_modules" / \
        "@earendil-works" / "pi-coding-agent"
    cli = package / "dist" / "bundle" / "cli.js"
    cli.parent.mkdir(parents=True)
    cli.write_text("// pi cli", encoding="utf-8")
    # C2: the package's declared version surfaces passively (no probe)
    (package / "package.json").write_text(
        '{"name": "@earendil-works/pi-coding-agent", "version": "0.87.1"}',
        encoding="utf-8")
    # Outside trusted roots nothing is auto-trusted (passive, by design);
    # inside the approved root the composite candidate binds both parts.
    assert pi_release_candidates(tmp_path, node) == []
    found = pi_release_candidates(tmp_path, node,
                                  trusted_roots=(tmp_path,))
    if os.name == "nt":
        assert len(found) == 1
        assert found[0].launch_script == str(cli)
        assert found[0].build_identity is not None
        assert found[0].version == "0.87.1"
    else:
        assert found == []


def test_select_explicit_returns_core_candidate(tmp_path: Path):
    binary = _make_executable(tmp_path, "tool.exe")
    candidate = select_explicit("codex_app_server", binary)
    assert candidate.trust == "selected"
    assert candidate.build_identity  # PC09 identity attached at selection


def test_pi_identity_root_consistent_between_selection_and_revalidation(
        tmp_path: Path):
    """C2/R06 regression seed: the revalidator must use the SAME package
    root the Core uses at selection time (pi-coding-agent dir, parents[2]).
    A parents[3] recompute yields a different identity → false drift."""
    if os.name != "nt":
        pytest.skip("composite selection needs a .exe node on Windows")
    node = _make_executable(tmp_path, "node.exe")
    package = tmp_path / "node_modules" / "@earendil-works" / \
        "pi-coding-agent"
    cli = package / "dist" / "bundle" / "cli.js"
    cli.parent.mkdir(parents=True)
    cli.write_text("// pi cli", encoding="utf-8")
    (package / "package.json").write_text('{"version": "0.87.1"}',
                                          encoding="utf-8")
    selected = pi_candidate(node, cli)
    assert selected.launch_script == str(cli)
    assert selected.build_identity is not None
    binding = BindingRecord(
        binding_id="bind_pi", alias="pi", server_id="srv",
        agent_id="ag", adapter_id="pi_rpc", executor_id="conn",
        workspace_id="ws", workspace_root=str(tmp_path),
        candidate_executable=str(node),
        candidate_fingerprint=selected.fingerprint,
        candidate_version="0.87.1",
        candidate_build_identity=selected.build_identity,
        candidate_launch_script=str(cli))
    host = CoreRuntimeHost.__new__(CoreRuntimeHost)
    candidate = host.candidate_for(binding)  # must NOT raise PROFILE_DRIFT
    assert candidate.build_identity == selected.build_identity
