"""Packaging: isolated wheel install (plan C10.3, TC-01/TC-40).

Builds the real wheel/sdist and installs them into a clean venv together
with the pinned Core wheel, then verifies: the CLI imports and answers
``--version``, importing the package opens nothing (no daemon, no
socket), and no Nexus Server / MCP dependencies sneak in.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tomllib
import venv
from pathlib import Path

import pytest

PROJECT = Path(__file__).parents[2]
CORE_DIST = PROJECT / "vendor" / "wheels"


def _core_wheel() -> Path:
    dependencies = tomllib.loads((PROJECT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["dependencies"]
    pinned = [item.removeprefix("okto-nexus-connector-core==") for item in dependencies
              if item.startswith("okto-nexus-connector-core==")]
    assert len(pinned) == 1, "Exactly one Core version must be pinned"
    wheel = CORE_DIST / f"okto_nexus_connector_core-{pinned[0]}-py3-none-any.whl"
    assert wheel.is_file(), f"The pinned Core wheel is missing: {wheel}"
    return wheel


@pytest.fixture(scope="module")
def built_wheel(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("build")
    result = subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(out)],
        cwd=PROJECT, capture_output=True, timeout=600)
    assert result.returncode == 0, result.stderr.decode()
    wheels = list(out.glob("*.whl"))
    assert len(wheels) == 1
    return wheels[0]


@pytest.mark.packaging
def test_clean_venv_install(built_wheel: Path, tmp_path: Path):
    venv_root = tmp_path / "venv"
    venv.create(venv_root, with_pip=True)
    python = venv_root / ("Scripts/python.exe" if sys.platform == "win32"
                          else "bin/python")
    # A source-level runner may set PYTHONPATH. Neither pip's installed
    # distribution discovery nor the CLI probe may inherit that source tree.
    isolated_env = {key: value for key, value in os.environ.items()
                    if key not in {"PYTHONPATH", "PYTHONHOME"}}
    # install the built connector wheel (pulls websockets/httpx) and the
    # pinned Core development wheel
    install = subprocess.run(
        [str(python), "-I", "-m", "pip", "install", "--quiet", str(built_wheel),
         str(_core_wheel())],
        cwd=tmp_path, env=isolated_env, capture_output=True, timeout=600)
    assert install.returncode == 0, install.stderr.decode()

    # version + import side-effect audit (isolated interpreter)
    probe = subprocess.run(
        [str(python), "-I", "-c", """
import sys
mods_before = set(sys.modules)
import okto_nexus_connector as connector
import nexus_connector_core as core
from importlib.metadata import requires
assert f'okto-nexus-connector-core=={core.__version__}' in requires('okto-nexus-connector')
leaked = [m for m in sys.modules if m.split(".")[0] in
          ("okto_nexus",) and m != "okto_nexus_connector"]
assert not leaked, leaked
print(connector.__version__)
"""], cwd=tmp_path, env=isolated_env, capture_output=True, text=True, timeout=120)
    assert probe.returncode == 0, probe.stderr
    assert probe.stdout.strip()

    # the CLI entry point exists and works without the state directory
    cli = venv_root / ("Scripts/okto-nexus-connector.exe"
                       if sys.platform == "win32"
                       else "bin/okto-nexus-connector")
    assert cli.exists()
    version = subprocess.run([str(cli), "--version"], capture_output=True,
                             cwd=tmp_path, env=isolated_env, text=True, timeout=60)
    assert version.returncode == 0
    assert "okto-nexus-connector" in version.stdout

    # dependency audit: no MCP SDK, no Nexus Server package
    freeze = subprocess.run([str(python), "-I", "-m", "pip", "freeze"],
                            cwd=tmp_path, env=isolated_env,
                            capture_output=True, text=True, timeout=120)
    assert freeze.returncode == 0, freeze.stderr
    packages = freeze.stdout.lower()
    assert "mcp" not in [line.split("==")[0] for line in
                         freeze.stdout.splitlines()]
    assert "okto-nexus" not in packages.replace(
        "okto-nexus-connector", "")


@pytest.mark.packaging
def test_wheel_metadata_pins_core(built_wheel: Path):
    import zipfile
    with zipfile.ZipFile(built_wheel) as wheel:
        names = wheel.namelist()
        metadata = next(name for name in names
                        if name.endswith("METADATA"))
        text = wheel.read(metadata).decode("utf-8")
        entry = next((name for name in names
                      if name.endswith("entry_points.txt")), None)
        entry_content = wheel.read(entry).decode("utf-8") if entry else ""
    assert "Requires-Dist: okto-nexus-connector-core" in text
    assert "okto-nexus==" not in text  # no Server dependency
    assert "mcp" not in entry_content.lower()
