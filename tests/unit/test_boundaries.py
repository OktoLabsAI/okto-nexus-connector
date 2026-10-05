"""Structural boundary tests: no MCP surface, no duplicated natives.

TC-20: the Connector contains no native Popen/parser duplication — every
managed process goes through the Core. TC-25: no MCP server/proxy/relay
of any transport exists in the package; the only stdio use is the native
adapter protocols owned by the Core.
"""

from __future__ import annotations

import ast
import pytest
from pathlib import Path

import okto_nexus_connector
from okto_nexus_connector import __version__

PACKAGE = Path(okto_nexus_connector.__file__).parent
SRC = PACKAGE.parent


def _python_files():
    return [p for p in PACKAGE.rglob("*.py")]


def _mcp_imports(source):
    modules = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            modules.append(node.module or "")
    return [name for name in modules if name == "mcp" or name.startswith("mcp.")]


@pytest.mark.parametrize("source, forbidden", [
    ("import mcp", True),
    ("import json, mcp as sdk", True),
    ("import mcp.server", True),
    ("from mcp.server import Server", True),
    ("from mcp import ClientSession", True),
    ("from .mcp_launch import mcp_template", False),
    ("from nexus_connector_core.harness_config import harness_http_template", False),
])
def test_mcp_import_boundary_checks_modules(source, forbidden):
    assert bool(_mcp_imports(source)) is forbidden


def test_no_mcp_imports_or_entrypoints():
    """No MCP SDK dependency, no mcp entrypoints, no envelope handling."""
    for path in _python_files():
        source = path.read_text(encoding="utf-8")
        assert not _mcp_imports(source), path
        assert "ModelContextProtocol" not in source, path
    pyproject = SRC / "pyproject.toml"
    if pyproject.exists():
        text = pyproject.read_text(encoding="utf-8")
        assert '"mcp' not in text and "mcp==" not in text


def test_no_mcp_symbols_in_package():
    for path in _python_files():
        source = path.read_text(encoding="utf-8")
        lowered = source.lower()
        # Reading a harness's MCP *client config file* to import a key is
        # not an MCP implementation; only server/proxy symbols are banned.
        for forbidden in ("mcp_server", "mcp_proxy", "stdio_server",
                          "list_tools", "call_tool"):
            assert forbidden not in lowered, (path, forbidden)


def test_no_subprocess_spawn_outside_platform_layer():
    """Process creation is allowed only for the daemon spawn and OS
    service integration; native harness launches belong to the Core."""
    allowed = ("daemon/manager.py", "platform/service_install.py",
               "platform/sysinfo.py", "cli/main.py")
    for path in _python_files():
        relative = path.relative_to(PACKAGE).as_posix()
        if relative in allowed:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and node.id in (
                    "Popen", "spawn", "create_subprocess_exec",
                    "create_subprocess_shell"):
                raise AssertionError(f"{relative} spawns processes")
            if isinstance(node, ast.Attribute) and node.attr in (
                    "Popen", "create_subprocess_exec",
                    "create_subprocess_shell"):
                raise AssertionError(f"{relative} spawns processes")


def test_no_okto_nexus_server_dependency():
    for path in _python_files():
        source = path.read_text(encoding="utf-8")
        assert "import okto_nexus" not in source, path
        assert "from okto_nexus" not in source, path


def test_no_shell_true_or_dangerous_flags():
    for path in _python_files():
        source = path.read_text(encoding="utf-8")
        assert "shell=True" not in source, path
        assert "--dangerously-skip-permissions" not in source, path
        assert "verify=False" not in source.replace(
            "verify=False,  # documented test-only", "")


def test_version_and_core_contract_pinned():
    import nexus_connector_core
    assert nexus_connector_core.CONTRACT_REVISION == \
        "nxl-1-agent-centric-http-only-2026-09-25-r3"
    assert __version__
