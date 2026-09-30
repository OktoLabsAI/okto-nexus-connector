"""Host-owned, secret-free per-session MCP configuration."""
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import time

from nexus_connector_core.harness_config import harness_http_template, render_codex_toml_fragment
from nexus_connector_core.protocol import canonical_json

from ..errors import ConnectorError
from ..transport.https_client import R4SessionCapability, origin_of


MCP_SESSION_ACTIONS = (
    "tools/call", "resources/read", "prompts/get", "agent_whoami",
    "handoff_list_available", "handoff_get", "handoff_claim", "handoff_complete",
    "event_cursor", "event_get", "event_wait",
)


def _refuse():
    raise ConnectorError("PROFILE_DRIFT", "mcp_configuration",
                         "The session MCP configuration is missing or has changed.")


def _plain(path):
    if path.is_symlink() or (hasattr(path, "is_junction") and path.is_junction()):
        _refuse()


def _identity(path):
    _plain(path)
    stat = path.stat()
    return str(stat.st_dev), str(stat.st_ino)


@dataclass(frozen=True)
class SessionMCPHome:
    root: Path
    home: Path
    config: Path
    marker: bytes
    content: bytes
    identities: tuple

    def require_current(self):
        try:
            paths = (self.root, self.home, self.config.parent)
            if tuple(_identity(p) for p in paths) != self.identities:
                _refuse()
            for path, content in ((self.home / ".owner.json", self.marker), (self.config, self.content)):
                _plain(path)
                if path.stat().st_size != len(content) or path.read_bytes() != content:
                    _refuse()
        except OSError:
            _refuse()


def _write_once(path, content):
    _plain(path)
    try:
        with path.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        if path.stat().st_size != len(content) or path.read_bytes() != content:
            _refuse()


def session_mcp_home(root, *, frame, configuration_digest, template):
    """Create once; never overwrite a foreign owner or modified config."""
    root = Path(root).absolute()
    for ancestor in (root, *root.parents):
        _plain(ancestor)
    root.mkdir(parents=True, exist_ok=True)
    _plain(root)
    ownership = dict(layout="r4-mcp-v1", scope={k: frame[k] for k in (
        "server_id", "executor_id", "binding_id", "session_id", "session_owner_generation")},
        configuration_digest=configuration_digest, capability_ref=template.capability_ref)
    marker = canonical_json(ownership)
    home = root / hashlib.sha256(marker).hexdigest()
    if home.exists():
        _plain(home)
        marker_path = home / ".owner.json"
        _plain(marker_path)
        if (not marker_path.is_file() or marker_path.stat().st_size != len(marker)
                or marker_path.read_bytes() != marker):
            _refuse()
    else:
        home.mkdir()
        _write_once(home / ".owner.json", marker)
    if template.adapter_id == "codex_app_server":
        config = home / ".codex" / "config.toml"
        _plain(config.parent)
        config.parent.mkdir(exist_ok=True)
        content = render_codex_toml_fragment(template).encode("utf-8")
    else:
        config = home / ".claude.json"
        content = json.dumps({"mcpServers": {template.entry_name: template.entry()}},
                             sort_keys=True, separators=(",", ":")).encode("utf-8")
    _write_once(config, content)
    result = SessionMCPHome(root, home, config, marker, content,
                           tuple(_identity(p) for p in (root, home, config.parent)))
    result.require_current()
    return result


def mcp_template(capability, *, adapter_id, approved_origin, process_http=False):
    """The HTTP exchange validates the result; composition validates it again."""
    cap = capability
    if (not isinstance(cap, R4SessionCapability) or cap.audience != "nexus-mcp-session"
            or cap.capability_ref != "mcp-cap:" + cap.capability_id
            or type(cap.capability) is not str or not cap.capability.startswith("nxc4_")
            or not 32 <= len(cap.capability) <= 4096
            or type(cap.deadline_monotonic) not in (int, float)
            or not math.isfinite(cap.deadline_monotonic)
            or cap.deadline_monotonic <= time.monotonic()
            or set(cap.actions) != set(MCP_SESSION_ACTIONS)):
        raise ConnectorError("AUTH_EXPIRED", "mcp_configuration",
                             "The session MCP capability is invalid or expired.")
    if not cap.mcp_url or origin_of(cap.mcp_url) != approved_origin:
        raise ConnectorError("PROFILE_DRIFT", "mcp_configuration",
                             "The session MCP URL differs from the approved Server origin.")
    # Native build qualification remains a Core launch gate. This selects
    # the existing public config format; it does not qualify a provider build.
    from urllib.parse import urlsplit
    loopback = urlsplit(cap.mcp_url).hostname in ("localhost", "127.0.0.1", "::1")
    return harness_http_template(adapter_id, cap.mcp_url, cap.capability_ref,
        entry_name="nexus_"+hashlib.sha256(capability.capability_ref.encode()).hexdigest()[:16] if process_http else "nexus",
        approved_origins={approved_origin},
        harness_is_local=loopback, loopback_reachable=loopback, format_qualified=True)
