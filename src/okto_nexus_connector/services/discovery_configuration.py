"""Persist local discovery choices without granting runtime authority."""
from __future__ import annotations

import asyncio
from pathlib import Path

from nexus_connector_core import discover_installations

from ..errors import ConnectorError


def _error(message, code="VALIDATION_ERROR"):
    return ConnectorError(code, "discovery_configuration", message)


def _path(raw, *, file=False):
    if not isinstance(raw, str) or not raw or not Path(raw).is_absolute():
        raise _error("Discovery paths must be existing absolute local paths.")
    try:
        path = Path(raw).resolve(strict=True)
        if not (path.is_file() if file else path.is_dir()):
            raise ValueError()
        stat = path.stat()
    except (OSError, ValueError):
        raise _error("An approved discovery path is unavailable.", "PROFILE_DRIFT") from None
    return dict(path=str(path), device=str(stat.st_dev), inode=str(stat.st_ino))


def configuration_arguments(value):
    """Validate stored shape and physical path identity before passive discovery."""
    if value == {}:
        return {}
    if (not isinstance(value, dict) or set(value) !=
            {"format_version", "roots", "pi_install_root", "pi_node"} or
            type(value["format_version"]) is not int or value["format_version"] != 1 or
            not isinstance(value["roots"], list) or len(value["roots"]) > 32 or
            (value["pi_install_root"] is None) != (value["pi_node"] is None)):
        raise _error("The stored discovery configuration is invalid.")
    def checked(entry, *, file=False):
        if not isinstance(entry, dict) or set(entry) != {"path", "device", "inode"}:
            raise _error("The stored discovery path is invalid.")
        if _path(entry["path"], file=file) != entry:
            raise _error("An approved discovery path has changed.", "PROFILE_DRIFT")
        return Path(entry["path"])
    roots = tuple(checked(entry) for entry in value["roots"])
    install = checked(value["pi_install_root"]) if value["pi_install_root"] is not None else None
    node = checked(value["pi_node"], file=True) if value["pi_node"] is not None else None
    trusted = roots + ((install,) if install is not None and install not in roots else ())
    # Approve only the named Node file, not its parent directory.
    if node is not None and not any(node.is_relative_to(root) for root in trusted):
        trusted += (node,)
    return dict(trusted_roots=trusted, pi_install_root=install, pi_node=node)


def configure_discovery(store, *, server_id, roots=(), pi_install_root=None, pi_node=None):
    if len(roots) > 32 or (pi_install_root is None) != (pi_node is None):
        raise _error("Use at most 32 roots and supply both Pi discovery paths together.")
    value = dict(format_version=1, roots=[_path(str(root)) for root in roots],
                 pi_install_root=_path(str(pi_install_root)) if pi_install_root is not None else None,
                 pi_node=_path(str(pi_node), file=True) if pi_node is not None else None)
    def update(state):
        matches = [r for r in state.execution_executors if r.server_id == server_id]
        if len(matches) != 1 or matches[0].state != "REGISTERED" or not matches[0].executor_id:
            raise _error("Register this Server executor before configuring discovery.")
        configuration_arguments(value)
        matches[0].discovery_configuration = value
    store.update(update)
    return value


async def configured_candidates(configuration, *, adapter_ids=None):
    """Use the public Core facade and preserve complete InstallationCandidate values."""
    def discover():
        arguments = configuration_arguments(configuration)
        inventory = discover_installations(adapter_ids=adapter_ids, **arguments)
        configuration_arguments(configuration)
        return list(inventory.candidates)
    return await asyncio.to_thread(discover)
