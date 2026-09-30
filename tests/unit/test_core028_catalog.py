"""Unit: Core 0.2.8 integrations — the public runtime catalog (C9/C01).

The catalog is the single source: the connector keeps no host-side
adapter arrays; ``claude_attach`` surfaces as registered-unqualified and
never enters discovery flows merely by existing.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from nexus_connector_core import get_runtime_catalog

from okto_nexus_connector.services.discovery_service import (
    adapter_ids, catalog_runtimes, discover_inventory, display_name,
    known_adapter, select_explicit, shim_candidates,
)


def test_catalog_is_the_single_source():
    catalog = get_runtime_catalog()
    assert catalog.format_version == 2
    assert all(descriptor.control_targeting for descriptor in catalog_runtimes())
    pi = next(descriptor for descriptor in catalog_runtimes() if descriptor.adapter_id == "pi_rpc")
    assert next(control for control in pi.control_targeting
                if control.action == "turn.steer").steer_timing == "NEXT_TURN_BOUNDARY"
    managed = {d.adapter_id for d in catalog_runtimes()}
    assert managed == {"codex_app_server", "pi_rpc", "claude_stream"}
    assert adapter_ids() == tuple(sorted(managed, key=list(managed).index)) \
        or set(adapter_ids()) == managed


def test_attach_is_registered_unqualified_not_discoverable():
    catalog = get_runtime_catalog()
    attach = next(d for d in catalog.runtimes
                  if d.adapter_id == "claude_attach")
    assert attach.support_status == "registered_unqualified"
    assert attach.discoverable is False
    assert attach.connection_mode == "attach"
    # the connector never treats it as a known managed harness
    assert not known_adapter("claude_attach")


def test_display_names_come_from_the_catalog():
    catalog = get_runtime_catalog()
    for descriptor in catalog.runtimes:
        assert display_name(descriptor.adapter_id) == descriptor.display_name


def test_discoverable_set_used_for_default_inventory():
    discoverable = {d.adapter_id for d in
                    catalog_runtimes(discoverable_only=True)}
    assert discoverable == {"codex_app_server", "pi_rpc", "claude_stream"}


def test_unknown_adapter_refused_everywhere(tmp_path: Path):
    with pytest.raises(Exception):
        select_explicit("claude_attach", tmp_path)
    if os.name == "nt":
        assert shim_candidates("claude_attach") == []
    assert not known_adapter("definitely_not_an_adapter")


@pytest.mark.asyncio
async def test_inventory_default_uses_catalog():
    entries = await discover_inventory()  # None → catalog decides
    for entry in entries:
        assert known_adapter(entry.adapter_id)


def test_npm_shim_names_derive_from_native_kind():
    from okto_nexus_connector.services.discovery_service import (
        _npm_command,
    )
    assert _npm_command("codex_app_server") == "codex"
    assert _npm_command("pi_rpc") == "pi"
    assert _npm_command("claude_stream") == "claude"  # claude_code kind
