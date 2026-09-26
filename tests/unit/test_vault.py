"""Unit: restricted-file vault semantics (plan C01, TC-05/TC-06)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.identity.vault import (
    RestrictedFileVault, handle_for, namespace_of,
)


def make_vault(tmp_path: Path, *, approved: bool = True) -> RestrictedFileVault:
    directory = tmp_path / "vault"
    directory.mkdir()
    return RestrictedFileVault(directory, approved=approved)


def test_store_resolve_roundtrip(tmp_path: Path):
    vault = make_vault(tmp_path)
    handle = vault.store("srv_a/ag_1", "nxs_key_alpha")
    assert handle == handle_for("srv_a", "ag_1")
    assert vault.resolve(handle) == "nxs_key_alpha"


def test_unapproved_fallback_fails_closed(tmp_path: Path):
    vault = make_vault(tmp_path, approved=False)
    with pytest.raises(ConnectorError) as error:
        vault.store("srv/ag", "nxs_key")
    assert error.value.code == "APPROVAL_REQUIRED"


def test_namespaces_isolated(tmp_path: Path):
    """TC-06: same alias on two servers keeps namespaces distinct."""
    vault = make_vault(tmp_path)
    vault.store("srv_a/ag_1", "nxs_key_a")
    vault.store("srv_b/ag_1", "nxs_key_b")
    assert vault.resolve(handle_for("srv_a", "ag_1")) == "nxs_key_a"
    assert vault.resolve(handle_for("srv_b", "ag_1")) == "nxs_key_b"
    # a handle from one context never reads another namespace
    with pytest.raises(ConnectorError):
        vault.resolve("vault:srv_c/ag_1")
    assert vault.list_namespaces() == ["srv_a/ag_1", "srv_b/ag_1"]


def test_duplicate_store_requires_replace(tmp_path: Path):
    vault = make_vault(tmp_path)
    vault.store("srv/ag", "nxs_first")
    with pytest.raises(ConnectorError):
        vault.store("srv/ag", "nxs_second")
    vault.replace("srv/ag", "nxs_rotated")
    assert vault.resolve(handle_for("srv", "ag")) == "nxs_rotated"


def test_remove_is_local_only(tmp_path: Path):
    vault = make_vault(tmp_path)
    vault.store("srv/ag", "nxs_key")
    vault.remove("srv/ag")
    with pytest.raises(ConnectorError):
        vault.resolve(handle_for("srv", "ag"))
    # removing again is a no-op (local removal, not central revocation)
    vault.remove("srv/ag")


def test_secret_shapes_validated(tmp_path: Path):
    vault = make_vault(tmp_path)
    with pytest.raises(ConnectorError):
        vault.store("", "nxs_key")
    with pytest.raises(ConnectorError):
        vault.store("srv/ag", "line\nbreak")
    with pytest.raises(ConnectorError):
        vault.store("bad\nnamespace", "nxs_key")


def test_no_secret_leak_in_status_or_listing(tmp_path: Path):
    vault = make_vault(tmp_path)
    vault.store("srv/ag", "nxs_super_secret_value")
    status = vault.status().to_json()
    listing = json.dumps(vault.list_namespaces())
    assert "nxs_super_secret_value" not in json.dumps(status)
    assert "nxs_super_secret_value" not in listing
