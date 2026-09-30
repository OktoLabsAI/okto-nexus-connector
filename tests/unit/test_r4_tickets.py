"""Ticket intent replay and conservative replacement after material loss."""
from dataclasses import replace
import sqlite3

import pytest

from okto_nexus_connector.errors import ConnectorError, TicketMaterialUnavailable
from okto_nexus_connector.services.r4_tickets import acquire_ticket
from okto_nexus_connector.storage.r4_tickets import R4TicketStore
from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_daemon_execution import lifecycle


async def current():
    pass


def metadata():
    return dict(server_id="srv", executor_id="exe", binding_id="binding", agent_id="agent",
                authority_digest="a"*64, scopes=["lane:attach"], expires_in=600)


def test_store_reopens_without_secrets_and_replacement_is_compare_and_swap(tmp_path):
    path = tmp_path / "tickets.db"
    first = R4TicketStore(path).reserve(metadata(), 100)
    assert R4TicketStore(path).reserve(metadata(), 200) == first
    second = R4TicketStore(path).replace(first, "ept_previous", 200)
    assert second["client_intent_id"] == first["client_intent_id"]
    assert second["credential_request_id"] != first["credential_request_id"]
    assert second["replaces_ticket_id"] == "ept_previous"
    with pytest.raises(ConnectorError, match="STALE_GENERATION"):
        R4TicketStore(path).replace(first, "ept_other", 300)
    with pytest.raises(ConnectorError, match="STALE_GENERATION"):
        R4TicketStore(path).require_current(first)
    R4TicketStore(path).require_current(second)


@pytest.mark.parametrize("now,replaces", [(699, "ept_previous"), (700, None), (50, "ept_previous")])
def test_expiration_hint_does_not_extend_or_invent_server_authority(tmp_path, now, replaces):
    store = R4TicketStore(tmp_path / "tickets.db")
    first = store.reserve(metadata(), 100)
    second = store.replace(first, "ept_previous", now)
    assert second["replaces_ticket_id"] == replaces
    assert second["client_intent_id"] == first["client_intent_id"]


def test_corrupt_intent_is_refused_before_reuse(tmp_path):
    store = R4TicketStore(tmp_path / "tickets.db")
    store.reserve(metadata(), 100)
    with sqlite3.connect(store.path) as conn:
        conn.execute("UPDATE intents SET metadata='{}'")
    with pytest.raises(ConnectorError, match="JOURNAL_UNAVAILABLE"):
        store.reserve(metadata(), 200)


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_lost_reply_replays_exact_request_then_replaces_original_intent(lifecycle):
    owner, http, store, _, native, attached, _, _ = lifecycle
    binding, identity = next(iter(owner._bindings().values()))
    original = http.request_r4_binding_ticket
    requests = []
    async def issuing(key, **kwargs):
        requests.append(kwargs)
        if len(requests) == 1:
            raise ConnectorError("OUTCOME_UNKNOWN", "http", possible_effect=True)
        if len(requests) == 2:
            raise TicketMaterialUnavailable("CREDENTIAL_MATERIAL_UNAVAILABLE", "credential", ticket_id="ept_original")
        return await original(key, **kwargs)
    http.request_r4_binding_ticket = issuing
    with pytest.raises(ConnectorError, match="OUTCOME_UNKNOWN"):
        await acquire_ticket(store, owner.vault, http, binding, identity, require_current=current)
    ticket, _ = await acquire_ticket(store, owner.vault, http, binding, identity, require_current=current)
    assert requests[0] == requests[1]
    assert requests[2]["client_intent_id"] == requests[0]["client_intent_id"]
    assert requests[2]["credential_request_id"] != requests[0]["credential_request_id"]
    assert requests[2]["replaces_ticket_id"] == "ept_original"
    raw = R4TicketStore.for_state(store).path.read_bytes()
    assert b"agent-key" not in raw and ticket.ticket.encode() not in raw
    assert not attached and not native.opened


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_live_bound_replacement_refusal_is_preserved_for_later_retry(lifecycle):
    owner, http, store, _, native, attached, _, _ = lifecycle
    binding, identity = next(iter(owner._bindings().values()))
    requests = []
    async def issuing(key, **kwargs):
        requests.append(kwargs)
        if len(requests) == 1:
            raise TicketMaterialUnavailable("CREDENTIAL_MATERIAL_UNAVAILABLE", "credential", ticket_id="ept_bound")
        raise ConnectorError("CONFLICT", "credential", "The prior ticket cannot be replaced.")
    http.request_r4_binding_ticket = issuing
    for _ in range(2):
        with pytest.raises(ConnectorError, match="CONFLICT"):
            await acquire_ticket(store, owner.vault, http, binding, identity, require_current=current)
    assert requests[1] == requests[2] and requests[1]["replaces_ticket_id"] == "ept_bound"
    assert not attached and not native.opened


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_persistence_failure_prevents_http(lifecycle, monkeypatch):
    owner, http, store, _, _, _, _, keys = lifecycle
    binding, identity = next(iter(owner._bindings().values()))
    def fail(*args):
        raise ConnectorError("JOURNAL_UNAVAILABLE", "r4_tickets")
    monkeypatch.setattr(R4TicketStore, "reserve", fail)
    with pytest.raises(ConnectorError, match="JOURNAL_UNAVAILABLE"):
        await acquire_ticket(store, owner.vault, http, binding, identity, require_current=current)
    assert not keys


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_concurrent_replacement_refuses_superseded_reply(lifecycle):
    owner, http, store, _, _, _, _, _ = lifecycle
    binding, identity = next(iter(owner._bindings().values()))
    original = http.request_r4_binding_ticket
    async def issuing(key, **kwargs):
        state = R4TicketStore.for_state(store)
        with sqlite3.connect(state.path) as conn:
            import json
            intent = json.loads(conn.execute("SELECT metadata FROM intents").fetchone()[0])
        state.replace(intent, "ept_previous", intent["created_at"]+1)
        return await original(key, **kwargs)
    http.request_r4_binding_ticket = issuing
    with pytest.raises(ConnectorError, match="STALE_GENERATION"):
        await acquire_ticket(store, owner.vault, http, binding, identity, require_current=current)


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_changed_identity_uses_a_different_durable_intent(lifecycle):
    owner, http, store, _, _, _, _, _ = lifecycle
    binding, identity = next(iter(owner._bindings().values()))
    original = http.request_r4_binding_ticket
    requests = []
    async def issuing(key, **kwargs):
        requests.append(kwargs)
        ticket = await original(key, **kwargs)
        return replace(ticket, credential_epoch=1 if len(requests)==1 else 2)
    http.request_r4_binding_ticket = issuing
    await acquire_ticket(store, owner.vault, http, binding, identity, require_current=current)
    await acquire_ticket(store, owner.vault, http, binding, replace(identity, credential_epoch=2), require_current=current)
    assert requests[0]["client_intent_id"] != requests[1]["client_intent_id"]
    assert requests[1]["replaces_ticket_id"] is None
