"""Negotiated passive refresh, scope checks and daemon observation ordering."""
import time
from dataclasses import replace

import httpx
import pytest

from tests.unit.test_r4_daemon_control import control, eventually, dispose
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.transport.https_client import NexusHTTPClient, MANAGEMENT_REVISION
from okto_nexus_connector.services.discovery_service import executor_inventory_snapshot
from nexus_connector_core import R4_PREVIEW_REVISION, SNAPSHOT_FORMAT_VERSION, __version__


@pytest.mark.parametrize('support', [None, False, True, 1, 'true'])
async def test_refresh_capability_must_be_explicit_boolean(support):
    payload = dict(management_revision=MANAGEMENT_REVISION, protocol_major=1, core_version=__version__,
        executor_snapshot_format=SNAPSHOT_FORMAT_VERSION, nxl_accepted=[R4_PREVIEW_REVISION],
        remote_execution_ready=True)
    if support is not None:
        payload['inventory_refresh_supported'] = support
    def respond(request):
        return httpx.Response(200, json=payload, headers={'X-Nexus-Connections-Revision': MANAGEMENT_REVISION})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as raw:
        async with NexusHTTPClient('https://nexus.test', client=raw) as http:
            if support is not None and type(support) is not bool:
                with pytest.raises(ConnectorError):
                    await http.r4_protocol()
            else:
                assert (await http.r4_protocol()).inventory_refresh_supported is (support is True)


@pytest.mark.parametrize('change', ['valid', 'none', 'scope', 'producer', 'invalid_id', 'extra'])
async def test_claim_response_and_publication_header(change):
    delivery = None if change == 'none' else 'delivery-1'
    payload = dict(server_id='srv', executor_id='executor', producer_instance_id='connection', delivery_id=delivery)
    if change == 'scope': payload['server_id'] = 'other'
    elif change == 'producer': payload['producer_instance_id'] = 'other'
    elif change == 'invalid_id': payload['delivery_id'] = 1
    elif change == 'extra': payload['unexpected'] = True
    snapshot = executor_inventory_snapshot([], server_id='srv', executor_id='executor',
        producer_instance_id='connection', publication_sequence=2)
    def respond(request):
        assert request.headers['Authorization'] == 'Bearer nxt4_technical'
        if request.method == 'POST':
            assert request.url.path.endswith('/inventory:claim-refresh')
            result = payload
        else:
            assert request.method == 'PUT'
            assert request.headers.get('X-Nexus-Inventory-Refresh') == delivery
            assert 'refresh_delivery_id' not in request.content.decode()
            result = dict(server_id='srv', executor_id='executor', publication_sequence=2,
                inventory_revision=snapshot['inventory_revision'], fresh_for_ms=120000, accepted=True)
        return httpx.Response(200, json=result, headers={'X-Nexus-Connections-Revision': MANAGEMENT_REVISION})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as raw:
        async with NexusHTTPClient('https://nexus.test', client=raw) as http:
            if change not in ('valid', 'none'):
                with pytest.raises(ConnectorError, match='invalid inventory refresh'):
                    await http.claim_inventory_refresh('nxt4_technical', server_id='srv',
                        executor_id='executor', producer_instance_id='connection')
            else:
                claimed = await http.claim_inventory_refresh('nxt4_technical', server_id='srv',
                    executor_id='executor', producer_instance_id='connection')
                assert claimed == delivery
                await http.publish_inventory('nxt4_technical', executor_id='executor',
                    snapshot=snapshot, refresh_delivery_id=claimed)


@pytest.mark.parametrize('lost_reply', [False, True])
async def test_daemon_claims_before_discovery_and_recovers_lost_reply(control, monkeypatch, lost_reply):
    owner, peer, store, host, _ = control
    protocol = peer.r4_protocol
    async def supported():
        return replace(await protocol(), inventory_refresh_supported=True)
    monkeypatch.setattr(peer, 'r4_protocol', supported)
    events, published = [], []
    claim_count = 0
    async def claim(ticket, **scope):
        nonlocal claim_count
        assert scope['producer_instance_id'] == owner.connection.state.connection_id
        claim_count += 1
        delivery = None if claim_count == 1 else f'delivery-{claim_count}'
        events.append(('claim', delivery))
        return delivery
    monkeypatch.setattr(peer, 'claim_inventory_refresh', claim, raising=False)
    async def discover():
        events.append(('discover', None))
        return []
    owner.discover = discover
    original = peer.publish_inventory
    async def publish(ticket, *, executor_id, snapshot, refresh_delivery_id=None):
        events.append(('publish', refresh_delivery_id))
        published.append(refresh_delivery_id)
        if lost_reply and refresh_delivery_id == 'delivery-2':
            peer.lost = True
        return await original(ticket, executor_id=executor_id, snapshot=snapshot)
    monkeypatch.setattr(peer, 'publish_inventory', publish)
    offset = 0
    owner.clock = lambda: time.monotonic() + offset
    owner.start()
    try:
        await eventually(lambda: owner.status()['control_ready'], diagnostics=owner.status)
        offset = 6
        await eventually(lambda: len(published) >= (3 if lost_reply else 2) and owner.status()['control_ready'],
                         diagnostics=owner.status)
        assert events[:6] == [('claim', None), ('discover', None), ('publish', None),
                            ('claim', 'delivery-2'), ('discover', None), ('publish', 'delivery-2')]
        if lost_reply:
            assert published[2] == 'delivery-3'
            assert peer.publications[2]['producer_instance_id'] != peer.publications[1]['producer_instance_id']
        assert [item['publication_sequence'] for item in peer.publications] == list(range(1, len(published) + 1))
        assert not owner.status()['execution_ready']
    finally:
        await dispose(owner, host)
