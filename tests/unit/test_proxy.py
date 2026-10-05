import asyncio
import ipaddress
import json
import ssl
from datetime import datetime, timedelta, timezone

import pytest

from okto_nexus_connector.cli.main import main
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.redaction import redact_text
from okto_nexus_connector.storage.state_store import StateStore
from okto_nexus_connector.transport import proxy
from okto_nexus_connector.transport.https_client import NexusHTTPClient
from okto_nexus_connector.transport.wss_r4 import connect_r4_control


@pytest.fixture(autouse=True)
def no_system_proxy(monkeypatch):
    monkeypatch.setattr(proxy, 'getproxies', lambda: {})


def save(root, policy):
    StateStore(root / 'state.json').update(lambda s: s.preferences.update({proxy.KEY: policy}))


def test_cli_persists_reference_without_secret(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv('TEST_PROXY_URL', 'http://alice:very-secret@proxy.test:8080')
    prefix = ['--json', '--state-dir', str(tmp_path), 'proxy']
    assert main(prefix + ['set', '--url-env', 'TEST_PROXY_URL', '--no-proxy', '.internal.test']) == 0
    assert main(prefix + ['show']) == 0
    assert 'very-secret' not in capsys.readouterr().out
    assert 'very-secret' not in (tmp_path / 'state.json').read_text()
    with proxy.proxy_scope(tmp_path):
        assert proxy.resolve_proxy('https://nexus.test') == 'http://alice:very-secret@proxy.test:8080'
        assert proxy.resolve_proxy('wss://node.internal.test/control') is None
        assert main(prefix + ['set', '--direct']) == 0
        assert proxy.resolve_proxy('https://nexus.test') is None
        assert main(prefix + ['clear']) == 0
        assert proxy.load_policy() == {}


@pytest.mark.parametrize('url', ['http://user:secret@proxy.test', 'socks5://proxy.test',
                                 'http://proxy.test:bad', 'http://proxy.test/path', 'not-a-url'])
def test_cli_rejects_invalid_or_secret_url(tmp_path, capsys, url):
    assert main(['--json', '--state-dir', str(tmp_path), 'proxy', 'set', '--url', url]) == 1
    assert 'secret@' not in capsys.readouterr().out
    assert not (tmp_path / 'state.json').exists()


def test_missing_secret_fails_closed_and_redaction(tmp_path):
    save(tmp_path, {'mode': 'manual', 'url_env': 'UNSET_PROXY_FOR_TEST'})
    with proxy.proxy_scope(tmp_path):
        with pytest.raises(ConnectorError, match='missing or empty'):
            proxy.resolve_proxy('wss://nexus.test')
    assert redact_text('http://alice:secret@proxy.test') == 'http://[redacted]@proxy.test'


@pytest.mark.parametrize('destination', ['http://localhost:8202', 'ws://127.0.0.1:8202', 'https://[::1]:8202'])
def test_loopback_always_direct(tmp_path, destination):
    save(tmp_path, {'mode': 'manual', 'url_env': 'UNSET_PROXY_FOR_TEST'})
    with proxy.proxy_scope(tmp_path):
        assert proxy.resolve_proxy(destination) is None


def test_environment_routes_and_bypass_are_shared(monkeypatch):
    monkeypatch.setattr(proxy, 'getproxies', lambda: {
        'https': 'http://secure-proxy.test:8080', 'http': 'http://proxy.test:8080',
        'no': '.internal.test,nexus.test:8443'})
    assert proxy.resolve_proxy('https://nexus.test') == proxy.resolve_proxy('wss://nexus.test')
    assert proxy.resolve_proxy('http://nexus.test') == proxy.resolve_proxy('ws://nexus.test')
    assert proxy.resolve_proxy('wss://nexus.test:8443') is None
    assert proxy.resolve_proxy('https://a.internal.test') is None
    assert proxy.resolve_proxy('https://notinternal.test') is not None


def test_manual_wins_over_environment_and_wildcard_bypasses(tmp_path, monkeypatch):
    monkeypatch.setattr(proxy, 'getproxies', lambda: {'https': 'http://environment.test', 'no': 'internal.test'})
    save(tmp_path, {'mode': 'manual', 'url': 'http://chosen.test'})
    with proxy.proxy_scope(tmp_path):
        assert proxy.resolve_proxy('https://nexus.test') == 'http://chosen.test'
        assert proxy.resolve_proxy('wss://nexus.test') == 'http://chosen.test'
        save(tmp_path, {'mode': 'manual', 'url': 'http://chosen.test', 'no_proxy': '*'})
        assert proxy.resolve_proxy('https://nexus.test') is None
        assert proxy.resolve_proxy('wss://nexus.test') is None


async def test_real_http_and_websocket_connect_proxy(tmp_path, monkeypatch):
    requests = []

    async def reject(reader, writer):
        requests.append((await reader.readuntil(b'\r\n\r\n')).decode())
        writer.write(b'HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n')
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(reject, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    monkeypatch.setenv('TEST_PROXY_URL', f'http://alice:secret@127.0.0.1:{port}')
    save(tmp_path, {'mode': 'manual', 'url_env': 'TEST_PROXY_URL'})
    try:
        with proxy.proxy_scope(tmp_path):
            async with NexusHTTPClient('https://nexus.invalid') as client:
                with pytest.raises(ConnectorError) as error:
                    await client.reach()
                assert 'secret' not in str(error.value)
            with pytest.raises(Exception, match='502'):
                await connect_r4_control('wss://nexus.invalid/control', 'nexus-ticket',
                    server_id='server', executor_id='executor', management_revision=1,
                    snapshot_format='test', boot_id='boot', report_reconciliation=None)
        assert len(requests) == 2
        for request in requests:
            assert request.startswith('CONNECT nexus.invalid:443 HTTP/1.1')
            assert 'Proxy-Authorization: Basic YWxpY2U6c2VjcmV0' in request
            assert 'nexus-ticket' not in request
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.parametrize('secure_proxy', [False, True])
async def test_tls_http_and_websocket_roundtrip_through_proxy(tmp_path, monkeypatch, secure_proxy):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    from websockets.asyncio.server import serve
    from okto_nexus_connector.transport import wss_r4

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'nexus.invalid')])
    now = datetime.now(timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName('nexus.invalid'),
                x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), critical=False)
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .sign(key, hashes.SHA256()))
    cert_file, key_file = tmp_path / 'ca.pem', tmp_path / 'key.pem'
    cert_file.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(key.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(cert_file, key_file)
    monkeypatch.setenv('SSL_CERT_FILE', str(cert_file))
    monkeypatch.delenv('SSL_CERT_DIR', raising=False)
    payload = {'service': 'okto-nexus', 'server_version': '1.0',
               'server_core_version': '1.0', 'minimum_cli_version': '0.5.0'}

    async def http_handler(reader, writer):
        await reader.readuntil(b'\r\n\r\n')
        body = json.dumps(payload).encode()
        writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: '
                     + str(len(body)).encode() + b'\r\nConnection: close\r\n\r\n' + body)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def ws_handler(socket):
        assert socket.request.headers['Authorization'] == 'Bearer test-ticket'
        async for message in socket:
            await socket.send(message)

    requests = []
    handlers = set()

    async def tunnel(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        upstream = None
        try:
            request = (await reader.readuntil(b'\r\n\r\n')).decode()
            requests.append(request)
            authority = request.split()[1]
            host, port = authority.rsplit(':', 1)
            assert host == 'nexus.invalid'
            remote_reader, upstream = await asyncio.open_connection('127.0.0.1', int(port))
            writer.write(b'HTTP/1.1 200 Connection Established\r\n\r\n')
            await writer.drain()

            async def copy(source, target):
                while data := await source.read(65536):
                    target.write(data)
                    await target.drain()
                target.close()

            await asyncio.gather(copy(reader, upstream), copy(remote_reader, writer))
        finally:
            writer.close()
            if upstream:
                upstream.close()
            handlers.discard(task)

    async def negotiated(*args, **kwargs):
        return 'negotiated'

    monkeypatch.setattr(wss_r4, 'negotiate_r4_control', negotiated)
    http_server = await asyncio.start_server(http_handler, '127.0.0.1', 0, ssl=tls)
    ws_server = await serve(ws_handler, '127.0.0.1', 0, ssl=tls, subprotocols=['nxl.v1'])
    proxy_server = await asyncio.start_server(tunnel, '127.0.0.1', 0, ssl=tls if secure_proxy else None)
    proxy_port = proxy_server.sockets[0].getsockname()[1]
    scheme = 'https' if secure_proxy else 'http'
    save(tmp_path, {'mode': 'manual', 'url': f'{scheme}://127.0.0.1:{proxy_port}'})
    try:
        with proxy.proxy_scope(tmp_path):
            http_port = http_server.sockets[0].getsockname()[1]
            async with NexusHTTPClient(f'https://nexus.invalid:{http_port}') as client:
                assert (await client.reach())['server_version'] == '1.0'
            ws_port = ws_server.sockets[0].getsockname()[1]
            socket, state = await connect_r4_control(f'wss://nexus.invalid:{ws_port}/control',
                'test-ticket', server_id='server', executor_id='executor', management_revision=1,
                snapshot_format='test', boot_id='boot', report_reconciliation=None)
            try:
                assert state == 'negotiated'
                await socket.send('proxy-roundtrip')
                assert await socket.recv() == 'proxy-roundtrip'
            finally:
                await socket.close()
        assert len(requests) == 2
        assert all('test-ticket' not in request for request in requests)
    finally:
        for server in (proxy_server, http_server, ws_server):
            server.close()
            await server.wait_closed()
        if handlers:
            await asyncio.gather(*list(handlers))
