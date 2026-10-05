"""Host-local proxy policy shared by HTTP and WebSocket connections."""
from __future__ import annotations

import ipaddress
import os
import re
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import getproxies, proxy_bypass_environment

from ..errors import ConnectorError
from ..storage.state_store import StateStore

_root: ContextVar[Path | None] = ContextVar('proxy_state_root', default=None)
KEY = 'network.proxy'


@contextmanager
def proxy_scope(root: Path):
    token = _root.set(root)
    try:
        yield
    finally:
        _root.reset(token)


def validate_url(value: str, *, allow_credentials: bool = True) -> str:
    try:
        parts = urlsplit(value)
        valid = (parts.scheme in ('http', 'https') and parts.hostname
                 and parts.path in ('', '/') and not parts.query
                 and not parts.fragment and not any(c.isspace() for c in value))
        _ = parts.port
        if not allow_credentials and (parts.username is not None or parts.password is not None):
            raise ConnectorError('PROXY_CONFIGURATION_INVALID', 'proxy',
                                 'Use --url-env for an authenticated proxy; credentials are not stored in configuration.')
        if valid:
            return value
    except ValueError:
        pass
    raise ConnectorError('PROXY_CONFIGURATION_INVALID', 'proxy',
                         'Proxy must be an HTTP or HTTPS URL with a valid host and port, without a path, query, or fragment.')


def load_policy(root: Path | None = None) -> dict:
    selected = root or _root.get()
    if selected is None or not (selected / 'state.json').exists():
        return {}
    policy = StateStore(selected / 'state.json').load().preferences.get(KEY, {})
    if not isinstance(policy, dict):
        raise ConnectorError('PROXY_CONFIGURATION_INVALID', 'proxy', 'Invalid proxy configuration.')
    return policy


def resolve_proxy(destination: str) -> str | None:
    """Resolve each connection anew, including daemon reconnects after edits."""
    parts = urlsplit(destination)
    host = parts.hostname or ''
    try:
        loopback = ipaddress.ip_address(host).is_loopback
    except ValueError:
        loopback = host.lower() == 'localhost'
    if loopback:
        return None
    policy = load_policy()
    mode = policy.get('mode', 'environment')
    if mode == 'direct':
        return None
    proxies = getproxies()
    bypass = ','.join(filter(None, (proxies.get('no', ''), policy.get('no_proxy', ''))))
    if '*' in (entry.strip() for entry in bypass.split(',')) or proxy_bypass_environment(parts.netloc, {'no': bypass}):
        return None
    if mode == 'manual':
        env_name = policy.get('url_env')
        value = os.environ.get(env_name, '') if env_name else policy.get('url', '')
        if not value:
            raise ConnectorError('PROXY_CONFIGURATION_INVALID', 'proxy',
                                 'The configured proxy environment variable is missing or empty.')
        return validate_url(value)
    if mode != 'environment':
        raise ConnectorError('PROXY_CONFIGURATION_INVALID', 'proxy', 'Unknown proxy mode.')
    # Both transports use HTTPS_PROXY for TLS, HTTP_PROXY for plaintext,
    # then ALL_PROXY. This avoids different routes for API and control traffic.
    scheme = 'https' if parts.scheme in ('https', 'wss') else 'http'
    value = proxies.get(scheme) or proxies.get('all')
    return validate_url(value) if value else None


def validate_env_name(name: str) -> None:
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', name):
        raise ConnectorError('PROXY_CONFIGURATION_INVALID', 'proxy', 'Invalid environment variable name.')
