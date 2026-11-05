"""Bounded host lifecycle documents and a stable, non-secret launch context."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import sys

from ..daemon.lock import InstanceLock, _atomic_write
from ..errors import ConnectorError
from .service_identity import service_instance_id

CONTEXT = 'service-context.json'
INSTALLATION = 'service-installation.json'
CONTROL = 'service-control.json'
LAUNCH = 'service-launch.json'
ENVIRONMENT_REFERENCES = ('SystemRoot', 'WINDIR', 'USERPROFILE', 'HOMEDRIVE',
    'HOMEPATH', 'APPDATA', 'LOCALAPPDATA', 'PROGRAMDATA', 'TEMP', 'TMP', 'HOME',
    'XDG_CONFIG_HOME', 'XDG_DATA_HOME', 'XDG_STATE_HOME', 'XDG_RUNTIME_DIR', 'LANG', 'LC_ALL')


def read_document(path: Path) -> dict:
    with path.open('r', encoding='utf-8') as stream:
        raw = stream.read(131073)
    if len(raw) > 131072:
        raise ValueError('Oversized lifecycle document')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError('Invalid lifecycle document')
    return value


def write_document(path: Path, value: dict) -> None:
    data = json.dumps(value, ensure_ascii=False, indent=2).encode('utf-8')
    if len(data) > 131072:
        raise ValueError('Oversized lifecycle document')
    _atomic_write(path, data)


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(',', ':')).encode('utf-8')).hexdigest()


def execution_context(root: Path, *, env=None) -> dict:
    root = root.resolve()
    env = os.environ if env is None else env
    environment = {key: env[key] for key in ENVIRONMENT_REFERENCES if env.get(key)}
    environment['PATH'] = os.pathsep.join(dict.fromkeys(str(Path(item).resolve())
        for item in env.get('PATH', os.defpath).split(os.pathsep)
        if item and Path(item).is_absolute()))
    environment['PYTHONUNBUFFERED'] = '1'
    environment['OKTO_NEXUS_CONNECTOR_STATE'] = str(root)
    if env.get('OKTO_NEXUS_CONNECTOR_VAULT', '').strip().lower() == 'file':
        # Backend selection is not fallback consent; the existing state-store
        # approval still governs every secret operation in RestrictedFileVault.
        environment['OKTO_NEXUS_CONNECTOR_VAULT'] = 'file'
    return {'revision': 1, 'component': 'connector', 'instance_id': service_instance_id(root),
        'root': str(root), 'cwd': str(root),
        # Keep a POSIX venv symlink invocation; resolving it would select base Python.
        'executable': str(Path(sys.executable).absolute()),
        'environment_prefix': str(Path(sys.prefix).absolute()), 'environment': environment}


def validate_context(root: Path, value: dict) -> dict:
    root = root.resolve()
    if (value.get('revision') != 1 or value.get('component') != 'connector'
            or value.get('instance_id') != service_instance_id(root)
            or value.get('root') != str(root) or value.get('cwd') != str(root)):
        raise ValueError('The launch context does not match this Connector instance')
    for key in ('executable', 'environment_prefix', 'cwd'):
        if not isinstance(value.get(key), str) or not Path(value[key]).is_absolute():
            raise ValueError('Lifecycle paths must be absolute')
    if not Path(value['executable']).is_file():
        raise ValueError('The recorded Python environment is unavailable')
    env = value.get('environment')
    if (not isinstance(env, dict) or not all(type(k) is str and type(v) is str for k, v in env.items())
            or set(env) - set(ENVIRONMENT_REFERENCES) - {'PATH', 'PYTHONUNBUFFERED', 'OKTO_NEXUS_CONNECTOR_STATE', 'OKTO_NEXUS_CONNECTOR_VAULT'}
            or env.get('OKTO_NEXUS_CONNECTOR_VAULT', 'file') != 'file'
            or env.get('OKTO_NEXUS_CONNECTOR_STATE') != str(root)):
        raise ValueError('Unsupported lifecycle environment')
    return value


def saved_context(root: Path) -> dict:
    try:
        return validate_context(root, read_document(root / CONTEXT))
    except FileNotFoundError:
        return execution_context(root)


def launch_environment(root: Path, context: dict, *, inherited=None) -> dict:
    """Resolve existing proxy references at launch, never serialize their values.

    Network policy remains in the existing state store. Only its transport
    variables cross this boundary; provider keys and shell startup hooks do not.
    """
    from ..transport.proxy import load_policy, validate_env_name, validate_url
    inherited = os.environ if inherited is None else inherited
    environment = dict(context['environment'])
    policy = load_policy(root)
    mode = policy.get('mode', 'environment')
    references = set()
    if mode == 'environment':
        references = {'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY',
                      'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy'}
    elif mode == 'manual' and policy.get('url_env'):
        name = policy['url_env']
        validate_env_name(name)
        if (name.upper() in {key.upper() for key in ENVIRONMENT_REFERENCES} | {'PATH', 'COMSPEC', 'SHELL'}
                or name.upper().startswith(('PYTHON', 'LD_', 'DYLD_', 'OKTO_NEXUS_CONNECTOR_'))):
            raise ConnectorError('PROXY_CONFIGURATION_INVALID', 'service_context',
                'The proxy reference collides with the stable process context.',
                action='Choose a dedicated proxy environment variable in the existing proxy configuration.')
        references = {name}
    for name in references:
        value = inherited.get(name)
        if value:
            if name.lower() != 'no_proxy':
                validate_url(value)
            environment[name] = value
    return environment


def launch_guard(root: Path) -> InstanceLock:
    return InstanceLock(root.resolve() / 'run', lock_name='service-launch.guard')


@contextmanager
def launch_exclusion(root: Path):
    guard = launch_guard(root)
    if not guard.acquire():
        raise ConnectorError('LIFECYCLE_BUSY', 'service', 'Another lifecycle operation is selecting this instance.',
            retry_safe=True, action='Observe the same startup; do not launch a competing process.')
    try:
        yield
    finally:
        guard.release()
