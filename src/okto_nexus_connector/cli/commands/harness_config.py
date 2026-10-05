"""Reusable settings with server-owned revision and execution authority."""
from nexus_connector_core import CoreError, discover_harness_configuration, parse_harness_configuration_file
from nexus_connector_core.configuration_file import MAX_CONFIGURATION_FILE_BYTES

from ...errors import ConnectorError
from ...platform import paths
from ...storage.state_store import StateStore
from ...transport.https_client import NexusHTTPClient
from .identity import _vault


def read_configuration_file(path):
    try:
        with path.open('rb') as stream:
            data = stream.read(MAX_CONFIGURATION_FILE_BYTES + 1)
        if len(data) > MAX_CONFIGURATION_FILE_BYTES:
            raise ValueError()
        return data.decode('utf-8-sig')
    except (OSError, ValueError, UnicodeError):
        raise ConnectorError('VALIDATION_ERROR', 'harness_config',
                             'Use a UTF-8 JSON file no larger than 64 KiB.') from None


async def run_harness_config(args, output, root):
    try:
        if args.subcommand == 'describe':
            return discover_harness_configuration(args.harness)
        text = read_configuration_file(args.file) if args.subcommand in ('validate', 'apply') else None
        if args.subcommand == 'validate':
            return parse_harness_configuration_file(text, adapter_id=args.harness)
        # Validate before accessing credentials or issuing any request.
        if text is not None:
            parse_harness_configuration_file(text)
        store = StateStore(paths.state_file(root))
        state = store.load()
        identity = state.identity_by_alias(args.identity)
        if identity is None or identity.server_id not in state.servers:
            raise ConnectorError('VALIDATION_ERROR', 'harness_config', 'Select an imported identity with a Server profile.')
        key = _vault(root, store).resolve(identity.secret_handle)
        async with NexusHTTPClient(state.servers[identity.server_id].base_url) as http:
            me = await http.me(key)
            if (me.server_id, me.agent_id) != (identity.server_id, identity.agent_id):
                raise ConnectorError('AGENT_ID_MISMATCH', 'harness_config', 'The credential identity changed.')
            current = await http.harness_settings(key, args.endpoint_id)
            if args.subcommand == 'show':
                return current
            document = parse_harness_configuration_file(text, adapter_id=current['adapter_id'],
                                                        configuration=current['configuration'])
            if args.expected_revision != current['revision']:
                raise ConnectorError('OPERATION_CONFLICT', 'harness_config', 'Settings changed. Review harness-config show again.')
            return await http.harness_settings(key, args.endpoint_id,
                changes=dict(expected_revision=args.expected_revision, settings=document['settings']))
    except CoreError:
        raise ConnectorError('VALIDATION_ERROR', 'harness_config', 'Invalid or incompatible harness configuration file.') from None
