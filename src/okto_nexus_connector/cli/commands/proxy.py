"""Configure the Connector's host-local outbound proxy."""
from ...storage.state_store import StateStore
from ...transport.proxy import KEY, load_policy, validate_env_name, validate_url


async def run_proxy(args, output, root):
    store = StateStore(root / 'state.json')
    if args.subcommand == 'set':
        policy = {'mode': 'direct' if args.direct else 'manual', 'no_proxy': args.no_proxy or ''}
        if args.url:
            policy['url'] = validate_url(args.url, allow_credentials=False)
        if args.url_env:
            validate_env_name(args.url_env)
            policy['url_env'] = args.url_env
        store.update(lambda state: state.preferences.update({KEY: policy}))
    elif args.subcommand == 'clear':
        store.update(lambda state: state.preferences.pop(KEY, None))
    policy = load_policy(root) or {'mode': 'environment'}
    # Never resolve environment references into output or persisted state.
    return {'proxy': policy, 'scope': 'connector-to-nexus',
            'applies': 'New connections and reconnects. Restart the daemon to replace existing connections. '
                       'Restart it after changing environment variables.'}
