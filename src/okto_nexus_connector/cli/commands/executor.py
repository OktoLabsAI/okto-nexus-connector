"""Register one executor with a durable intent; never print derivative secrets."""

import asyncio
from dataclasses import asdict

from ...errors import ConnectorError
from ...platform import paths
from ...services.executor_registration import ExecutorRegistrationService
from ...storage.state_store import StateStore
from .identity import _vault


async def run_executor(args, output, root):
    store = StateStore(paths.state_file(root))
    if args.subcommand == 'register':
        service = ExecutorRegistrationService(store, _vault(root, store))
        record = await service.register(identity_alias=args.identity, label=args.label,
                                        client_intent_id=args.client_intent_id)
        output.line('Executor registered. Inventory, approved bindings and runtime authority are still required.')
        return asdict(record)
    state = await asyncio.to_thread(store.load)
    records = state.execution_executors
    if args.subcommand == 'show':
        records = [record for record in records if record.server_id == args.server_id]
        if len(records) != 1:
            raise ConnectorError('VALIDATION_ERROR', 'executor_registration',
                                 'The Server executor registration is missing or ambiguous.')
        output.line(f'Executor registration: {records[0].state}')
        return asdict(records[0])
    if args.subcommand == 'list':
        for record in records:
            output.line(f'{record.server_id}: {record.executor_id or "Pending"} ({record.state})')
        return {'executors': [asdict(record) for record in records]}
    raise ConnectorError('VALIDATION_ERROR', 'executor_registration', 'Unknown executor command.')
