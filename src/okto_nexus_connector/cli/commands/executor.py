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
    if args.subcommand == 'probe':
        from ...services.installation_observation import observe_installation
        result = await observe_installation(store, server_id=args.server_id,
            adapter_id=args.harness, candidate_ref=args.candidate_ref, inventory_revision=args.inventory_revision)
        output.line('Version observed. Wait for daemon inventory publication; binding approval and runtime authority are separate.')
        return result
    if args.subcommand in ("configure-launch", "realize"):
        from ...services.executor_onboarding import ExecutorOnboarding
        service = ExecutorOnboarding(store)
        if args.subcommand == "configure-launch":
            bindings = {}
            for entry in args.secret_ref:
                name, separator, reference = entry.partition("=")
                if not separator or name in bindings:
                    raise ConnectorError("VALIDATION_ERROR", "executor_onboarding",
                                         "Each secret reference must have a unique NAME=REFERENCE value.")
                bindings[name] = reference
            result = await service.configure_launch(identity_alias=args.identity, adapter_id=args.harness,
                local_consent_id=args.local_consent_id, profile_revision=args.profile_revision,
                secret_bindings=bindings, provider_home=args.provider_home)
            output.line("Launch consent staged. Publish the realization and obtain binding approval before execution.")
            return asdict(result)
        from ...daemon import manager
        def publish():
            with manager.connect(root) as client:
                response = client.call("executor.realize", dict(identity_alias=args.identity,
                    client_intent_id=args.client_intent_id, adapter_id=args.harness,
                    candidate_ref=args.candidate_ref, inventory_revision=args.inventory_revision,
                    configuration_digest=args.configuration_digest, workspace_root=str(args.project),
                    workspace_id=args.workspace_id, workspace_label=args.label))
            if not response.get("ok"):
                raise ConnectorError.from_json(response.get("error", {}))
            return dict(response.get("result", {}))
        result = await asyncio.to_thread(publish)
        output.line("Realization published. Binding approval and runtime authority are still required.")
        return result
    if args.subcommand == 'configure-discovery':
        from ...services.discovery_configuration import configure_discovery
        configuration = await asyncio.to_thread(configure_discovery, store,
            server_id=args.server_id, roots=args.harness_root,
            pi_install_root=args.pi_install_root, pi_node=args.pi_node)
        output.line('Discovery configuration saved. Runtime execution still requires an approved binding and lease.')
        return {'server_id': args.server_id, 'discovery_configuration': configuration}
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
