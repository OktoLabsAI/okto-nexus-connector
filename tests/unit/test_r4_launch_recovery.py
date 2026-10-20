"""Connector recovery uses durable Core producer proof before reporting release."""
import asyncio
from dataclasses import asdict
import hashlib
import secrets
import sys

import pytest
from nexus_connector_core import OperationKey, SessionKey, encode_r4_frame, decode_r4_frame, project_r4_bound_receipt
from nexus_connector_core.process_launch import ProcessLaunchIdentity
from nexus_connector_core.protocol import canonical_json
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services.core_host import CoreRuntimeHost
from okto_nexus_connector.services.r4_reconciliation import R4ReconciliationReporter
from okto_nexus_connector.storage.r4_publications import R4PublicationStore
from tests.unit.test_execution_selection import selection


async def setup(selection, tmp_path, *, marked):
    store, _, _, frame, *_ = selection
    host = CoreRuntimeHost(tmp_path, None)
    journal, ledger = await host.ensure_history_journal(), await host.ensure_ledger()
    key = OperationKey('srv','exe','open')
    launch = ProcessLaunchIdentity(SessionKey('srv','exe','session'), 'open', 1, 1,
        'win32', 'windows_job', 'Global\\nexus-core-' + secrets.token_hex(16), secrets.token_hex(16))
    core_hash = 'sha256:' + 'c'*64
    await journal.admit(key, core_hash, 'session', claim_session=True,
        connection_generation=1, session_owner_generation=1, process_launch_identity=launch)
    if marked:
        await journal.mark_possible_effect(key)
        await journal.reserve_owned_slot(key, 'session')
        await ledger.reserve_owned_slot(key, 'session')
    body = dict(schema_version=1, action='runtime.open', core_intent_hash=core_hash,
        source={k: frame[k] for k in ('protocol_major','contract_revision','server_id','executor_id','binding_id',
            'agent_id','session_id','connection_id','connection_generation','operation_id','intent_hash')})
    binding = body | dict(digest='sha256:' + hashlib.sha256(canonical_json(body)).hexdigest())
    outbox = R4PublicationStore.for_state(store)
    outbox.reserve(frame)
    outbox.bind(binding, stream_epoch='epoch')
    first = project_r4_bound_receipt(binding, await journal.get_receipt(key), key=key, receipt_revision=1)
    outbox.record(first)
    outbox.acknowledge(first)
    request = {k: frame[k] for k in ('protocol_major','contract_revision','server_id','executor_id')}
    request.update(type='reconcile.request', connection_id='replacement', connection_generation=2,
        reconcile_id='cycle', cursor=None, operation_ids=['open'], session_ids=['session'],
        stream_watermarks=[], launch_recovery_supported=True)
    return store, host, journal, ledger, key, launch, outbox, request


@pytest.mark.parametrize('marked', [False, True])
@pytest.mark.parametrize('initial_turn', [False, True])
async def test_unclaimed_open_is_published_before_complete_report_without_native_replay(selection, tmp_path, marked, initial_turn):
    store, host, journal, ledger, key, launch, outbox, request = await setup(selection, tmp_path, marked=marked)
    published, authority = [], []
    if initial_turn:
        request['operation_ids'].append('unsent-initial-turn')
    def verify(request): authority.append(request['connection_generation'])
    async def publish():
        for value in outbox.ready('srv','exe'):
            published.append(value)
            outbox.acknowledge(value)
    try:
        reporter = R4ReconciliationReporter(store, host, 'srv','exe', validate_authority=verify, publish_receipts=publish)
        report = decode_r4_frame(encode_r4_frame(await reporter.report(request)))
        assert report['complete'] and not reporter.blocked
        assert report['ownership_facts'][0]['process_state'] == 'NOT_STARTED'
        assert report['claims'][0]['state'] == 'RELEASED'
        assert [r['operation_id'] for r in report['receipts']] == ['open']
        assert len(published) == 1 and published[0]['stage'] == 'FAILED'
        assert published[0]['connection_id'] == 'connection' and published[0]['connection_generation'] == 1
        assert published[0]['possible_effect'] is marked and published[0]['retry_safe'] is not marked
        assert not (await ledger.owned_slot_page()).reservations
        assert not (await journal.owned_slot_page()).reservations
        assert await journal.get_process_birth(launch.key) is None
        assert set(authority) == {2}
        assert asdict(await journal.get_process_launch(launch.key)) == report['ownership_facts'][0]['launch_proof']
        # A duplicate recovery only reports the retained original fact.
        repeated = await reporter.report(request)
        assert repeated == report and len(published) == 1
    finally:
        await host.shutdown_all()


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows native containment')
@pytest.mark.parametrize('boundary', ['intent','claim','container','spawn','birth'])
async def test_real_crashed_producer_is_recovered_through_connector_report(selection, tmp_path, boundary):
    import json
    from pathlib import Path
    import subprocess
    store, host, journal, ledger, key, launch, outbox, request = await setup(selection,tmp_path,marked=True)
    paths = json.dumps([str(Path(p).resolve()) for p in sys.path if p])
    loader = 'import sys,json,runpy; sys.path[:]=json.loads(sys.argv.pop(1)); runpy.run_path(sys.argv.pop(1),run_name="__main__")'
    fixture = Path(__file__).parents[1]/'fixtures/launch_owner_crash.py'
    async def publish():
        for value in outbox.ready('srv','exe'): outbox.acknowledge(value)
    try:
        result = await asyncio.to_thread(subprocess.run,
            [sys.executable,'-I','-c',loader,paths,str(fixture),str(tmp_path),boundary],
            capture_output=True,text=True,timeout=30)
        assert result.returncode == 17, result.stderr
        reporter = R4ReconciliationReporter(store,host,'srv','exe',
            validate_authority=lambda _: None,publish_receipts=publish)
        report = decode_r4_frame(encode_r4_frame(await reporter.report(request)))
        assert report['complete'] and not reporter.blocked
        assert report['ownership_facts'][0]['process_state'] == ('NOT_STARTED' if boundary == 'intent' else 'EXITED')
        assert (await journal.get_process_launch(launch.key)).producer_fenced
        assert not (await ledger.owned_slot_page()).reservations
        assert not (await journal.owned_slot_page()).reservations
        # A stopped resource never becomes successful business execution.
        receipt = await journal.get_receipt(key)
        assert receipt.stage == ('FAILED' if boundary == 'intent' else 'SUBMISSION_STARTED')
        assert receipt.possible_effect and not receipt.retry_safe
    finally:
        await host.shutdown_all()


@pytest.mark.parametrize('fault', ['authority_lost','no_authority','publication_failed','missing_ack','no_extension'])
async def test_recovery_failure_never_reports_ready_or_loses_original_fact(selection, tmp_path, fault):
    store, host, journal, ledger, key, launch, outbox, request = await setup(selection, tmp_path, marked=True)
    def verify(request):
        if fault == 'authority_lost':
            raise ConnectorError('STALE_GENERATION','test','Lost owner')
    async def publish():
        if fault == 'publication_failed': raise OSError('Synthetic publication outage')
        if fault != 'missing_ack':
            for value in outbox.ready('srv','exe'): outbox.acknowledge(value)
    if fault == 'no_extension': request.pop('launch_recovery_supported')
    try:
        reporter = R4ReconciliationReporter(store, host, 'srv','exe',
            validate_authority=None if fault == 'no_authority' else verify, publish_receipts=publish)
        with pytest.raises((ConnectorError, OSError)):
            await reporter.report(request)
        receipt = await journal.get_receipt(key)
        assert receipt.possible_effect and not receipt.retry_safe
        if fault in ('authority_lost','no_authority'):
            assert receipt.stage == 'SUBMISSION_STARTED'
            assert len((await ledger.owned_slot_page()).reservations) == 1
            assert not (await journal.get_process_launch(launch.key)).producer_fenced
        elif fault in ('publication_failed','missing_ack'):
            assert receipt.stage == 'FAILED' and outbox.pending('srv','exe')
            assert reporter.blocked
        assert await journal.get_process_birth(launch.key) is None
    finally:
        await host.shutdown_all()


async def test_lost_authority_after_worker_stop_cannot_release_slot(selection, tmp_path, monkeypatch):
    from okto_nexus_connector.services import r4_launch_recovery as launch_recovery
    from nexus_connector_core.process_launch_control import ProcessLaunchController
    from nexus_connector_core.native.process.launch_exclusion import HostLaunchExclusion
    store, host, journal, ledger, key, launch, outbox, request = await setup(selection, tmp_path, marked=True)
    current = True
    def verify(request):
        if not current: raise ConnectorError('STALE_GENERATION','test','Lost owner')
    async def stopped(*args, **kwargs):
        nonlocal current
        result = await ProcessLaunchController(journal, HostLaunchExclusion()).recover(launch,
            validate_authority=lambda: None, stop_container=lambda _: 'STOPPED')
        await journal.settle_unclaimed_process_launch(launch)
        current = False
        return result
    monkeypatch.setattr(launch_recovery, 'recover_process_launch', stopped)
    try:
        reporter = R4ReconciliationReporter(store,host,'srv','exe',validate_authority=verify)
        with pytest.raises(ConnectorError, match='STALE_GENERATION'):
            await reporter.report(request)
        assert len((await ledger.owned_slot_page()).reservations) == 1
        assert len((await journal.owned_slot_page()).reservations) == 1
        assert (await journal.get_receipt(key)).stage == 'FAILED'
    finally:
        await host.shutdown_all()
