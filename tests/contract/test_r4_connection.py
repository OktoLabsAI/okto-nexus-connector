"""Single-reader correlation, bounded reservations and owned request completion."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
import json

import pytest
from nexus_connector_core import (
    CoreError, InstallationCandidate, R4_PREVIEW_REVISION, ShutdownPolicy, create_runtime,
    decode_r4_frame, encode_r4_frame, r4_submit_intent_hash,
)
from nexus_connector_core.discovery import fingerprint
from nexus_connector_core.journal import open_journal
from nexus_connector_core.native.pi_renderer import PiEditorUpdate
from nexus_connector_core.targeted_control_link import (
    CLAUDE_OPERATION_INTERRUPT_CODEC, CLAUDE_OPERATION_STEER_CODEC, PI_RUN_CONTROL_CODEC,
)
from nexus_connector_core.presentation_link import (
    PI_PRESENTATION_CLOSE, PI_PRESENTATION_EDIT, PI_PRESENTATION_LINK_REVISION, PI_PRESENTATION_NEXT,
    PI_PRESENTATION_OPEN, PI_PRESENTATION_PROBE, decode_pi_presentation_frame, decode_pi_presentation_output,
    encode_pi_presentation_frame, presentation_request_hash,
)

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.transport.terminal_presentation import TerminalPresentationStreams
from okto_nexus_connector.transport.r4_connection import R4Connection
from okto_nexus_connector.transport.wss_r4 import R4ControlState, apply_r4_lease


BASE = dict(protocol_major=1, contract_revision=R4_PREVIEW_REVISION, server_id='srv', executor_id='exe')
STATE = R4ControlState('srv', 'exe', 'conn', 2, True)
CHANNEL = dict(connection_id='conn', connection_generation=2)
SCOPE = dict(server_id='srv', executor_id='exe', binding_id='binding', agent_id='agent',
    workspace_id='ws', workspace_binding_id='wxb', session_id='session', session_owner_generation=1,
    authorization_revision=1, configuration_revision=1, binding_revision=1, credential_epoch=1)


@pytest.mark.parametrize('failure', ['timeout', 'error'])
async def test_failed_graceful_close_aborts_and_confirms_transport(failure):
    class LostSocket:
        def __init__(self):
            self.transport = self
            self.aborted = asyncio.Event()
        async def close(self):
            if failure == 'error': raise OSError('peer lost')
            await asyncio.Event().wait()
        def abort(self): self.aborted.set()
        async def wait_closed(self): await self.aborted.wait()
    socket = LostSocket()
    owner = R4Connection(socket, STATE, boot_id='boot', send_timeout=.01)
    await owner.close()
    assert socket.aborted.is_set()
    assert not owner.online and owner.close_error is None
    with pytest.raises(ConnectorError): owner._require_online()


async def test_unconfirmed_abort_keeps_cleanup_failure():
    class UnclosedSocket:
        transport = None
        async def close(self): raise OSError('cannot close')
    owner = R4Connection(UnclosedSocket(), STATE, boot_id='boot', send_timeout=.01)
    await owner.close()
    assert isinstance(owner.close_error, OSError)
    assert not owner.online


async def test_failed_socket_close_can_be_retried():
    class RetrySocket:
        transport = None
        attempts = 0
        async def close(self):
            self.attempts += 1
            if self.attempts == 1:
                raise OSError('temporary close failure')
    socket = RetrySocket()
    owner = R4Connection(socket, STATE, boot_id='boot', send_timeout=.01)
    await owner.close()
    assert isinstance(owner.close_error, OSError)
    await owner.close()
    assert socket.attempts == 2 and owner.close_error is None
    assert not owner.online


class Socket:
    def __init__(self):
        self.incoming, self.outgoing = asyncio.Queue(), asyncio.Queue()
        self.reading = 0
        self.peak_readers = 0
        self.closed = False
        self.lease_requests = set()
        self.confirm_replays = True

    async def recv(self):
        self.reading += 1
        self.peak_readers = max(self.reading, self.peak_readers)
        try:
            return await self.incoming.get()
        finally:
            self.reading -= 1

    async def send(self, raw):
        frame = decode_r4_frame(raw.encode())
        await self.outgoing.put(frame)
        if frame['type'] == 'lease.renew':
            if frame['request_id'] in self.lease_requests and self.confirm_replays:
                await self.emit(granted(frame))
            self.lease_requests.add(frame['request_id'])

    async def close(self):
        self.closed = True

    async def emit(self, frame):
        await self.incoming.put(encode_r4_frame(frame).decode())


def attached(request):
    return {**BASE, **CHANNEL, 'type': 'binding.attached', 'expires_in': 60,
        **{k: request[k] for k in ('attach_request_id', 'binding_id', 'agent_id',
            'credential_epoch', 'authorization_revision', 'configuration_revision')}}


async def attach(owner, socket, **changes):
    args = dict(binding_id='binding', agent_id='agent', ticket='t' * 32,
                credential_epoch=1, authorization_revision=1, configuration_revision=1)
    task = asyncio.create_task(owner.attach_binding(**{**args, **changes}))
    request = await asyncio.wait_for(socket.outgoing.get(), 2)
    await socket.emit(attached(request))
    await asyncio.wait_for(task, 2)


async def test_saved_codex_probe_checks_exact_closed_history_and_current_lane(tmp_path):
    from nexus_connector_core.native.codex_rollout import observe_rollout
    from nexus_connector_core.saved_context_link import (
        SAVED_CODEX_LINK_REVISION, SAVED_CODEX_PROBE,
        decode_saved_codex_frame, encode_saved_codex_frame, saved_codex_result,
    )
    from okto_nexus_connector.services.r4_saved_codex import probe_saved_codex
    from okto_nexus_connector.storage.r4_session_releases import R4SessionReleaseStore

    class ProbeSocket(Socket):
        async def send(self, raw):
            if SAVED_CODEX_LINK_REVISION in raw:
                await self.outgoing.put(decode_saved_codex_frame(raw.encode()))
            else:
                await super().send(raw)

    home = tmp_path / 'profile'
    rollout = home / 'sessions' / 'rollout-test.jsonl'
    rollout.parent.mkdir(parents=True)
    thread = 'bdfe5b81-54cf-43cf-8001-5de670d3dd69'
    header = json.dumps({'type': 'session_meta', 'payload': {'id': thread, 'cwd': str(tmp_path)}})
    rollout.write_text(header + '\n{"type":"response_item"}\n', encoding='utf-8')
    evidence = observe_rollout(str(home), str(rollout), thread, str(tmp_path))
    assert evidence is not None
    source = dict(source_connection_id='prior', source_connection_generation=1,
        opening_operation_id='open', close_operation_id='close', stream_epoch='old-stream',
        opening_binding_digest='sha256:' + 'a' * 64, close_binding_digest='sha256:' + 'b' * 64,
        resource_release_digest='sha256:' + 'c' * 64)
    store = R4SessionReleaseStore(tmp_path / 'releases.sqlite3')
    store.record_saved_codex_context(dict(source, scope=SCOPE, workspace_root=str(tmp_path), rollout=asdict(evidence)))
    calls = []

    async def observe(frame):
        calls.append(frame['request_id'])
        return await asyncio.to_thread(probe_saved_codex, store, frame)

    socket = ProbeSocket()
    state = replace(STATE, control_capabilities=(SAVED_CODEX_LINK_REVISION,))
    owner = R4Connection(socket, state, boot_id='boot', saved_codex_handler=observe)
    probe = dict(CHANNEL, server_id='srv', executor_id='exe', contract_revision=SAVED_CODEX_LINK_REVISION,
                 type=SAVED_CODEX_PROBE, request_id='before-attach', scope=SCOPE, source=source)

    async def check(frame, *, available):
        await socket.incoming.put(encode_saved_codex_frame(frame).decode())
        reply = await asyncio.wait_for(socket.outgoing.get(), 2)
        assert reply == saved_codex_result(frame, available=available)

    owner.start()
    try:
        await check(probe, available=False)
        assert calls == []
        await attach(owner, socket)
        await check(probe | {'request_id': 'intact'}, available=True)
        await check(probe | {'request_id': 'wrong-close', 'source': source | {'close_operation_id': 'other'}},
                    available=False)
        with rollout.open('ab') as stream:
            stream.write(b'{"type":"changed"}\n')
        await check(probe | {'request_id': 'drifted'}, available=False)
        assert calls == ['intact', 'wrong-close', 'drifted']
    finally:
        await owner.close()


async def test_pi_presentation_control_pulls_exact_editor_and_retires_on_disconnect():
    class PiSocket(Socket):
        async def send(self, raw):
            if PI_PRESENTATION_LINK_REVISION in raw:
                await self.outgoing.put(decode_pi_presentation_frame(raw.encode()))
            else:
                await super().send(raw)

    calls, viewports = [], []
    output = asyncio.Queue()
    retired = asyncio.Event()

    class Host:
        async def prove_r4_pi_presentation(self, scope, target, **channel):
            assert scope == SCOPE and target.native_session_id == 'owned' and channel == CHANNEL
            calls.append('prove')

        @asynccontextmanager
        async def open_r4_pi_presentation(self, scope, target, *, columns, rows, editor, **channel):
            await self.prove_r4_pi_presentation(scope, target, **channel)
            viewports.append((columns, rows, editor))

            async def edit(key, *, text):
                calls.append((key, text))
                await output.put(PiEditorUpdate(1, 'Draft accepted by the editor'))

            try:
                yield output.get, edit if editor else None
            finally:
                retired.set()

    socket = PiSocket()
    owner = R4Connection(socket, replace(STATE, control_capabilities=(PI_PRESENTATION_LINK_REVISION,)),
                         boot_id='boot', pi_presentation_host=Host())
    owner.start()
    serial = 0
    stream = 'piv_' + 'a' * 32

    async def request(kind, *, stream_id=stream, **payload):
        nonlocal serial
        serial += 1
        frame = {'type': kind, 'contract_revision': PI_PRESENTATION_LINK_REVISION, 'server_id': 'srv',
                 'executor_id': 'exe', **CHANNEL, 'stream_id': stream_id, 'request_id': f'request-{serial}', **payload}
        await socket.incoming.put(encode_pi_presentation_frame(frame).decode())
        result = await asyncio.wait_for(socket.outgoing.get(), 2)
        assert result['request_hash'] == presentation_request_hash(frame) and result['stream_id'] == stream_id
        return result['result']

    scope = {'scope': SCOPE, 'target': dict(adapter_id='pi_rpc', native_session_id='owned',
                                         opening_operation_id='opening', stream_epoch='epoch')}
    options = {'columns': 80, 'rows': 24, 'editor': True}
    try:
        assert (await request(PI_PRESENTATION_PROBE, **scope))['state'] == 'refused'
        assert not calls
        await attach(owner, socket)
        assert (await request(PI_PRESENTATION_PROBE, **scope))['state'] == 'available'
        assert not viewports
        assert (await request(PI_PRESENTATION_OPEN, **scope, **options))['state'] == 'opened'
        assert viewports == [(80, 24, True)]
        assert (await request(PI_PRESENTATION_OPEN, stream_id='piv_' + 'b' * 32, **scope, **options))['state'] == 'refused'
        assert (await request(PI_PRESENTATION_NEXT))['state'] == 'idle'
        assert (await request(PI_PRESENTATION_EDIT, key='enter', text=None, sequence=1))['state'] == 'edited'
        assert (await request(PI_PRESENTATION_EDIT, key='enter', text=None, sequence=1))['state'] == 'refused'
        assert decode_pi_presentation_output(await request(PI_PRESENTATION_NEXT)) == PiEditorUpdate(
            1, 'Draft accepted by the editor')
        assert calls == ['prove', 'prove', ('enter', None)]

        readers = ['piv_' + char * 32 for char in 'cde']
        reader_options = options | {'editor': False}
        for reader in readers[:2]:
            assert (await request(PI_PRESENTATION_OPEN, stream_id=reader, **scope, **reader_options))['state'] == 'opened'
        assert (await request(PI_PRESENTATION_OPEN, stream_id=readers[2], **scope, **reader_options))['state'] == 'refused'
        for reader in readers[:2]:
            assert (await request(PI_PRESENTATION_EDIT, stream_id=reader, key='enter', text=None, sequence=1))['state'] == 'refused'
            assert (await request(PI_PRESENTATION_CLOSE, stream_id=reader))['state'] == 'closed'
        retired.clear()

        assert (await request(PI_PRESENTATION_NEXT))['state'] == 'idle'
        owner._lanes.clear()
        assert (await request(PI_PRESENTATION_EDIT, key='enter', text=None, sequence=2))['state'] == 'refused'
        assert retired.is_set() and owner.online and not socket.closed
        assert (await request(PI_PRESENTATION_CLOSE))['state'] == 'closed'

        retired.clear()
        await attach(owner, socket)
        reader = 'piv_' + 'c' * 32
        assert (await request(PI_PRESENTATION_OPEN, stream_id=reader, **scope, **(options | {'editor': False})))['state'] == 'opened'
        assert (await request(PI_PRESENTATION_EDIT, stream_id=reader, key='enter', text=None, sequence=1))['state'] == 'refused'
        assert (await request(PI_PRESENTATION_NEXT, stream_id=reader))['state'] == 'idle'
    finally:
        await owner.close()
    assert retired.is_set() and socket.closed
    assert calls.count(('enter', None)) == 1


async def test_cancelled_pi_draft_edit_cannot_be_replayed_on_the_same_viewer():
    entered, retired = asyncio.Event(), asyncio.Event()
    calls = []

    class Host:
        @asynccontextmanager
        async def open_r4_pi_presentation(self, *args, **kwargs):
            async def edit(key, *, text):
                calls.append((key, text))
                entered.set()
                await asyncio.Future()

            try:
                yield None, edit
            finally:
                retired.set()

    streams = TerminalPresentationStreams(Host())
    identity = {'stream_id': 'piv_' + 'a' * 32, **CHANNEL}
    target = dict(adapter_id='pi_rpc', native_session_id='owned', opening_operation_id='opening', stream_epoch='epoch')
    assert await streams.request(identity | {'type': PI_PRESENTATION_OPEN, 'scope': SCOPE, 'target': target,
        'columns': 80, 'rows': 24, 'editor': True}, lambda _: None) == {'state': 'opened'}
    edit = identity | {'type': PI_PRESENTATION_EDIT, 'key': 'insert', 'text': 'uncertain draft', 'sequence': 1}
    task = asyncio.create_task(streams.request(edit, lambda _: None))
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert retired.is_set()
    assert await streams.request(edit, lambda _: None) == {'state': 'refused'}
    assert calls == [('insert', 'uncertain draft')]


@pytest.mark.parametrize('reject',[False,True])
async def test_event_artifact_transport_is_correlated_and_does_not_ack_events(reject):
    from dataclasses import replace
    from nexus_connector_core.event_artifacts import (EVENT_ARTIFACT_REVISION,externalize_event,
        decode_event_artifact,encode_event_artifact,artifact_ack)
    class ArtifactSocket(Socket):
        async def send(self,raw):
            if 'event.artifact.put' in raw:
                frame=decode_event_artifact(raw.encode())
                await self.outgoing.put(frame)
            else:
                await super().send(raw)
    socket=ArtifactSocket()
    owner=R4Connection(socket,replace(STATE,control_capabilities=(EVENT_ARTIFACT_REVISION,)),boot_id='boot')
    owner.start()
    try:
        await attach(owner,socket)
        event=dict(server_id='srv',executor_id='exe',session_id='session',stream_epoch='epoch',sequence=1,
                   category='tool_activity',payload={'text':'x'*95000})
        wire,raw=externalize_event(event)
        scope=dict(binding_id='binding',agent_id='agent')
        task=asyncio.create_task(owner.publish_event_artifact(scope,wire,raw))
        for offset in range(0,len(raw),32768):
            frame=await asyncio.wait_for(socket.outgoing.get(),3)
            assert frame['offset']==offset
            reply=artifact_ack(frame)
            if reject:
                reply=dict(reply,type='event.artifact.rejected',code='QUOTA_EXCEEDED')
            await socket.incoming.put(encode_event_artifact(reply).decode())
            if reject: break
        if reject:
            with pytest.raises(CoreError,match='QUOTA_EXCEEDED'): await asyncio.wait_for(task,3)
        else:
            await asyncio.wait_for(task,3)
        # Duplicate/late artifact replies have no authority to disconnect lanes.
        await socket.incoming.put(encode_event_artifact(reply).decode())
        await asyncio.sleep(.02)
        assert owner._event_written=={} and socket.peak_readers==1 and owner.online
    finally:
        await owner.close()


def operation(operation_id='op', action='turn.submit'):
    payload = {'text': 'Hello'} if action in ('turn.submit', 'turn.steer') else {'reason': 'Stop'}
    frame = {**BASE, **CHANNEL, **SCOPE, 'type': 'operation.submit',
             'operation_id': operation_id, 'action': action, 'payload': payload,
             'grant_id': 'grant', 'expected_turn_id': None}
    frame['intent_hash'] = r4_submit_intent_hash(frame)
    return frame


@pytest.mark.parametrize('mode', ['current', 'unnegotiated', 'stale', 'oversize'])
@pytest.mark.parametrize('codec,target,action', [
    (PI_RUN_CONTROL_CODEC, 'prun_' + '1' * 32, 'turn.interrupt'),
    (CLAUDE_OPERATION_STEER_CODEC, 'op_reviewed_target', 'turn.steer'),
    (CLAUDE_OPERATION_INTERRUPT_CODEC, 'op_reviewed_target', 'turn.interrupt'),
])
async def test_control_reserves_only_a_negotiated_current_exact_target(mode, codec, target, action):
    socket = Socket()
    capabilities = () if mode == 'unnegotiated' else (codec.revision,)
    state = replace(STATE, control_capabilities=capabilities)
    frame = operation('reviewed-control', action)
    if mode == 'stale':
        frame['connection_generation'] = 1
    raw = codec.encode(frame, target)
    limit = len(raw) - 1 if mode == 'oversize' else len(raw)
    owner = R4Connection(socket, state, boot_id='boot', control_bytes=limit)
    owner.start()
    try:
        await attach(owner, socket)
        await socket.incoming.put(raw.decode())
        if mode == 'current':
            item = await asyncio.wait_for(owner.receive_operation(control=True), 2)
            assert item.encoded == raw and item.target == (codec, target)
            assert owner.require_current(item) == frame
            owner.release_operation(item)
        else:
            with pytest.raises(ConnectorError):
                await asyncio.wait_for(owner.receive_operation(control=True), 2)
        assert owner.pending_operations == 0
    finally:
        await owner.close()


def granted(request):
    return {'protocol_major': 1, 'contract_revision': R4_PREVIEW_REVISION,
            'type': 'lease.granted', 'request_id': request['request_id'],
            'grant_id': request['grant_id'], 'scope': request['scope'],
            'lease_id': 'lease-' + request['scope']['session_id'], 'lease_serial': 1,
            'valid_for_ms': 60000, 'allowed_actions': []}


@pytest.fixture
async def runtime(tmp_path):
    binary = tmp_path / 'codex.exe'
    binary.write_bytes(b'Synthetic transport candidate')
    candidate = InstallationCandidate('codex_app_server', str(binary), fingerprint(binary), 'explicit', 'selected')
    journal = await open_journal(tmp_path / 'core.db')
    async def environment(_):
        raise AssertionError('The transport must not start a harness.')
    core = create_runtime(journal=journal, environment=environment,
        candidates={candidate.adapter_id: candidate}, workspace_roots={'ws': str(tmp_path)})
    try:
        yield core
    finally:
        await core.shutdown(ShutdownPolicy(0, 0))
        await journal.aclose()


async def test_reader_correlates_reordered_leases_with_interleaved_operation(runtime):
    socket = Socket()
    owner = R4Connection(socket, STATE, boot_id=runtime.r4_boot_id)
    owner.start()
    try:
        await attach(owner, socket)
        tasks = [asyncio.create_task(apply_r4_lease(owner, STATE, runtime,
            scope={**SCOPE, 'session_id': name}, grant_id='grant')) for name in ('a', 'b')]
        requests = [await asyncio.wait_for(socket.outgoing.get(), 2) for _ in range(2)]
        await socket.emit(granted(requests[1]))
        await socket.emit(operation())
        await socket.emit(granted(requests[0]))
        applications = await asyncio.wait_for(asyncio.gather(*tasks), 2)
        assert [a.context.r4_authority.session_id for a in applications] == ['a', 'b']
        sent = [await socket.outgoing.get() for _ in range(4)]
        acknowledgements = [frame for frame in sent if frame['type'] == 'lease.applied']
        assert {a['lease_id'] for a in acknowledgements} == {'lease-a', 'lease-b'}
        assert {r['request_id'] for r in sent if r['type'] == 'lease.renew'} == {r['request_id'] for r in requests}
        item = await asyncio.wait_for(owner.receive_operation(), 2)
        assert owner.require_current(item)['operation_id'] == 'op'
        assert socket.peak_readers == 1
        owner.release_operation(item)
        assert owner.usage == {False: (0, 0), True: (0, 0)}
    finally:
        await owner.close()


async def test_cancelled_lease_waiter_preserves_installation_and_ack(runtime):
    socket = Socket()
    owner = R4Connection(socket, STATE, boot_id=runtime.r4_boot_id)
    owner.start()
    try:
        waiter = asyncio.create_task(apply_r4_lease(owner, STATE, runtime, scope=SCOPE, grant_id='grant'))
        request = await asyncio.wait_for(socket.outgoing.get(), 2)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert len(owner._producers) == 1
        await socket.emit(granted(request))
        ack = await asyncio.wait_for(socket.outgoing.get(), 2)
        assert ack['type'] == 'lease.applied' and ack['lease_serial'] == 1
        assert socket.peak_readers == 1
        async with asyncio.timeout(2):
            while owner._producers:
                await asyncio.sleep(0)
    finally:
        await owner.close()


async def test_lease_application_waits_for_server_commit_without_reanchoring(runtime):
    socket = Socket()
    socket.confirm_replays = False
    owner = R4Connection(socket, STATE, boot_id=runtime.r4_boot_id)
    owner.start()
    waiter = asyncio.create_task(owner.apply_lease(runtime, scope=SCOPE, grant_id='grant'))
    try:
        request = await socket.outgoing.get()
        grant = granted(request)
        await socket.emit(grant)
        ack = await asyncio.wait_for(socket.outgoing.get(), 2)
        replay = await asyncio.wait_for(socket.outgoing.get(), 2)
        assert ack['type'] == 'lease.applied' and replay == request
        assert not waiter.done()
        await socket.emit(grant)
        application = await asyncio.wait_for(waiter, 2)
        assert application.acknowledgement == ack
        assert application.context.r4_authority.lease_serial == 1
    finally:
        await owner.close()
        await asyncio.gather(waiter, return_exceptions=True)


async def test_cancelled_close_waiter_does_not_abandon_install_producer(runtime):
    socket = Socket()
    owner = R4Connection(socket, STATE, boot_id=runtime.r4_boot_id)
    entered, release = asyncio.Event(), asyncio.Event()
    class DelayedInstallation:
        begin_r4_lease_request = runtime.begin_r4_lease_request
        async def install_r4_lease(self, attempt, grant):
            application = await runtime.install_r4_lease(attempt, grant)
            entered.set()
            await release.wait()
            return application
    owner.start()
    waiter = asyncio.create_task(owner.apply_lease(DelayedInstallation(), scope=SCOPE, grant_id='grant'))
    try:
        request = await asyncio.wait_for(socket.outgoing.get(), 2)
        await socket.emit(granted(request))
        await asyncio.wait_for(entered.wait(), 2)
        closing = asyncio.create_task(owner.close())
        # Wait for the observable socket close while installation is blocked.
        async with asyncio.timeout(2):
            while not socket.closed:
                await asyncio.sleep(0)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert len(owner._producers) == 1 and not waiter.done()
        release.set()
        with pytest.raises(ConnectorError):
            await asyncio.wait_for(waiter, 2)
        await owner.close()
        assert not owner._producers and socket.outgoing.empty()
    finally:
        release.set()
        await owner.close()
        await asyncio.gather(waiter, return_exceptions=True)


@pytest.mark.parametrize('limit', ['items', 'bytes'])
async def test_control_capacity_is_independent_and_taken_reservation_survives_close(limit):
    socket = Socket()
    owner = R4Connection(socket, STATE, boot_id='boot',
        regular_items=1 if limit == 'items' else 8, control_items=1,
        regular_bytes=len(encode_r4_frame(operation())) if limit == 'bytes' else 256 * 1024)
    owner.start()
    try:
        await attach(owner, socket)
        await socket.emit(operation())
        productive = await asyncio.wait_for(owner.receive_operation(), 2)
        await socket.emit(operation('stop', 'turn.interrupt'))
        control = await asyncio.wait_for(owner.receive_operation(control=True), 2)
        assert owner.require_current(control)['operation_id'] == 'stop'
        owner.release_operation(control)
        with pytest.raises(RuntimeError):
            owner.release_operation(control)
        await socket.emit(operation('overflow'))
        await asyncio.wait_for(owner._reader, 2)
        assert not owner.online and owner.failure.code == 'CAPACITY_EXCEEDED'
        assert owner.usage[False][0] == 1
        with pytest.raises(ConnectorError):
            owner.require_current(productive)
        owner.release_operation(productive)
        assert owner.usage == {False: (0, 0), True: (0, 0)}
    finally:
        await owner.close()


async def test_late_attach_cannot_restore_old_lane_or_queued_authority():
    socket = Socket()
    owner = R4Connection(socket, STATE, boot_id='boot')
    owner.start()
    try:
        await attach(owner, socket)
        await socket.emit(operation())
        item = await asyncio.wait_for(owner.receive_operation(), 2)
        old = asyncio.create_task(owner.attach_binding(binding_id='binding', agent_id='agent',
            ticket='a' * 32, credential_epoch=1, authorization_revision=1, configuration_revision=1))
        old_request = await socket.outgoing.get()
        new = asyncio.create_task(owner.attach_binding(binding_id='binding', agent_id='agent',
            ticket='b' * 32, credential_epoch=2, authorization_revision=2, configuration_revision=1))
        new_request = await socket.outgoing.get()
        await socket.emit(attached(new_request))
        await new
        await socket.emit(attached(old_request))
        with pytest.raises(CoreError):
            await old
        assert owner._lanes['binding'].credential_epoch == 2
        with pytest.raises(CoreError):
            owner.require_current(item)
        owner.release_operation(item)
        assert owner.online
    finally:
        await owner.close()


@pytest.mark.parametrize('failure', ['foreign', 'timeout'])
async def test_invalid_or_absent_lease_reply_fences_connection(runtime, failure):
    socket = Socket()
    owner = R4Connection(socket, STATE, boot_id=runtime.r4_boot_id, request_timeout=0.05)
    owner.start()
    try:
        waiter = asyncio.create_task(owner.apply_lease(runtime, scope=SCOPE, grant_id='grant'))
        request = await socket.outgoing.get()
        if failure == 'foreign':
            reply = granted(request)
            reply['scope'] = {**reply['scope'], 'executor_id': 'foreign'}
            await socket.emit(reply)
        with pytest.raises((ConnectorError, asyncio.TimeoutError)):
            await asyncio.wait_for(waiter, 2)
        assert not owner.online
        assert socket.outgoing.empty()
    finally:
        await owner.close()


def event(sequence):
    return dict(server_id="srv",executor_id="exe",session_id="session",stream_epoch="epoch",
                sequence=sequence,category="text_delta",payload={"text":"Hello"})


def event_ack(sequence, **changes):
    return dict(**BASE,**CHANNEL,type="event.ack",binding_id="binding",agent_id="agent",
                session_id="session",stream_epoch="epoch",sequence=sequence) | changes


async def publish_event(owner, sequence):
    return await owner.publish_events(binding_id="binding",agent_id="agent",session_id="session",
                                      stream_epoch="epoch",events=[event(sequence)])


async def test_event_ack_targets_and_late_duplicate_do_not_advance_next_batch():
    socket=Socket()
    owner=R4Connection(socket,STATE,boot_id="boot",request_timeout=.1)
    owner.start()
    try:
        await attach(owner,socket)
        first=asyncio.create_task(publish_event(owner,1))
        await socket.outgoing.get()
        await socket.emit(event_ack(1))
        assert (await first)["sequence"]==1
        second=asyncio.create_task(publish_event(owner,2))
        await socket.outgoing.get()
        await socket.emit(event_ack(1))
        with pytest.raises(TimeoutError): await second
        assert owner.online
        # The same stable batch is retried; late ACK1 cannot satisfy it.
        retry=asyncio.create_task(publish_event(owner,2))
        await socket.outgoing.get()
        await socket.emit(event_ack(2))
        assert (await retry)["sequence"]==2
        await socket.emit(event_ack(1))
        await socket.emit(operation())
        item=await asyncio.wait_for(owner.receive_operation(),1)
        owner.release_operation(item)
        assert owner.online and socket.peak_readers==1
    finally:
        await owner.close()


@pytest.mark.parametrize("changes",[{"sequence":2},{"stream_epoch":"foreign"},{"agent_id":"foreign"},
                                    {"connection_generation":1},{"connection_id":"foreign"}])
async def test_event_ack_rejects_unwritten_or_cross_scoped_fact(changes):
    socket=Socket()
    owner=R4Connection(socket,STATE,boot_id="boot",request_timeout=.2)
    owner.start()
    try:
        await attach(owner,socket)
        pending=asyncio.create_task(publish_event(owner,1))
        await socket.outgoing.get()
        ack=event_ack(1)
        ack.update(changes)
        await socket.emit(ack)
        with pytest.raises(ConnectorError):
            await pending
        assert not owner.online
    finally:
        await owner.close()


async def test_recovery_lane_transfers_before_first_dispatch():
    from dataclasses import replace
    from okto_nexus_connector.transport.r4_recovery import R4RecoveryChannel
    socket=Socket()
    recovery=R4RecoveryChannel(socket,replace(STATE,control_ready=False),"boot")
    attaching=asyncio.create_task(recovery.attach_binding(binding_id="binding",agent_id="agent",ticket="t"*32,
        credential_epoch=1,authorization_revision=1,configuration_revision=1))
    request=await socket.outgoing.get()
    await socket.emit(attached(request))
    await attaching
    pending=asyncio.create_task(recovery.publish_events(binding_id="binding",agent_id="agent",session_id="session",
                                                       stream_epoch="epoch",events=[event(1)]))
    await socket.outgoing.get()
    await socket.emit(event_ack(1))
    assert (await pending)["sequence"]==1
    recovery.online=False
    await socket.emit(operation())
    owner=R4Connection(socket,STATE,boot_id="boot",initial_lanes=recovery.lanes)
    owner.start()
    try:
        item=await asyncio.wait_for(owner.receive_operation(),1)
        owner.release_operation(item)
        assert owner.online and socket.peak_readers==1
        with pytest.raises(CoreError):
            await recovery.publish_events(binding_id="binding",agent_id="agent",session_id="session",
                                          stream_epoch="epoch",events=[event(2)])
    finally: await owner.close()


async def test_lease_authority_is_rechecked_after_reply_before_installation(runtime):
    socket=Socket()
    owner=R4Connection(socket,STATE,boot_id=runtime.r4_boot_id)
    owner.start()
    calls=[]
    class GuardedRuntime:
        begin_r4_lease_request=runtime.begin_r4_lease_request
        async def install_r4_lease(self,attempt,grant):
            raise AssertionError("A stale authority reached Core installation.")
    async def current():
        calls.append(1)
        if len(calls)==3:
            raise ConnectorError("STALE_GENERATION","test_authority")
    waiter=asyncio.create_task(owner.apply_lease(GuardedRuntime(),scope=SCOPE,grant_id="grant",
        require_current=current,fence_on_error=False))
    try:
        request=await asyncio.wait_for(socket.outgoing.get(),2)
        await socket.emit(granted(request))
        with pytest.raises(ConnectorError,match="STALE_GENERATION"):
            await asyncio.wait_for(waiter,2)
        assert len(calls)==3 and socket.outgoing.empty() and owner.online
    finally:
        await owner.close()
