"""Bounded viewers of owned terminals on one R4 connection."""

import asyncio
from contextlib import AsyncExitStack
from dataclasses import dataclass, field

from nexus_connector_core import CoreError, NativeSessionTarget
from nexus_connector_core.presentation_link import (
    CLAUDE_PRESENTATION_CLOSE,
    CLAUDE_PRESENTATION_NEXT,
    CLAUDE_PRESENTATION_OPEN,
    CLAUDE_PRESENTATION_PROBE,
    PI_PRESENTATION_CLOSE,
    PI_PRESENTATION_EDIT,
    PI_PRESENTATION_NEXT,
    PI_PRESENTATION_OPEN,
    PI_PRESENTATION_PROBE,
    encode_pi_presentation_output,
)

_MAX_SESSIONS = 2
_MAX_READERS_PER_SESSION = 2


@dataclass(slots=True)
class _Viewer:
    scope: dict
    editor: bool
    touched: float
    stack: AsyncExitStack = field(default_factory=AsyncExitStack)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    read: object = None
    edit: object = None
    output: asyncio.Task | None = None
    expiry: asyncio.Task | None = None
    sequence: int = 0


class TerminalPresentationStreams:
    def __init__(self, host, *, claude=False, idle_seconds=15, poll_seconds=.35):
        self.host = host
        self._claude = claude
        self._probe, self._open_type, self._next_type, self._close_type = (
            (CLAUDE_PRESENTATION_PROBE, CLAUDE_PRESENTATION_OPEN, CLAUDE_PRESENTATION_NEXT, CLAUDE_PRESENTATION_CLOSE)
            if claude else (PI_PRESENTATION_PROBE, PI_PRESENTATION_OPEN, PI_PRESENTATION_NEXT, PI_PRESENTATION_CLOSE))
        self._idle_seconds = idle_seconds
        self._poll_seconds = poll_seconds
        self._viewers = {}

    async def request(self, frame, verify_lane):
        action, identity = frame['type'], frame['stream_id']
        if action in (self._probe, self._open_type):
            return await self._open(frame, verify_lane)
        viewer = self._viewers.get(identity)
        if viewer is None:
            return {'state': 'closed' if action == self._close_type else 'refused'}
        async with viewer.lock:
            if self._viewers.get(identity) is not viewer:
                return {'state': 'closed'}
            if action == self._close_type:
                await self._retire(identity, viewer)
                return {'state': 'closed'}
            try:
                verify_lane(viewer.scope)
                viewer.touched = asyncio.get_running_loop().time()
                if not self._claude and action == PI_PRESENTATION_EDIT:
                    if viewer.edit is None or frame['sequence'] != viewer.sequence + 1:
                        return {'state': 'refused'}
                    await viewer.edit(frame['key'], text=frame['text'])
                    verify_lane(viewer.scope)
                    viewer.sequence = frame['sequence']
                    return {'state': 'edited'}
                if action == self._next_type:
                    return await self._next(identity, viewer, verify_lane)
                return {'state': 'refused'}
            except (CoreError, OSError, TimeoutError, ValueError):
                await self._retire(identity, viewer)
                return {'state': 'refused'}
            except asyncio.CancelledError:
                await self._retire(identity, viewer)
                raise

    async def _open(self, frame, verify_lane):
        scope = dict(frame['scope'])
        target = NativeSessionTarget(**frame['target'])
        connection = {key: frame[key] for key in ('connection_id', 'connection_generation')}
        verify_lane(scope)
        if frame['type'] == self._probe:
            prove = self.host.prove_r4_claude_presentation if self._claude else self.host.prove_r4_pi_presentation
            await prove(scope, target, **connection)
            verify_lane(scope)
            return {'state': 'available'}

        identity = frame['stream_id']
        if identity in self._viewers:
            return {'state': 'refused'}
        active_sessions = {other.scope['session_id'] for other in self._viewers.values()}
        session_id = scope['session_id']
        if session_id not in active_sessions and len(active_sessions) >= _MAX_SESSIONS:
            return {'state': 'refused'}
        editor = False if self._claude else frame['editor']
        same_mode_count = sum(other.scope['session_id'] == session_id and other.editor == editor
                              for other in self._viewers.values())
        if same_mode_count >= (1 if editor else _MAX_READERS_PER_SESSION):
            return {'state': 'refused'}
        viewer = _Viewer(scope, editor, asyncio.get_running_loop().time())
        self._viewers[identity] = viewer
        async with viewer.lock:
            try:
                if self._claude:
                    opening = self.host.open_r4_claude_presentation(scope, target, **connection)
                else:
                    options = {field: frame[field] for field in ('columns', 'rows', 'editor')}
                    opening = self.host.open_r4_pi_presentation(scope, target, **connection, **options)
                viewer.read, viewer.edit = await viewer.stack.enter_async_context(
                    opening)
                verify_lane(scope)
            except BaseException:
                await self._retire(identity, viewer)
                raise
            viewer.expiry = asyncio.create_task(self._expire(identity, viewer), name='r4-terminal-presentation-expiry')
        return {'state': 'opened'}

    async def _next(self, identity, viewer, verify_lane):
        if viewer.output is None:
            viewer.output = asyncio.create_task(viewer.read(), name='r4-terminal-presentation-output')
        done, _ = await asyncio.wait((viewer.output,), timeout=self._poll_seconds)
        if not done:
            return {'state': 'idle'}
        task, viewer.output = viewer.output, None
        value = task.result()
        verify_lane(viewer.scope)
        if value is None:
            await self._retire(identity, viewer)
            return {'state': 'closed'}
        return encode_pi_presentation_output(value)

    async def _expire(self, identity, viewer):
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(max(0, viewer.touched + self._idle_seconds - loop.time()))
            async with viewer.lock:
                if loop.time() - viewer.touched >= self._idle_seconds:
                    await self._retire(identity, viewer)
                    return

    async def _retire(self, identity, viewer):
        if self._viewers.get(identity) is not viewer:
            return
        del self._viewers[identity]
        tasks = {task for task in (viewer.expiry, viewer.output) if task is not None and task is not asyncio.current_task()}
        for task in tasks:
            task.cancel()
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await viewer.stack.aclose()

    async def reset(self):
        """Retire every presentation before closing or replacing the socket."""
        for identity, viewer in tuple(self._viewers.items()):
            async with viewer.lock:
                await self._retire(identity, viewer)
