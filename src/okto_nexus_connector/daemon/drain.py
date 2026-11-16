"""Operator drain policy; observing a deadline never authorizes containment."""
from __future__ import annotations

import math
import time

from ..errors import ConnectorError


class OperatorDrain:
    def __init__(self, *, persist, begin, clock=time.monotonic):
        self.persist, self.begin, self.clock = persist, begin, clock
        self.deadline = None
        self.force_requested = False
        self.phase = 'waiting'
        self.observation = 'UNKNOWN'

    def request(self, *, force=False, timeout_seconds=60):
        if type(force) is not bool or type(timeout_seconds) not in (int, float) or not (
                math.isfinite(timeout_seconds) and 0 < timeout_seconds <= 300):
            raise ConnectorError('VALIDATION_ERROR', 'shutdown', 'Invalid drain request.')
        if force and (self.deadline is None or self.clock() < self.deadline):
            raise ConnectorError('DRAIN_NOT_EXPIRED', 'shutdown',
                'Force requires an existing drain whose deadline has expired.')
        if self.deadline is None:
            # Persistence precedes every effect, including closing admission.
            self.persist({'desired': 'draining', 'force_requested': False})
            self.deadline = self.clock() + timeout_seconds
            self.begin()
        if force and not self.force_requested:
            self.persist({'desired': 'draining', 'force_requested': True})
            self.force_requested = True
        return self.status()

    def status(self):
        return dict(state='RUNNING' if self.deadline is None else 'DRAINING_PENDING',
            phase=self.phase, observation=self.observation,
            force_requested=self.force_requested,
            force_available=self.deadline is not None and self.clock() >= self.deadline,
            remaining_seconds=None if self.deadline is None else max(0, self.deadline-self.clock()))
