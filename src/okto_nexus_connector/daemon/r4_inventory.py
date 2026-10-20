"""Connection-owned independent inventory; discovery never holds lease renewal."""
from __future__ import annotations

import asyncio

from nexus_connector_core import HarnessObservationResult, IndependentInventoryObserver

from ..services.discovery_configuration import configured_candidates
from ..errors import ConnectorError


class R4InventoryOwner:
    def __init__(self, control, http, snapshot, bootstrap, claim_refresh):
        self.control, self.http, self.scope = control, http, snapshot
        self.bootstrap, self.claim_refresh = bootstrap, claim_refresh
        self.published_batch = None
        self.failure = None
        self._stop = asyncio.Event()
        self._task = self._close_task = None
        self._delivery = self._frontier = None
        self.observer = IndependentInventoryObserver(self._observe,
            clock=lambda: control.clock())

    async def _observe(self, adapter_id, cancel_requested):
        await self.control._require(self.scope)
        if self.control.discover is not None:
            candidates = tuple(await self.control.discover(adapter_ids=(adapter_id,),
                cancel_requested=cancel_requested))
        else:
            record = self.scope[0]
            candidates = tuple(await configured_candidates(record.discovery_configuration,
                adapter_ids=(adapter_id,), observations=record.installation_observations,
                cancel_requested=cancel_requested))
        await self.control._require(self.scope)
        return HarnessObservationResult(candidates)

    def start(self):
        if self._task is not None or self._stop.is_set():
            raise RuntimeError('The inventory producer cannot restart.')
        self._task = asyncio.create_task(self._run(), name='r4-independent-inventory')

    async def require_batch(self, adapter_id):
        """Read accepted evidence without rediscovery or waiting on siblings."""
        await self.control._require(self.scope)
        if self.failure is not None:
            raise self.failure
        batch = self.published_batch
        row = next((r for r in batch.observations if r.adapter_id == adapter_id), None) if batch else None
        elapsed = max(0, int((self.control.clock() - batch.assembled_at_monotonic) * 1000)) if batch else 0
        if (self._stop.is_set() or row is None or row.state not in ('PRESENT', 'ABSENT')
                or row.observation_age_ms is None or row.observation_age_ms + elapsed >= 120_000):
            raise ConnectorError('INVENTORY_NOT_FRESH', 'r4_inventory',
                'The selected harness has no fresh accepted observation.', retry_safe=True)
        return batch

    async def _claim(self):
        delivery = await self.claim_refresh()
        if not self._stop.is_set() and delivery is not None and delivery != self._delivery:
            self._delivery = delivery
            self._frontier = self.observer.request_refresh()

    async def _run(self):
        try:
            # Claims predate the attempts which can acknowledge them.
            await self._claim()
            if self._stop.is_set():
                return
            self.observer.start()
            next_claim = self.control.clock() + 5
            next_publication = 0.0
            while not self._stop.is_set():
                await self.control._require(self.scope)
                now = self.control.clock()
                if now >= next_claim:
                    await self._claim()
                    next_claim = self.control.clock() + 5
                if self._stop.is_set():
                    break
                batch = self.observer.snapshot()
                delivery = self._delivery if (self._frontier is not None
                    and self.observer.refresh_complete(self._frontier)
                    and all(row.state in ('PRESENT', 'ABSENT') and row.observation_age_ms < 120_000
                            for row in batch.observations)) else None
                if batch.revision > 0 and (self.published_batch is None or
                        batch.revision != self.published_batch.revision or
                        delivery is not None or now >= next_publication):
                    next_publication = await self.control._publish(self.http, self.bootstrap(),
                        self.scope, delivery, batch=batch, current_bootstrap=self.bootstrap)
                    self.published_batch = batch
                    if delivery is not None:
                        self._delivery = self._frontier = None
                try:
                    await asyncio.wait_for(self.observer.wait_changed(batch.revision), .5)
                except TimeoutError:
                    pass
        except Exception as error:
            self.failure = error  # The control loop handles authority/transport failure.
        finally:
            self.observer.begin_shutdown()

    def begin_shutdown(self):
        self._stop.set()
        self.observer.begin_shutdown()

    async def _close(self):
        self.begin_shutdown()
        try:
            if self._task is not None:
                await self._task
        finally:
            await self.observer.aclose()

    async def aclose(self):
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close(), name='r4-inventory-join')
        await asyncio.shield(self._close_task)
