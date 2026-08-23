from __future__ import annotations

import asyncio

from miry.collector.queue import ByteBoundedQueues
from miry.collector.writer import WriterPool
from miry.contracts.models import RawEvent


class IngestCoordinator:
    """Serialize contract boundaries against new queue admissions."""

    def __init__(self, queues: ByteBoundedQueues, writers: WriterPool) -> None:
        self._queues = queues
        self._writers = writers
        self._boundary_lock = asyncio.Lock()

    async def put(self, event: RawEvent) -> None:
        async with self._boundary_lock:
            await self._queues.put(event)

    async def rotate(self, *, universe_hash: str) -> None:
        async with self._boundary_lock:
            await self._writers.rotate_all(universe_hash=universe_hash)
