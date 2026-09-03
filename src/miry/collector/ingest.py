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
        self._rotation_lock = asyncio.Lock()
        self._accepting = asyncio.Event()
        self._accepting.set()

    async def put(self, event: RawEvent) -> None:
        if not self._accepting.is_set():
            await self._accepting.wait()
        self._queues.put_nowait(event)

    async def rotate(self, *, universe_hash: str) -> None:
        async with self._rotation_lock:
            self._accepting.clear()
            try:
                await self._writers.rotate_all(universe_hash=universe_hash)
            finally:
                self._accepting.set()
