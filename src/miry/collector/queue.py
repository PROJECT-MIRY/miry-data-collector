from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from miry.contracts.models import RawEvent, WriterGroup


class QueueOverloaded(RuntimeError):
    pass


@dataclass(slots=True)
class QueuedEvent:
    event: RawEvent
    reserved_bytes: int


@dataclass(slots=True)
class RotateWriter:
    universe_hash: str
    completion: asyncio.Future[None]


@dataclass(slots=True)
class StopWriter:
    completion: asyncio.Future[None]


WriterItem = QueuedEvent | RotateWriter | StopWriter


class ByteBoundedQueues:
    """Three unbounded item queues sharing one strict byte budget."""

    def __init__(self, max_bytes: int, *, warn_ratio: float, resume_ratio: float) -> None:
        self.max_bytes = max_bytes
        self.warn_bytes = int(max_bytes * warn_ratio)
        self.resume_bytes = int(max_bytes * resume_ratio)
        self._used_bytes = 0
        self._queues = {
            group: asyncio.Queue[WriterItem]()
            for group in (
                WriterGroup.DEPTH,
                WriterGroup.TRADES_MARKET,
                WriterGroup.METADATA,
            )
        }
        self._last_event_monotonic: dict[WriterGroup, float | None] = {
            group: None for group in self._queues
        }
        self._condition = asyncio.Condition()

    @property
    def used_bytes(self) -> int:
        return self._used_bytes

    @property
    def utilization(self) -> float:
        return self._used_bytes / self.max_bytes

    def idle_seconds(self, group: WriterGroup, *, now: float | None = None) -> float | None:
        last_event = self._last_event_monotonic[group]
        if last_event is None:
            return None
        return max(0.0, (time.monotonic() if now is None else now) - last_event)

    async def put(self, event: RawEvent) -> None:
        reserved = event.approximate_size_bytes
        async with self._condition:
            if reserved > self.max_bytes or self._used_bytes + reserved > self.max_bytes:
                raise QueueOverloaded(
                    f"raw queue hard limit: used={self._used_bytes} incoming={reserved} "
                    f"max={self.max_bytes}"
                )
            self._used_bytes += reserved
            self._last_event_monotonic[event.writer_group] = time.monotonic()
        self._queues[event.writer_group].put_nowait(QueuedEvent(event, reserved))

    async def get(self, group: WriterGroup) -> WriterItem:
        return await self._queues[group].get()

    async def release(self, reserved_bytes: int) -> None:
        async with self._condition:
            self._used_bytes -= reserved_bytes
            if self._used_bytes < 0:
                raise RuntimeError("queue byte accounting became negative")
            self._condition.notify_all()

    async def wait_until_resumable(self) -> None:
        async with self._condition:
            await self._condition.wait_for(lambda: self._used_bytes <= self.resume_bytes)

    def put_control(self, group: WriterGroup, item: RotateWriter | StopWriter) -> None:
        self._queues[group].put_nowait(item)
