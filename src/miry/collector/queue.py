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

    def __init__(
        self,
        max_bytes: int,
        *,
        warn_ratio: float,
        resume_ratio: float,
    ) -> None:
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
        self._used_bytes_by_group = dict.fromkeys(self._queues, 0)
        self._high_water_bytes = 0
        self._interval_high_water_bytes = 0
        self._warn_crossings = 0
        self._hard_rejections = 0
        self._above_warn = False
        self._condition = asyncio.Condition()

    @property
    def used_bytes(self) -> int:
        return self._used_bytes

    @property
    def utilization(self) -> float:
        return self._used_bytes / self.max_bytes

    @property
    def used_bytes_by_group(self) -> dict[WriterGroup, int]:
        return dict(self._used_bytes_by_group)

    @property
    def high_water_bytes(self) -> int:
        return self._high_water_bytes

    @property
    def warn_crossings(self) -> int:
        return self._warn_crossings

    @property
    def hard_rejections(self) -> int:
        return self._hard_rejections

    def take_interval_high_water_bytes(self) -> int:
        high_water = self._interval_high_water_bytes
        self._interval_high_water_bytes = self._used_bytes
        return high_water

    def idle_seconds(self, group: WriterGroup, *, now: float | None = None) -> float | None:
        last_event = self._last_event_monotonic[group]
        if last_event is None:
            return None
        return max(0.0, (time.monotonic() if now is None else now) - last_event)

    async def put(self, event: RawEvent) -> None:
        self.put_nowait(event)

    def put_nowait(self, event: RawEvent) -> None:
        reserved = event.approximate_size_bytes
        if reserved > self.max_bytes or self._used_bytes + reserved > self.max_bytes:
            self._hard_rejections += 1
            raise QueueOverloaded(
                f"raw queue hard limit: used={self._used_bytes} incoming={reserved} "
                f"max={self.max_bytes}"
            )
        self._used_bytes += reserved
        group = event.writer_group
        self._used_bytes_by_group[group] += reserved
        self._high_water_bytes = max(self._high_water_bytes, self._used_bytes)
        self._interval_high_water_bytes = max(
            self._interval_high_water_bytes, self._used_bytes
        )
        if not self._above_warn and self._used_bytes >= self.warn_bytes:
            self._above_warn = True
            self._warn_crossings += 1
        self._last_event_monotonic[group] = event.app_receive_monotonic_ns / 1_000_000_000
        self._queues[group].put_nowait(QueuedEvent(event, reserved))

    async def get(self, group: WriterGroup) -> WriterItem:
        return await self._queues[group].get()

    def get_nowait(self, group: WriterGroup) -> WriterItem:
        return self._queues[group].get_nowait()

    async def release(self, group: WriterGroup, reserved_bytes: int) -> None:
        async with self._condition:
            self._used_bytes -= reserved_bytes
            self._used_bytes_by_group[group] -= reserved_bytes
            if self._used_bytes < 0 or self._used_bytes_by_group[group] < 0:
                raise RuntimeError("queue byte accounting became negative")
            if self._above_warn and self._used_bytes <= self.resume_bytes:
                self._above_warn = False
            self._condition.notify_all()

    async def wait_until_resumable(self) -> None:
        async with self._condition:
            await self._condition.wait_for(lambda: self._used_bytes <= self.resume_bytes)

    def put_control(self, group: WriterGroup, item: RotateWriter | StopWriter) -> None:
        self._queues[group].put_nowait(item)
