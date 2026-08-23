from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq

from miry.collector.day_index import DayIndex
from miry.collector.queue import (
    ByteBoundedQueues,
    QueuedEvent,
    RotateWriter,
    StopWriter,
)
from miry.contracts.models import (
    ChunkManifest,
    ContentType,
    RawEvent,
    WriterGroup,
)
from miry.contracts.raw import RAW_EVENT_SCHEMA, raw_events_to_table
from miry.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    fsync_directory,
    sha256_file,
)


@dataclass(frozen=True, slots=True)
class ChunkLimits:
    max_seconds: int
    max_bytes: int
    max_events: int
    batch_events: int
    batch_bytes: int


@dataclass(frozen=True, slots=True)
class WriterMetrics:
    chunks: int
    events: int
    compressed_bytes: int
    max_finalize_seconds: float
    durable_through_ns: int | None


class ChunkSession:
    def __init__(
        self,
        data_root: Path,
        *,
        collector_id: str,
        writer_group: WriterGroup,
        utc_date: date,
        data_contract_hash: str,
        universe_hash: str,
        limits: ChunkLimits,
    ) -> None:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
        self.chunk_id = f"{collector_id}-{writer_group.value}-{timestamp}-{uuid4().hex[:12]}"
        self.data_root = data_root
        self.collector_id = collector_id
        self.writer_group = writer_group
        self.utc_date = utc_date
        self.data_contract_hash = data_contract_hash
        self.universe_hash = universe_hash
        self.limits = limits
        self.started_monotonic = time.monotonic()
        self.event_count = 0
        self.estimated_bytes = 0
        self.min_realtime_ns: int | None = None
        self.max_realtime_ns: int | None = None
        self._batch: list[QueuedEvent] = []
        self._batch_bytes = 0
        writing_dir = data_root / "writing" / writer_group.value
        writing_dir.mkdir(parents=True, exist_ok=True)
        self.partial_path = writing_dir / f"{self.chunk_id}.parquet.partial"
        metadata = {
            b"data_contract_hash": data_contract_hash.encode(),
            b"universe_hash": universe_hash.encode(),
            b"collector_id": collector_id.encode(),
            b"writer_group": writer_group.value.encode(),
            b"utc_date": utc_date.isoformat().encode(),
            b"chunk_id": self.chunk_id.encode(),
        }
        self._parquet = pq.ParquetWriter(
            self.partial_path,
            RAW_EVENT_SCHEMA.with_metadata(metadata),
            compression="zstd",
            compression_level=1,
            use_dictionary=True,
            write_statistics=True,
        )

    def should_rotate_before(self, event: RawEvent, event_date: date) -> bool:
        if event_date != self.utc_date:
            return True
        if self.event_count == 0:
            return False
        return (
            self.estimated_bytes + event.approximate_size_bytes > self.limits.max_bytes
            or self.event_count + 1 > self.limits.max_events
        )

    def add(self, queued: QueuedEvent) -> bool:
        event = queued.event
        self._batch.append(queued)
        self._batch_bytes += queued.reserved_bytes
        self.event_count += 1
        self.estimated_bytes += queued.reserved_bytes
        timestamp = event.app_receive_realtime_ns
        self.min_realtime_ns = (
            timestamp if self.min_realtime_ns is None else min(self.min_realtime_ns, timestamp)
        )
        self.max_realtime_ns = (
            timestamp if self.max_realtime_ns is None else max(self.max_realtime_ns, timestamp)
        )
        return (
            len(self._batch) >= self.limits.batch_events
            or self._batch_bytes >= self.limits.batch_bytes
        )

    def flush(self) -> int:
        if not self._batch:
            return 0
        batch = self._batch
        reserved = self._batch_bytes
        self._batch = []
        self._batch_bytes = 0
        self._parquet.write_table(raw_events_to_table([item.event for item in batch]))
        return reserved

    def elapsed(self) -> float:
        return time.monotonic() - self.started_monotonic

    def finish(self) -> ChunkManifest:
        if self._batch:
            raise RuntimeError("chunk must be flushed before finish")
        if self.event_count == 0 or self.min_realtime_ns is None or self.max_realtime_ns is None:
            raise RuntimeError("cannot finish an empty chunk")
        self._parquet.close()
        with self.partial_path.open("rb") as source:
            os.fsync(source.fileno())

        relative = (
            Path(f"date={self.utc_date.isoformat()}")
            / f"writer={self.writer_group.value}"
            / f"{self.chunk_id}.parquet"
        )
        destination = self.data_root / "ready" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.partial_path.replace(destination)
        fsync_directory(destination.parent)

        manifest = ChunkManifest(
            chunk_id=self.chunk_id,
            data_path=relative.as_posix(),
            sha256=sha256_file(destination),
            size_bytes=destination.stat().st_size,
            content_type=ContentType.PARQUET,
            collector_id=self.collector_id,
            writer_group=self.writer_group,
            utc_date=self.utc_date,
            event_count=self.event_count,
            min_app_receive_realtime_ns=self.min_realtime_ns,
            max_app_receive_realtime_ns=self.max_realtime_ns,
            data_contract_hash=self.data_contract_hash,
            universe_hash=self.universe_hash,
            created_at=datetime.now(UTC),
        )
        manifest_path = destination.with_suffix(".manifest.json")
        atomic_write_bytes(manifest_path, canonical_json_bytes(manifest))
        return manifest


class WriterPool:
    def __init__(
        self,
        data_root: Path,
        *,
        collector_id: str,
        data_contract_hash: str,
        universe_hash: str,
        queues: ByteBoundedQueues,
        day_index: DayIndex,
        limits: ChunkLimits,
    ) -> None:
        self._data_root = data_root
        self._collector_id = collector_id
        self._data_contract_hash = data_contract_hash
        self._universe_hash = universe_hash
        self._queues = queues
        self._day_index = day_index
        self._limits = limits
        self._tasks: list[asyncio.Task[None]] = []
        self._chunks_written = 0
        self._events_written = 0
        self._compressed_bytes = 0
        self._max_finalize_seconds = 0.0
        self._groups = (
            WriterGroup.DEPTH,
            WriterGroup.TRADES_MARKET,
            WriterGroup.METADATA,
        )
        self._durable_through_ns: dict[WriterGroup, int | None] = {
            group: None for group in self._groups
        }

    def start(self) -> None:
        if self._tasks:
            raise RuntimeError("writer pool already started")
        self._tasks = [
            asyncio.create_task(self._run(group), name=f"parquet-writer-{group.value}")
            for group in self._groups
        ]

    @property
    def metrics(self) -> WriterMetrics:
        critical_watermarks = (
            self._durable_through_ns[WriterGroup.DEPTH],
            self._durable_through_ns[WriterGroup.TRADES_MARKET],
        )
        durable_through_ns = (
            min(value for value in critical_watermarks if value is not None)
            if all(value is not None for value in critical_watermarks)
            else None
        )
        return WriterMetrics(
            self._chunks_written,
            self._events_written,
            self._compressed_bytes,
            self._max_finalize_seconds,
            durable_through_ns,
        )

    async def rotate_all(self, *, universe_hash: str) -> None:
        loop = asyncio.get_running_loop()
        completions = [loop.create_future() for _ in self._groups]
        for group, completion in zip(self._groups, completions, strict=True):
            self._queues.put_control(group, RotateWriter(universe_hash, completion))
        await asyncio.gather(*completions)
        self._universe_hash = universe_hash

    async def stop(self) -> None:
        if not self._tasks:
            return
        loop = asyncio.get_running_loop()
        stops = []
        for group, task in zip(self._groups, self._tasks, strict=True):
            if task.done():
                stops.append(task)
                continue
            completion = loop.create_future()
            self._queues.put_control(group, StopWriter(completion))
            stops.append(asyncio.create_task(_wait_for_writer_stop(completion, task)))
        results = await asyncio.gather(*stops, return_exceptions=True)
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        for result in results:
            if isinstance(result, BaseException):
                raise result

    async def wait_for_failure(self) -> None:
        if not self._tasks:
            raise RuntimeError("writer pool is not running")
        done, _ = await asyncio.wait(self._tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in done:
            exception = task.exception()
            if exception is not None:
                raise exception

    async def _run(self, group: WriterGroup) -> None:
        session: ChunkSession | None = None
        universe_hash = self._universe_hash

        async def flush() -> None:
            if session is None:
                return
            released = await asyncio.to_thread(session.flush)
            if released:
                await self._queues.release(released)

        async def finish() -> None:
            nonlocal session
            if session is None:
                return
            await flush()
            started = time.monotonic()
            manifest = await asyncio.to_thread(session.finish)
            elapsed = time.monotonic() - started
            await self._day_index.record(manifest.utc_date, manifest.as_ref())
            self._chunks_written += 1
            self._events_written += manifest.event_count
            self._compressed_bytes += manifest.size_bytes
            self._max_finalize_seconds = max(self._max_finalize_seconds, elapsed)
            current_watermark = self._durable_through_ns[group]
            self._durable_through_ns[group] = max(
                current_watermark or 0, manifest.max_app_receive_realtime_ns
            )
            session = None

        while True:
            try:
                item = await asyncio.wait_for(self._queues.get(group), timeout=1.0)
            except TimeoutError:
                if session is not None and session.elapsed() >= self._limits.max_seconds:
                    await finish()
                continue

            if isinstance(item, QueuedEvent):
                event_date = datetime.fromtimestamp(
                    item.event.app_receive_realtime_ns // 1_000_000_000, tz=UTC
                ).date()
                if session is not None and session.should_rotate_before(item.event, event_date):
                    await finish()
                if session is None:
                    session = await asyncio.to_thread(
                        ChunkSession,
                        self._data_root,
                        collector_id=self._collector_id,
                        writer_group=group,
                        utc_date=event_date,
                        data_contract_hash=self._data_contract_hash,
                        universe_hash=universe_hash,
                        limits=self._limits,
                    )
                if session.add(item):
                    await flush()
                if (
                    session.estimated_bytes >= self._limits.max_bytes
                    or session.event_count >= self._limits.max_events
                    or session.elapsed() >= self._limits.max_seconds
                ):
                    await finish()
                continue

            if isinstance(item, RotateWriter):
                try:
                    await finish()
                    universe_hash = item.universe_hash
                    item.completion.set_result(None)
                except BaseException as exc:
                    item.completion.set_exception(exc)
                    raise
                continue

            if isinstance(item, StopWriter):
                try:
                    await finish()
                    item.completion.set_result(None)
                except BaseException as exc:
                    item.completion.set_exception(exc)
                    raise
                return


async def _wait_for_writer_stop(completion: asyncio.Future[None], task: asyncio.Task[None]) -> None:
    done, _ = await asyncio.wait((completion, task), return_when=asyncio.FIRST_COMPLETED)
    if completion in done:
        await completion
    else:
        await task
