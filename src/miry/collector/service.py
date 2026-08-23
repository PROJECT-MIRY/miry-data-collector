from __future__ import annotations

import asyncio
import logging
import os
import resource
import shutil
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pyarrow as pa

from miry.collector.config import CollectorConfig
from miry.collector.day_index import DayIndex
from miry.collector.gaps import GapJournal
from miry.collector.ingest import IngestCoordinator
from miry.collector.lease import CollectorLease
from miry.collector.membership import UniverseStore
from miry.collector.queue import ByteBoundedQueues
from miry.collector.sources import SourceManager
from miry.collector.spool import AckApplyResult, SpoolManager
from miry.collector.websocket import SourceIdentity
from miry.collector.writer import ChunkLimits, WriterPool
from miry.contracts.collection import data_contract_hash
from miry.contracts.models import (
    GapEvent,
    GapReason,
    StreamType,
    WriterGroup,
)
from miry.contracts.serde import atomic_write_bytes, canonical_json_bytes
from miry.universe.models import DiscoverySnapshot

logger = logging.getLogger(__name__)


class CollectorService:
    def __init__(self, config: CollectorConfig) -> None:
        self._config = config
        self._boot_id = uuid4().hex
        self._operation_lock = asyncio.Lock()
        self._stop = asyncio.Event()

        self._spool = SpoolManager(
            config.data_root,
            max_bytes=config.spool_max_bytes,
            minimum_free_bytes=config.minimum_free_bytes,
        )
        self._universe_store = UniverseStore(config.data_root, config.universe)
        active = self._universe_store.initialize()
        active = self._universe_store.apply_due() or active
        contract_hash = data_contract_hash(
            d0_enabled=config.d0_enabled,
            open_interest_interval_seconds=config.open_interest_interval_seconds,
        )
        self._day_index = DayIndex(config.data_root, config.collector_id)
        self._queues = ByteBoundedQueues(
            config.queue_max_bytes,
            warn_ratio=config.queue_warn_ratio,
            resume_ratio=config.queue_resume_ratio,
        )
        self._writers = WriterPool(
            config.data_root,
            collector_id=config.collector_id,
            data_contract_hash=contract_hash,
            universe_hash=active.universe_hash,
            queues=self._queues,
            day_index=self._day_index,
            limits=ChunkLimits(
                max_seconds=config.chunk_max_seconds,
                max_bytes=config.chunk_max_bytes,
                max_events=config.chunk_max_events,
                batch_events=config.writer_batch_events,
                batch_bytes=config.writer_batch_bytes,
            ),
        )
        self._ingest = IngestCoordinator(self._queues, self._writers)
        self._gaps = GapJournal(
            config.data_root,
            collector_id=config.collector_id,
            data_contract_hash=contract_hash,
            universe_hash=active.universe_hash,
            day_index=self._day_index,
        )
        self._sources = SourceManager(
            config,
            collector_id=config.collector_id,
            boot_id=self._boot_id,
            ingest=self._ingest,
            queues=self._queues,
            gaps=self._gaps,
            on_discovery=self._on_discovery,
        )
        self._control_identity = SourceIdentity(
            config.collector_id,
            self._boot_id,
            uuid4().hex,
            f"collector-control-{uuid4().hex}",
        )
        self._formal_start_path = config.data_root / "control" / "formal-start.json"
        self._service_started_ns = time.time_ns()
        self._lease = CollectorLease(config.data_root, self._boot_id)
        self._storage_gap_id: str | None = None
        self._stale_gaps: tuple[GapEvent, ...] = ()
        self._previous_cpu = _host_cpu_sample()
        self._previous_written_bytes = 0

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        self._spool.initialize()
        initial_acks = await asyncio.to_thread(self._spool.apply_acks)
        self._log_ack_result(initial_acks)
        self._gaps.initialize()
        recovery_start_ns = await asyncio.to_thread(self._lease.recovery_start_ns)
        if recovery_start_ns is not None:
            await self._gaps.open(
                GapReason.COLLECTOR_STOPPED,
                exchange_symbols=self._universe_store.active.members,
                affected_from_realtime_ns=recovery_start_ns,
                detail="previous collector boot ended without a clean shutdown",
            )
        self._stale_gaps = self._gaps.stale_open_events()
        await asyncio.to_thread(self._lease.write_running, self._lease_watermark_ns())
        for gap in self._stale_gaps:
            if gap.reason is GapReason.STORAGE_EXHAUSTED:
                self._storage_gap_id = gap.gap_id
                break
        self._quarantine_incomplete_chunks()
        self._writers.start()
        try:
            if not (await asyncio.to_thread(self._spool.status)).hard_limited:
                await self._sources.start(self._universe_store.active.members)
                await self._wait_source_readiness()
                await self._close_stale_gaps()
                await self._seal_completed_days()
                await self._mark_formal_start()
        except BaseException:
            await self._sources.stop()
            await self._writers.stop()
            raise

        background = [
            asyncio.create_task(self._storage_loop(), name="storage-monitor"),
            asyncio.create_task(self._midnight_loop(), name="utc-midnight-rotation"),
            asyncio.create_task(self._stats_loop(), name="collector-stats"),
            asyncio.create_task(self._writers.wait_for_failure(), name="writer-health"),
            asyncio.create_task(self._lease_loop(), name="collector-lease"),
        ]
        stop_task = asyncio.create_task(self._stop.wait(), name="collector-stop")
        done, _ = await asyncio.wait((*background, stop_task), return_when=asyncio.FIRST_COMPLETED)
        failure: BaseException | None = None
        for task in done:
            if task is stop_task or task.cancelled():
                continue
            failure = task.exception()
            if failure is not None:
                logger.exception("collector background task failed", exc_info=failure)
                break
        self._stop.set()
        for task in background:
            if not task.done():
                task.cancel()
        if self._sources.running:
            await self._gaps.open(
                GapReason.COLLECTOR_STOPPED,
                exchange_symbols=self._universe_store.active.members,
                detail="collector service stopped; closes after the next full source readiness",
            )
        await self._sources.stop()
        await asyncio.gather(*background, return_exceptions=True)
        await self._writers.stop()
        await asyncio.to_thread(self._lease.write_clean, self._lease_watermark_ns())
        if failure is not None:
            raise failure

    async def _storage_loop(self) -> None:
        while not self._stop.is_set():
            async with self._operation_lock:
                self._sources.raise_if_failed()
                ack_result = await asyncio.to_thread(self._spool.apply_acks)
                status = await asyncio.to_thread(self._spool.status)
                self._log_ack_result(ack_result)
                if status.hard_limited:
                    if self._storage_gap_id is None:
                        self._storage_gap_id = await self._gaps.open(
                            GapReason.STORAGE_EXHAUSTED,
                            exchange_symbols=self._universe_store.active.members,
                            detail=(
                                f"spool_bytes={status.used_bytes} "
                                f"free_bytes={status.free_bytes}"
                            ),
                        )
                    if self._sources.running:
                        await self._sources.stop()
                elif self._storage_gap_id is not None:
                    recovery_ready = True
                    if not self._sources.running:
                        try:
                            await self._sources.start(self._universe_store.active.members)
                            await self._wait_source_readiness()
                        except TimeoutError as exc:
                            logger.warning(
                                "storage recovery source readiness failed; will retry error=%r",
                                exc,
                            )
                            await self._sources.stop()
                            recovery_ready = False
                        else:
                            await self._close_stale_gaps()
                            await self._seal_completed_days()
                            await self._mark_formal_start()
                    if recovery_ready:
                        await self._gaps.close(
                            self._storage_gap_id,
                            GapReason.STORAGE_EXHAUSTED,
                            exchange_symbols=self._universe_store.active.members,
                        )
                        self._storage_gap_id = None
            await _wait_event(self._stop, self._config.storage_check_seconds)

    @staticmethod
    def _log_ack_result(result: AckApplyResult) -> None:
        if not (
            result.seen
            or result.applied
            or result.replayed
            or result.invalid
            or result.hash_mismatches
            or result.unknown
            or result.manifest_errors
        ):
            return
        level = (
            logging.ERROR
            if result.invalid
            or result.hash_mismatches
            or result.unknown
            or result.manifest_errors
            else logging.INFO
        )
        logger.log(
            level,
            "ack apply complete seen=%d applied=%d replayed=%d gc_bytes=%d invalid=%d "
            "hash_mismatches=%d unknown=%d manifest_errors=%d recovered=%d",
            result.seen,
            result.applied,
            result.replayed,
            result.gc_bytes,
            result.invalid,
            result.hash_mismatches,
            result.unknown,
            result.manifest_errors,
            result.recovered,
        )

    async def _midnight_loop(self) -> None:
        while not self._stop.is_set():
            now = datetime.now(UTC)
            midnight = datetime.combine(now.date() + timedelta(days=1), datetime.min.time(), UTC)
            await _wait_event(self._stop, max(0.0, (midnight - now).total_seconds()))
            if self._stop.is_set():
                return
            async with self._operation_lock:
                await self._apply_midnight_boundary(midnight)

    async def _lease_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.to_thread(self._lease.write_running, self._lease_watermark_ns())
            await _wait_event(self._stop, self._config.lease_heartbeat_seconds)

    async def _apply_midnight_boundary(self, midnight: datetime) -> None:
        previous = self._universe_store.active
        was_running = self._sources.running
        boundary_gap: str | None = None
        await _sleep_until(midnight)
        previous_date = midnight.date() - timedelta(days=1)
        await self._gaps.rollover(midnight)
        decision = await asyncio.to_thread(self._universe_store.apply_due, midnight)
        if decision is not None:
            changed = tuple(sorted(set(previous.members) ^ set(decision.members)))
            if was_running and changed:
                boundary_gap = await self._gaps.open(
                    GapReason.PLANNED_BOUNDARY,
                    exchange_symbols=changed,
                    affected_from_realtime_ns=int(midnight.timestamp() * 1_000_000_000),
                    detail="live subscriptions changing at a formal universe boundary",
                )
        active = decision or previous
        # The writer barrier makes the sealed day inventory complete without
        # interrupting any source when membership is unchanged.
        await self._ingest.rotate(universe_hash=active.universe_hash)
        if decision is not None:
            self._gaps.set_universe_hash(active.universe_hash)
            if was_running:
                await self._sources.update_instruments(
                    active.members,
                    rebalance_public=True,
                )
            if boundary_gap is not None:
                await self._gaps.close(
                    boundary_gap,
                    GapReason.PLANNED_BOUNDARY,
                    exchange_symbols=changed,
                    detail="new subscriptions, L2 snapshots, and first OI samples are ready",
                )
        elif was_running:
            await self._sources.rebalance_public_routes()
        await self._emit_control_event(StreamType.UNIVERSE_DECISION, canonical_json_bytes(active))
        await self._ingest.rotate(universe_hash=active.universe_hash)
        await _wait_event(self._stop, self._config.day_seal_grace_seconds)
        await self._day_index.seal(previous_date, sealed_at=datetime.now(UTC))
        self._lease.record_sealed(previous_date)
        await asyncio.to_thread(self._lease.write_running, self._lease_watermark_ns())
        logger.info(
            "sealed UTC day date=%s universe_changed=%s",
            previous_date,
            decision is not None,
        )

    async def _on_discovery(self, snapshot: DiscoverySnapshot) -> None:
        async with self._operation_lock:
            decision = await asyncio.to_thread(self._universe_store.observe_and_plan, snapshot)
            if decision is not None:
                await self._emit_control_event(
                    StreamType.UNIVERSE_DECISION,
                    canonical_json_bytes(decision),
                )
                logger.info(
                    "planned universe version=%s decision_sequence=%d effective_at=%s changed=%d",
                    decision.universe_version,
                    decision.decision_sequence,
                    decision.effective_at,
                    len(set(self._universe_store.active.members) ^ set(decision.members)),
                )

    async def _wait_source_readiness(self) -> None:
        await self._sources.wait_ready()
        if not self._formal_start_path.exists():
            await self._sources.wait_discovery_ready()

    async def _mark_formal_start(self) -> None:
        active = self._universe_store.active
        await self._emit_control_event(StreamType.UNIVERSE_DECISION, canonical_json_bytes(active))
        if self._formal_start_path.exists():
            await self._ingest.rotate(universe_hash=active.universe_hash)
            return
        started_at = datetime.now(UTC)
        payload = {
            "event": "FORMAL_COLLECTION_STARTED",
            "experiment_id": self._config.universe.experiment_id,
            "core_generation": active.core_generation,
            "candidate_revision": active.candidate_revision,
            "decision_sequence": active.decision_sequence,
            "universe_version": active.universe_version,
            "started_at": started_at.isoformat(),
            "universe_hash": active.universe_hash,
        }
        await self._emit_control_event(
            StreamType.FORMAL_COLLECTION_STARTED,
            canonical_json_bytes(payload),
        )
        await self._ingest.rotate(universe_hash=active.universe_hash)
        await asyncio.to_thread(
            atomic_write_bytes, self._formal_start_path, canonical_json_bytes(payload)
        )
        logger.info(
            "FORMAL_COLLECTION_STARTED experiment_id=%s universe_version=%s "
            "decision_sequence=%d symbols=60",
            self._config.universe.experiment_id,
            active.universe_version,
            active.decision_sequence,
        )

    async def _emit_control_event(self, stream_type: StreamType, payload: bytes) -> None:
        event = self._control_identity.event(
            stream_type=stream_type,
            exchange_symbol=None,
            payload=payload,
            realtime_ns=time.time_ns(),
            monotonic_ns=time.monotonic_ns(),
        )
        await self._ingest.put(event)

    async def _stats_loop(self) -> None:
        loop = asyncio.get_running_loop()
        previous_tick = loop.time()
        while not self._stop.is_set():
            status = await asyncio.to_thread(self._spool.status)
            usage = resource.getrusage(resource.RUSAGE_SELF)
            current_cpu = _host_cpu_sample()
            steal_ratio = _steal_ratio(self._previous_cpu, current_cpu)
            self._previous_cpu = current_cpu
            writer = self._writers.metrics
            now = loop.time()
            elapsed = max(now - previous_tick, 0.001)
            write_bytes_per_second = (
                writer.compressed_bytes - self._previous_written_bytes
            ) / elapsed
            writer_idle = {
                group: self._queues.idle_seconds(group, now=now)
                for group in (
                    WriterGroup.DEPTH,
                    WriterGroup.TRADES_MARKET,
                    WriterGroup.METADATA,
                )
            }
            event_loop_lag = max(0.0, elapsed - 60.0) if previous_tick else 0.0
            self._previous_written_bytes = writer.compressed_bytes
            previous_tick = now
            logger.info(
                "collector status queue_bytes=%d queue_ratio=%.3f spool_bytes=%d "
                "free_bytes=%d rss_bytes=%d arrow_bytes=%d cpu_user_s=%.3f cpu_system_s=%.3f "
                "cpu_steal_ratio=%.4f event_loop_lag_s=%.6f chunks=%d events=%d "
                "compressed_bytes=%d write_bytes_s=%.1f max_finalize_s=%.6f "
                "depth_idle_s=%.3f trades_market_idle_s=%.3f metadata_idle_s=%.3f",
                self._queues.used_bytes,
                self._queues.utilization,
                status.used_bytes,
                status.free_bytes,
                _current_rss_bytes(),
                pa.total_allocated_bytes(),
                usage.ru_utime,
                usage.ru_stime,
                steal_ratio,
                event_loop_lag,
                writer.chunks,
                writer.events,
                writer.compressed_bytes,
                write_bytes_per_second,
                writer.max_finalize_seconds,
                _idle_metric(writer_idle[WriterGroup.DEPTH]),
                _idle_metric(writer_idle[WriterGroup.TRADES_MARKET]),
                _idle_metric(writer_idle[WriterGroup.METADATA]),
            )
            await _wait_event(self._stop, 60)

    def _quarantine_incomplete_chunks(self) -> None:
        writing_root = self._config.data_root / "writing"
        partials = list(writing_root.rglob("*.partial")) if writing_root.exists() else []
        if not partials:
            return
        quarantine = self._config.data_root / "control" / "quarantine" / self._boot_id
        quarantine.mkdir(parents=True, exist_ok=True)
        for path in partials:
            shutil.move(path, quarantine / path.name)
        logger.error("quarantined incomplete chunks count=%d", len(partials))

    async def _close_stale_gaps(self) -> None:
        stale = self._stale_gaps
        self._stale_gaps = ()
        for gap in stale:
            if gap.gap_id == self._storage_gap_id:
                continue
            await self._gaps.close(
                gap.gap_id,
                gap.reason,
                connection_id=gap.connection_id,
                exchange_symbols=gap.exchange_symbols,
                stream_types=gap.stream_types,
                detail="new collector boot reached realtime source readiness",
            )

    async def _seal_completed_days(self) -> None:
        now = datetime.now(UTC)
        completed = await asyncio.to_thread(self._day_index.completed_dates, now.date())
        for utc_date in completed:
            await self._day_index.seal(utc_date, sealed_at=now)
            self._lease.record_sealed(utc_date)

    def _lease_watermark_ns(self) -> int:
        return self._writers.metrics.durable_through_ns or self._service_started_ns


async def _sleep_until(moment: datetime) -> None:
    delay = (moment - datetime.now(UTC)).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay)


async def _wait_event(event: asyncio.Event, delay_seconds: float) -> None:
    try:
        await asyncio.wait_for(event.wait(), timeout=delay_seconds)
    except TimeoutError:
        pass


def _current_rss_bytes() -> int:
    try:
        fields = Path("/proc/self/statm").read_text().split()
        return int(fields[1]) * os.sysconf("SC_PAGE_SIZE")
    except (FileNotFoundError, IndexError, ValueError):
        return 0


def _idle_metric(value: float | None) -> float:
    return -1.0 if value is None else value


def _host_cpu_sample() -> tuple[int, int]:
    try:
        fields = Path("/proc/stat").read_text().splitlines()[0].split()[1:]
        values = [int(value) for value in fields]
    except (FileNotFoundError, IndexError, ValueError):
        return 0, 0
    total = sum(values)
    steal = values[7] if len(values) > 7 else 0
    return total, steal


def _steal_ratio(previous: tuple[int, int], current: tuple[int, int]) -> float:
    total = current[0] - previous[0]
    steal = current[1] - previous[1]
    return max(0.0, steal / total) if total > 0 else 0.0
