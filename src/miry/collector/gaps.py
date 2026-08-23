from __future__ import annotations

import asyncio
import os
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from uuid import uuid4

from miry.collector.day_index import DayIndex
from miry.contracts.models import (
    ChunkManifest,
    ContentType,
    GapEvent,
    GapReason,
    GapState,
    StreamType,
    WriterGroup,
)
from miry.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    fsync_directory,
    sha256_file,
)


class GapJournal:
    """Publish gap facts without using the market-data queue."""

    def __init__(
        self,
        data_root: Path,
        *,
        collector_id: str,
        data_contract_hash: str,
        universe_hash: str,
        day_index: DayIndex,
        reserve_bytes: int = 1024 * 1024,
    ) -> None:
        self._data_root = data_root
        self._collector_id = collector_id
        self._data_contract_hash = data_contract_hash
        self._universe_hash = universe_hash
        self._day_index = day_index
        self._reserve_path = data_root / "control" / "gap-journal.reserve"
        self._open_root = data_root / "control" / "open-gaps"
        self._reserve_bytes = reserve_bytes

    def initialize(self) -> None:
        self._reserve_path.parent.mkdir(parents=True, exist_ok=True)
        self._open_root.mkdir(parents=True, exist_ok=True)
        if self._reserve_path.exists():
            return
        descriptor = os.open(self._reserve_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            if hasattr(os, "posix_fallocate"):
                os.posix_fallocate(descriptor, 0, self._reserve_bytes)
            else:
                os.ftruncate(descriptor, self._reserve_bytes)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def set_universe_hash(self, value: str) -> None:
        self._universe_hash = value

    async def open(
        self,
        reason: GapReason,
        *,
        connection_id: str | None = None,
        exchange_symbols: tuple[str, ...] = (),
        stream_types: tuple[StreamType, ...] = (),
        affected_from_realtime_ns: int | None = None,
        detail: str = "",
    ) -> str:
        gap_id = f"gap-{uuid4().hex}"
        event = GapEvent(
            gap_id=gap_id,
            state=GapState.OPEN,
            reason=reason,
            collector_id=self._collector_id,
            connection_id=connection_id,
            exchange_symbols=exchange_symbols,
            stream_types=stream_types,
            observed_at_realtime_ns=time_ns(),
            affected_from_realtime_ns=affected_from_realtime_ns,
            detail=detail,
        )
        await self._publish(event)
        await asyncio.to_thread(
            atomic_write_bytes,
            self._open_root / f"{gap_id}.json",
            canonical_json_bytes(event),
        )
        return gap_id

    async def close(
        self,
        gap_id: str,
        reason: GapReason,
        *,
        connection_id: str | None = None,
        exchange_symbols: tuple[str, ...] = (),
        stream_types: tuple[StreamType, ...] = (),
        detail: str = "",
    ) -> None:
        observed_at = time_ns()
        state_path = self._open_root / f"{gap_id}.json"
        if await asyncio.to_thread(state_path.exists):
            closed_at = datetime.fromtimestamp(observed_at // 1_000_000_000, tz=UTC)
            await self._roll_state_to(state_path, closed_at.date())
        await self._publish(
            GapEvent(
                gap_id=gap_id,
                state=GapState.CLOSED,
                reason=reason,
                collector_id=self._collector_id,
                connection_id=connection_id,
                exchange_symbols=exchange_symbols,
                stream_types=stream_types,
                observed_at_realtime_ns=observed_at,
                detail=detail,
            )
        )
        await asyncio.to_thread(self._remove_open_state, state_path)

    def stale_open_events(self) -> tuple[GapEvent, ...]:
        return tuple(
            GapEvent.model_validate_json(path.read_bytes())
            for path in sorted(self._open_root.glob("*.json"))
        )

    async def rollover(self, boundary: datetime) -> None:
        if boundary.tzinfo is None or boundary.utcoffset() != UTC.utcoffset(boundary):
            raise ValueError("gap rollover boundary must be UTC")
        if any((boundary.hour, boundary.minute, boundary.second, boundary.microsecond)):
            raise ValueError("gap rollover boundary must be UTC midnight")
        state_paths = await asyncio.to_thread(lambda: tuple(sorted(self._open_root.glob("*.json"))))
        for state_path in state_paths:
            await self._roll_state_to(state_path, boundary.date())

    async def _publish(self, event: GapEvent) -> None:
        await self._day_index.record_generated(
            _event_date(event),
            lambda: self._write(event),
            recover_io=lambda: self._reserve_path.unlink(missing_ok=True),
        )

    async def _roll_state_to(self, state_path: Path, target_date: date) -> None:
        opened = await asyncio.to_thread(_read_gap_event, state_path)
        opened_ns = opened.affected_from_realtime_ns or opened.observed_at_realtime_ns
        opened_at = datetime.fromtimestamp(opened_ns // 1_000_000_000, tz=UTC)
        current_date = opened_at.date() + timedelta(days=1)
        while current_date <= target_date:
            day_start = datetime.combine(current_date, time.min, UTC)
            previous_day_end_ns = int(day_start.timestamp() * 1_000_000_000) - 1
            previous_date = current_date - timedelta(days=1)
            if not await asyncio.to_thread(self._day_is_sealed, previous_date):
                await self._publish(
                    opened.model_copy(
                        update={
                            "state": GapState.CLOSED,
                            "observed_at_realtime_ns": previous_day_end_ns,
                            "affected_from_realtime_ns": None,
                            "detail": "gap continues into the next UTC day",
                        }
                    )
                )
            opened = opened.model_copy(
                update={
                    "state": GapState.OPEN,
                    "observed_at_realtime_ns": int(day_start.timestamp() * 1_000_000_000),
                    "affected_from_realtime_ns": int(day_start.timestamp() * 1_000_000_000),
                    "detail": "gap continued from a previous UTC day",
                }
            )
            await self._publish(opened)
            await asyncio.to_thread(atomic_write_bytes, state_path, canonical_json_bytes(opened))
            current_date += timedelta(days=1)

    def _write(self, event: GapEvent) -> ChunkManifest:
        effective_ns = _event_effective_ns(event)
        effective = datetime.fromtimestamp(effective_ns // 1_000_000_000, tz=UTC)
        artifact_id = f"{event.gap_id}-{event.observed_at_realtime_ns}-{event.state.value.lower()}"
        relative = (
            Path(f"date={effective.date().isoformat()}")
            / "writer=control"
            / f"{artifact_id}.gap.json"
        )
        destination = self._data_root / "ready" / relative
        content = canonical_json_bytes(event)
        atomic_write_bytes(destination, content)
        manifest = ChunkManifest(
            chunk_id=artifact_id,
            data_path=relative.as_posix(),
            sha256=sha256_file(destination),
            size_bytes=destination.stat().st_size,
            content_type=ContentType.GAP_JSON,
            collector_id=self._collector_id,
            writer_group=WriterGroup.CONTROL,
            utc_date=effective.date(),
            event_count=1,
            min_app_receive_realtime_ns=effective_ns,
            max_app_receive_realtime_ns=effective_ns,
            data_contract_hash=self._data_contract_hash,
            universe_hash=self._universe_hash,
            created_at=datetime.now(UTC),
        )
        atomic_write_bytes(
            destination.with_suffix(".manifest.json"), canonical_json_bytes(manifest)
        )
        return manifest

    def _remove_open_state(self, state_path: Path) -> None:
        state_path.unlink(missing_ok=True)
        fsync_directory(self._open_root)

    def _day_is_sealed(self, utc_date: date) -> bool:
        return (
            self._data_root
            / "ready"
            / "day-manifests"
            / f"date={utc_date.isoformat()}"
            / "SEALED.json"
        ).exists()


def time_ns() -> int:
    import time

    return time.time_ns()


def _read_gap_event(path: Path) -> GapEvent:
    return GapEvent.model_validate_json(path.read_bytes())


def _event_effective_ns(event: GapEvent) -> int:
    if event.state is GapState.OPEN and event.affected_from_realtime_ns is not None:
        return event.affected_from_realtime_ns
    return event.observed_at_realtime_ns


def _event_date(event: GapEvent) -> date:
    return datetime.fromtimestamp(_event_effective_ns(event) // 1_000_000_000, tz=UTC).date()
