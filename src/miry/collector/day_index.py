from __future__ import annotations

import asyncio
import shutil
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

from pydantic import TypeAdapter

from miry.contracts.models import ChunkManifest, ChunkRef, DayManifest
from miry.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    fsync_directory,
)

CHUNK_REF_ADAPTER = TypeAdapter(ChunkRef)


class DayIndex:
    """Persistent append-only chunk inventory used to seal a UTC day."""

    def __init__(self, data_root: Path, collector_id: str) -> None:
        self._data_root = data_root
        self._collector_id = collector_id
        self._lock = asyncio.Lock()

    async def record(self, utc_date: date, chunk: ChunkRef) -> None:
        async with self._lock:
            if self._sealed_path(utc_date).exists():
                raise RuntimeError(f"cannot add a chunk to sealed UTC day {utc_date}")
            await asyncio.to_thread(self._record, utc_date, chunk)

    async def record_generated(
        self,
        utc_date: date,
        create: Callable[[], ChunkManifest],
        *,
        recover_io: Callable[[], None] | None = None,
    ) -> ChunkManifest:
        async with self._lock:
            if self._sealed_path(utc_date).exists():
                raise RuntimeError(f"cannot publish into sealed UTC day {utc_date}")

            def generate_and_record() -> ChunkManifest:
                try:
                    return self._generate_and_record(utc_date, create)
                except OSError:
                    if recover_io is None:
                        raise
                    recover_io()
                    return self._generate_and_record(utc_date, create)

            return await asyncio.to_thread(generate_and_record)

    def _generate_and_record(
        self, utc_date: date, create: Callable[[], ChunkManifest]
    ) -> ChunkManifest:
        manifest = create()
        if manifest.utc_date != utc_date:
            raise ValueError("generated chunk UTC date mismatch")
        self._record(utc_date, manifest.as_ref())
        return manifest

    async def seal(self, utc_date: date, *, sealed_at: datetime | None = None) -> Path:
        async with self._lock:
            return await asyncio.to_thread(self._seal, utc_date, sealed_at)

    def _record(self, utc_date: date, chunk: ChunkRef) -> None:
        path = self._journal_path(utc_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab", buffering=0) as destination:
            destination.write(canonical_json_bytes(chunk))
            destination.flush()
            import os

            os.fsync(destination.fileno())

    def _seal(self, utc_date: date, sealed_at: datetime | None) -> Path:
        destination = self._sealed_path(utc_date)
        if destination.exists():
            self._clean_sealed_day_state(utc_date)
            return destination
        chunks = self._read_refs(utc_date)
        manifest = DayManifest(
            collector_id=self._collector_id,
            utc_date=utc_date,
            sealed_at=sealed_at or datetime.now(UTC),
            chunks=tuple(chunks),
        )
        atomic_write_bytes(destination, canonical_json_bytes(manifest))
        self._clean_sealed_day_state(utc_date)
        return destination

    def completed_dates(self, today: date) -> tuple[date, ...]:
        values: set[date] = set()
        journal_root = self._data_root / "control" / "day-index"
        for path in journal_root.glob("*.jsonl"):
            try:
                values.add(date.fromisoformat(path.stem))
            except ValueError:
                continue
        for root in (
            self._data_root / "ready",
            self._data_root / "control" / "acked-manifests",
        ):
            for path in root.glob("date=*"):
                try:
                    values.add(date.fromisoformat(path.name.removeprefix("date=")))
                except ValueError:
                    continue
        return tuple(sorted(value for value in values if value < today))

    def _read_refs(self, utc_date: date) -> list[ChunkRef]:
        path = self._journal_path(utc_date)
        if not path.exists():
            return []
        by_identity: dict[tuple[str, str], ChunkRef] = {}
        with path.open("rb") as source:
            for line in source:
                chunk = CHUNK_REF_ADAPTER.validate_json(line)
                by_identity[(chunk.chunk_id, chunk.sha256)] = chunk
        manifest_roots = (
            self._data_root / "ready" / f"date={utc_date.isoformat()}",
            self._data_root / "control" / "acked-manifests" / f"date={utc_date.isoformat()}",
        )
        for root in manifest_roots:
            for manifest_path in root.rglob("*.manifest.json") if root.exists() else ():
                manifest = ChunkManifest.model_validate_json(manifest_path.read_bytes())
                chunk = manifest.as_ref()
                by_identity[(chunk.chunk_id, chunk.sha256)] = chunk
        return [by_identity[key] for key in sorted(by_identity)]

    def _journal_path(self, utc_date: date) -> Path:
        return self._data_root / "control" / "day-index" / f"{utc_date.isoformat()}.jsonl"

    def _sealed_path(self, utc_date: date) -> Path:
        return (
            self._data_root
            / "ready"
            / "day-manifests"
            / f"date={utc_date.isoformat()}"
            / "SEALED.json"
        )

    def _clean_sealed_day_state(self, utc_date: date) -> None:
        journal = self._journal_path(utc_date)
        if journal.exists():
            journal.unlink()
            fsync_directory(journal.parent)
        acked = self._data_root / "control" / "acked-manifests" / f"date={utc_date.isoformat()}"
        if acked.exists():
            shutil.rmtree(acked)
            fsync_directory(acked.parent)
