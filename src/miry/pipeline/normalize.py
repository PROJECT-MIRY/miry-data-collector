from __future__ import annotations

import hashlib
import heapq
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from miry.contracts.models import (
    ChunkManifest,
    ContentType,
    DayManifest,
    StreamType,
    UniverseDecision,
)
from miry.contracts.raw import RAW_EVENT_SCHEMA
from miry.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    fsync_directory,
    sha256_file,
)
from miry.contracts.typed import TYPED_EVENT_SCHEMA
from miry.pipeline.parsing import logical_identity, parse_typed_row

DEDUP_WINDOW_NS = 600 * 1_000_000_000


@dataclass(frozen=True, slots=True)
class NormalizeResult:
    raw_events: int
    typed_events: int
    duplicate_events: int
    output_files: int


class DayNormalizer:
    def __init__(
        self,
        *,
        raw_root: Path,
        derived_root: Path,
        collector_id: str,
        utc_date: date,
    ) -> None:
        self._collector_root = raw_root / f"collector={collector_id}"
        self._output_root = (
            derived_root / "typed" / f"collector={collector_id}" / f"date={utc_date.isoformat()}"
        )
        self._utc_date = utc_date
        self._collector_id = collector_id
        self._day_start_ns = int(
            datetime.combine(utc_date, datetime.min.time(), UTC).timestamp() * 1_000_000_000
        )
        self._day_end_ns = self._day_start_ns + 86_400 * 1_000_000_000

    def run(self) -> NormalizeResult:
        day_manifest = self._load_day_manifest()
        chunk_manifests = self._load_chunk_manifests(day_manifest)
        deduplicator = _Deduplicator.load(self._previous_dedup_checkpoint())
        formal_starts: list[tuple[int, dict[str, Any], str]] = []
        universe_events: list[tuple[UniverseDecision, str]] = []
        raw_count = 0
        typed_count = 0
        duplicate_count = 0
        output_count = 0

        for manifest in sorted(
            chunk_manifests,
            key=lambda item: (item.min_app_receive_realtime_ns, item.chunk_id),
        ):
            if manifest.content_type is not ContentType.PARQUET:
                continue
            raw_path = self._collector_root / manifest.data_path
            self._verify_chunk(raw_path, manifest)
            output_path = self._output_root / f"{manifest.chunk_id}.typed.parquet"
            rows: list[dict[str, Any]] = []
            parquet = pq.ParquetFile(raw_path)
            self._verify_parquet(parquet, manifest, raw_path)
            for batch in parquet.iter_batches(batch_size=10_000):
                for raw_row in batch.to_pylist():
                    raw_count += 1
                    stream_type = StreamType(str(raw_row["stream_type"]))
                    if stream_type is StreamType.FORMAL_COLLECTION_STARTED:
                        observed_ns = int(raw_row["app_receive_realtime_ns"])
                        formal_starts.append(
                            (
                                observed_ns,
                                _formal_start_payload(bytes(raw_row["payload_bytes"])),
                                manifest.universe_hash,
                            )
                        )
                    elif stream_type is StreamType.UNIVERSE_DECISION:
                        decision = UniverseDecision.model_validate_json(
                            bytes(raw_row["payload_bytes"])
                        )
                        universe_events.append((decision, manifest.universe_hash))
                    typed = parse_typed_row(raw_row)
                    if typed is None:
                        continue
                    identity = logical_identity(typed)
                    if identity is not None:
                        typed["is_duplicate"] = deduplicator.observe(
                            identity,
                            bytes(typed["payload_hash"]),
                            int(typed["app_receive_realtime_ns"]),
                        )
                        if typed["is_duplicate"]:
                            duplicate_count += 1
                    rows.append(typed)
                    typed_count += 1
            if not rows:
                continue
            self._write_typed(output_path, rows)
            output_count += 1

        result = NormalizeResult(raw_count, typed_count, duplicate_count, output_count)
        atomic_write_bytes(
            self._output_root / "_DEDUP_CHECKPOINT.json",
            canonical_json_bytes(deduplicator.checkpoint(day_end_ns=self._day_end_ns)),
        )
        formal_start = min(formal_starts, default=None, key=lambda item: item[0])
        formal_start_ns = formal_start[0] if formal_start is not None else None
        collection_window_start_ns = formal_start_ns or self._day_start_ns
        active_universe = self._active_universe(universe_events, at_ns=collection_window_start_ns)
        if formal_start is not None:
            if active_universe is None:
                raise ValueError("formal start has no active universe decision")
            _validate_formal_start(formal_start, active_universe)
        marker = {
            "schema_version": 1,
            "collector_id": self._collector_id,
            "utc_date": self._utc_date.isoformat(),
            "raw_events": result.raw_events,
            "typed_events": result.typed_events,
            "duplicate_events": result.duplicate_events,
            "output_files": result.output_files,
            "collection_window_start_ns": collection_window_start_ns,
            "collection_window_end_ns": self._day_end_ns,
            "formal_start_realtime_ns": formal_start_ns,
            "formal_start_experiment_id": (
                formal_start[1]["experiment_id"] if formal_start is not None else None
            ),
            "expected_symbols": (
                list(active_universe.members) if active_universe is not None else None
            ),
            "core_generation": (
                active_universe.core_generation if active_universe is not None else None
            ),
            "candidate_revision": (
                active_universe.candidate_revision if active_universe is not None else None
            ),
            "decision_sequence": (
                active_universe.decision_sequence if active_universe is not None else None
            ),
            "universe_version": (
                active_universe.universe_version if active_universe is not None else None
            ),
            "universe_hash": (
                active_universe.universe_hash if active_universe is not None else None
            ),
            "sealed_manifest_sha256": sha256_file(self._day_manifest_path()),
            "dedup_window_ns": DEDUP_WINDOW_NS,
        }
        atomic_write_bytes(self._output_root / "_NORMALIZED.json", canonical_json_bytes(marker))
        return result

    @staticmethod
    def _active_universe(
        events: list[tuple[UniverseDecision, str]], *, at_ns: int
    ) -> UniverseDecision | None:
        candidates = [
            (decision, chunk_universe_hash)
            for decision, chunk_universe_hash in events
            if int(decision.effective_at.timestamp() * 1_000_000_000) <= at_ns
        ]
        if not candidates:
            return None
        decision, chunk_universe_hash = max(
            candidates,
            key=lambda item: (
                item[0].decision_sequence,
                item[0].effective_at,
            ),
        )
        if decision.universe_hash != chunk_universe_hash:
            raise ValueError("active universe event/chunk hash mismatch")
        return decision

    def _load_day_manifest(self) -> DayManifest:
        path = self._day_manifest_path()
        if not path.exists():
            raise FileNotFoundError(f"sealed day manifest does not exist: {path}")
        manifest = DayManifest.model_validate_json(path.read_bytes())
        if manifest.collector_id != self._collector_id or manifest.utc_date != self._utc_date:
            raise ValueError("sealed day manifest identity mismatch")
        return manifest

    def _day_manifest_path(self) -> Path:
        return (
            self._collector_root
            / "day-manifests"
            / f"date={self._utc_date.isoformat()}"
            / "SEALED.json"
        )

    def _previous_dedup_checkpoint(self) -> Path:
        previous_date = self._utc_date - timedelta(days=1)
        return (
            self._output_root.parent
            / f"date={previous_date.isoformat()}"
            / "_DEDUP_CHECKPOINT.json"
        )

    def _load_chunk_manifests(self, day_manifest: DayManifest) -> list[ChunkManifest]:
        manifests = []
        for chunk in day_manifest.chunks:
            data_path = self._collector_root / chunk.data_path
            manifest_path = data_path.with_suffix(".manifest.json")
            manifest = ChunkManifest.model_validate_json(manifest_path.read_bytes())
            if manifest.as_ref() != chunk:
                raise ValueError(f"day/chunk manifest disagreement: {manifest_path}")
            if manifest.collector_id != self._collector_id:
                raise ValueError(f"chunk collector mismatch: {manifest_path}")
            if manifest.utc_date != self._utc_date:
                raise ValueError(f"chunk UTC date mismatch: {manifest_path}")
            expected_parent = PurePosixPath(
                f"date={self._utc_date.isoformat()}",
                f"writer={manifest.writer_group.value}",
            )
            if PurePosixPath(manifest.data_path).parent != expected_parent:
                raise ValueError(f"chunk path/writer mismatch: {manifest_path}")
            manifests.append(manifest)
        return manifests

    @staticmethod
    def _verify_chunk(path: Path, manifest: ChunkManifest) -> None:
        if path.stat().st_size != manifest.size_bytes or sha256_file(path) != manifest.sha256:
            raise ValueError(f"raw chunk integrity failure: {path}")

    def _verify_parquet(
        self, parquet: pq.ParquetFile, manifest: ChunkManifest, path: Path
    ) -> None:
        if not parquet.schema_arrow.remove_metadata().equals(RAW_EVENT_SCHEMA):
            raise ValueError(f"raw schema mismatch: {path}")
        expected = {
            b"chunk_id": manifest.chunk_id.encode(),
            b"collector_id": manifest.collector_id.encode(),
            b"data_contract_hash": manifest.data_contract_hash.encode(),
            b"universe_hash": manifest.universe_hash.encode(),
            b"utc_date": manifest.utc_date.isoformat().encode(),
            b"writer_group": manifest.writer_group.value.encode(),
        }
        actual = parquet.schema_arrow.metadata or {}
        mismatched = [
            key.decode()
            for key, expected_value in expected.items()
            if actual.get(key) != expected_value
        ]
        if mismatched:
            raise ValueError(f"raw metadata mismatch ({','.join(mismatched)}): {path}")
        file_metadata = parquet.metadata
        if file_metadata.num_rows != manifest.event_count:
            raise ValueError(f"raw event count mismatch: {path}")
        column_index = parquet.schema_arrow.get_field_index("app_receive_realtime_ns")
        statistics = [
            file_metadata.row_group(index).column(column_index).statistics
            for index in range(file_metadata.num_row_groups)
        ]
        if not statistics or any(item is None or not item.has_min_max for item in statistics):
            raise ValueError(f"raw receive time statistics unavailable: {path}")
        min_realtime_ns = min(int(item.min) for item in statistics if item is not None)
        max_realtime_ns = max(int(item.max) for item in statistics if item is not None)
        if (
            min_realtime_ns != manifest.min_app_receive_realtime_ns
            or max_realtime_ns != manifest.max_app_receive_realtime_ns
        ):
            raise ValueError(f"raw receive time range mismatch: {path}")
        if not (self._day_start_ns <= min_realtime_ns <= max_realtime_ns < self._day_end_ns):
            raise ValueError(f"raw receive time range falls outside UTC date: {path}")

    @staticmethod
    def _write_typed(path: Path, rows: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(f".{path.name}.partial")
        table = pa.Table.from_pylist(rows, schema=TYPED_EVENT_SCHEMA)
        pq.write_table(table, partial, compression="zstd", compression_level=3)
        with partial.open("rb") as source:
            os.fsync(source.fileno())
        partial.replace(path)
        fsync_directory(path.parent)


class _Deduplicator:
    def __init__(self) -> None:
        self._seen: dict[str, tuple[bytes, int]] = {}
        self._timeline: list[tuple[int, str]] = []
        self._watermark_ns = 0

    @classmethod
    def load(cls, path: Path) -> _Deduplicator:
        deduplicator = cls()
        if not path.exists():
            return deduplicator
        value = orjson.loads(path.read_bytes())
        if value.get("schema_version") != 1 or value.get("window_ns") != DEDUP_WINDOW_NS:
            raise ValueError("incompatible dedup checkpoint")
        for item in value.get("entries", []):
            key = str(item["identity_key"])
            payload_hash = bytes.fromhex(str(item["payload_hash"]))
            observed_ns = int(item["observed_ns"])
            deduplicator._seen[key] = (payload_hash, observed_ns)
            heapq.heappush(deduplicator._timeline, (observed_ns, key))
            deduplicator._watermark_ns = max(deduplicator._watermark_ns, observed_ns)
        return deduplicator

    def observe(self, identity: tuple[object, ...], payload_hash: bytes, observed_ns: int) -> bool:
        self._watermark_ns = max(self._watermark_ns, observed_ns)
        self._prune(self._watermark_ns - DEDUP_WINDOW_NS)
        key = _identity_key(identity)
        previous = self._seen.get(key)
        if previous is not None and previous[0] != payload_hash:
            raise ValueError(f"conflicting payload for logical event identity {key}")
        duplicate = previous is not None
        self._seen[key] = (payload_hash, observed_ns)
        heapq.heappush(self._timeline, (observed_ns, key))
        return duplicate

    def checkpoint(self, *, day_end_ns: int) -> dict[str, Any]:
        cutoff = day_end_ns - DEDUP_WINDOW_NS
        entries = [
            {
                "identity_key": key,
                "payload_hash": payload_hash.hex(),
                "observed_ns": observed_ns,
            }
            for key, (payload_hash, observed_ns) in sorted(self._seen.items())
            if observed_ns >= cutoff
        ]
        return {"schema_version": 1, "window_ns": DEDUP_WINDOW_NS, "entries": entries}

    def _prune(self, cutoff_ns: int) -> None:
        while self._timeline and self._timeline[0][0] < cutoff_ns:
            observed_ns, key = heapq.heappop(self._timeline)
            current = self._seen.get(key)
            if current is not None and current[1] == observed_ns:
                del self._seen[key]


def _identity_key(identity: tuple[object, ...]) -> str:
    normalized = [
        {"bytes": value.hex()} if isinstance(value, bytes) else str(value) for value in identity
    ]
    return hashlib.sha256(orjson.dumps(normalized)).hexdigest()


def _formal_start_payload(raw: bytes) -> dict[str, Any]:
    value = orjson.loads(raw)
    if not isinstance(value, dict) or value.get("event") != "FORMAL_COLLECTION_STARTED":
        raise ValueError("invalid formal collection start event")
    if not isinstance(value.get("experiment_id"), str) or not value["experiment_id"]:
        raise ValueError("formal start has no experiment ID")
    integer_fields = ("core_generation", "candidate_revision", "decision_sequence")
    if any(
        not isinstance(value.get(field), int) or isinstance(value[field], bool)
        for field in integer_fields
    ):
        raise ValueError("formal start has invalid structured universe version fields")
    if (
        value["core_generation"] < 1
        or value["candidate_revision"] < 0
        or value["decision_sequence"] < 1
        or value.get("universe_version")
        != f'{value["core_generation"]}.{value["candidate_revision"]}'
    ):
        raise ValueError("formal start has an invalid structured universe version")
    if not isinstance(value.get("universe_hash"), str):
        raise ValueError("formal start has no universe hash")
    try:
        started_at = datetime.fromisoformat(str(value["started_at"]))
    except (KeyError, ValueError) as exc:
        raise ValueError("formal start has an invalid started_at") from exc
    if started_at.tzinfo is None or started_at.utcoffset() != UTC.utcoffset(started_at):
        raise ValueError("formal start started_at must be UTC")
    return value


def _validate_formal_start(
    evidence: tuple[int, dict[str, Any], str], active: UniverseDecision
) -> None:
    _observed_ns, payload, chunk_universe_hash = evidence
    version_matches = all(
        (
            payload.get("core_generation") == active.core_generation,
            payload.get("candidate_revision") == active.candidate_revision,
            payload.get("decision_sequence") == active.decision_sequence,
            payload.get("universe_version") == active.universe_version,
        )
    )
    if not version_matches or (
        payload["universe_hash"] != active.universe_hash
        or chunk_universe_hash != active.universe_hash
    ):
        raise ValueError("formal start/universe identity mismatch")
