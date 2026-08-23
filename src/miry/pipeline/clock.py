from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq

from miry.contracts.models import StreamType
from miry.contracts.serde import atomic_write_bytes, canonical_json_bytes


def build_clock_quality(*, derived_root: Path, collector_id: str, utc_date: date) -> Path:
    typed_root = (
        derived_root / "typed" / f"collector={collector_id}" / f"date={utc_date.isoformat()}"
    )
    samples = []
    for row in _clock_rows(typed_root, collector_id=collector_id):
        requested = int(row["request_realtime_ns"])
        received = int(row["app_receive_realtime_ns"])
        server = int(row["exchange_event_time_ms"]) * 1_000_000
        uncertainty = max(0, received - requested) // 2
        offset = server - ((requested + received) // 2)
        bound = abs(offset) + uncertainty
        status = "VALID" if bound <= 100_000_000 else "DEGRADED"
        if bound > 1_000_000_000:
            status = "INVALID"
        samples.append(
            {
                "schema_version": 1,
                "observed_at_ns": received,
                "clock_offset_estimate_ns": offset,
                "clock_offset_uncertainty_ns": uncertainty,
                "clock_sample_rtt_ns": received - requested,
                "status": status,
            }
        )
    destination = (
        derived_root
        / "quality"
        / f"collector={collector_id}"
        / f"date={utc_date.isoformat()}"
        / "clock-quality.jsonl"
    )
    atomic_write_bytes(destination, b"".join(canonical_json_bytes(sample) for sample in samples))
    return destination


def _clock_rows(typed_root: Path, *, collector_id: str) -> Any:
    columns = [
        "stream_type",
        "exchange_event_time_ms",
        "request_realtime_ns",
        "app_receive_realtime_ns",
    ]
    for path in sorted(typed_root.glob(f"{collector_id}-metadata-*.typed.parquet")):
        for batch in pq.ParquetFile(path).iter_batches(batch_size=10_000, columns=columns):
            selected = batch.filter(
                pc.equal(batch.column("stream_type"), StreamType.CLOCK_SAMPLE.value)
            )
            yield from selected.to_pylist()
