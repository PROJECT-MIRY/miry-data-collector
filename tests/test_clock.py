from __future__ import annotations

import json
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

from miry.pipeline.clock import build_clock_quality


def test_clock_quality_filters_before_python_conversion(tmp_path) -> None:
    typed_root = tmp_path / "typed/collector=tokyo01/date=2026-08-10"
    typed_root.mkdir(parents=True)
    stream_type = pa.array(
        ["depth", "clock_sample"],
        type=pa.dictionary(pa.int16(), pa.string()),
    )
    table = pa.Table.from_arrays(
        [
            stream_type,
            pa.array([None, 1_000], type=pa.int64()),
            pa.array([None, 999_900_000], type=pa.int64()),
            pa.array([1, 1_000_100_000], type=pa.int64()),
        ],
        names=(
            "stream_type",
            "exchange_event_time_ms",
            "request_realtime_ns",
            "app_receive_realtime_ns",
        ),
    )
    pq.write_table(table, typed_root / "tokyo01-metadata-chunk.typed.parquet")

    output = build_clock_quality(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=date(2026, 8, 10),
    )

    rows = [json.loads(line) for line in output.read_text().splitlines()]
    assert rows == [
        {
            "clock_offset_estimate_ns": 0,
            "clock_offset_uncertainty_ns": 100_000,
            "clock_sample_rtt_ns": 200_000,
            "observed_at_ns": 1_000_100_000,
            "schema_version": 1,
            "status": "VALID",
        }
    ]
