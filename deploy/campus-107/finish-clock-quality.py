#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Finish clock quality after normalized typed outputs are atomically complete"
    )
    parser.add_argument("--derived-root", type=Path, required=True)
    parser.add_argument("--collector", required=True)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    output = finish_clock_quality(
        derived_root=args.derived_root,
        collector=args.collector,
        utc_date=args.date,
    )
    print(f"clock quality complete output={output}")


def finish_clock_quality(*, derived_root: Path, collector: str, utc_date: str) -> Path:
    typed_root = derived_root / "typed" / f"collector={collector}" / f"date={utc_date}"
    marker = json.loads((typed_root / "_NORMALIZED.json").read_bytes())
    typed_files = sorted(typed_root.glob("*.typed.parquet"))
    metadata_files = sorted(typed_root.glob(f"{collector}-metadata-*.typed.parquet"))
    if (
        marker.get("collector_id") != collector
        or marker.get("utc_date") != utc_date
        or marker.get("output_files") != len(typed_files)
        or len(marker.get("expected_symbols") or ()) != 60
    ):
        raise ValueError("normalized marker does not match completed typed outputs")
    quality_root = derived_root / "quality" / f"collector={collector}" / f"date={utc_date}"
    gap_path = quality_root / "transport-gaps.jsonl"
    if not gap_path.is_file():
        raise FileNotFoundError(f"transport gap ledger is not complete: {gap_path}")

    samples = []
    columns = (
        "stream_type",
        "exchange_event_time_ms",
        "request_realtime_ns",
        "app_receive_realtime_ns",
    )
    for path in metadata_files:
        for batch in pq.ParquetFile(path).iter_batches(batch_size=10_000, columns=columns):
            selected = batch.filter(pc.equal(batch.column("stream_type"), "clock_sample"))
            for row in selected.to_pylist():
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
    content = b"".join(canonical_json(sample) for sample in samples)
    output = quality_root / "clock-quality.jsonl"
    atomic_write(output, content)
    return output


def canonical_json(value: dict[str, object]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode()


def atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.partial")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as destination:
            destination.write(content)
            destination.flush()
            os.fsync(destination.fileno())
    finally:
        os.close(descriptor)
    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


if __name__ == "__main__":
    main()
