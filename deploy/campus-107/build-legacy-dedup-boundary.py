#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq
from ft_shadow_data_plane.central.binance import logical_identity, parse_typed_row
from ft_shadow_data_plane.central.normalize import (
    DEDUP_WINDOW_NS,
    DayNormalizer,
    _Deduplicator,
)
from ft_shadow_data_plane.contracts.models import ContentType
from ft_shadow_data_plane.contracts.serde import atomic_write_bytes, canonical_json_bytes


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the exact end-of-day legacy dedup boundary from the final window"
    )
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--derived-root", type=Path, required=True)
    parser.add_argument("--collector", required=True)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--publish", type=Path)
    args = parser.parse_args()

    normalizer = DayNormalizer(
        raw_root=args.raw_root,
        derived_root=args.derived_root,
        collector_id=args.collector,
        utc_date=args.date,
    )
    day_manifest = normalizer._load_day_manifest()
    manifests = normalizer._load_chunk_manifests(day_manifest)
    day_end_ns = (
        int(datetime.combine(args.date, datetime.min.time(), UTC).timestamp() * 1_000_000_000)
        + 86_400 * 1_000_000_000
    )
    cutoff_ns = day_end_ns - DEDUP_WINDOW_NS
    deduplicator = _Deduplicator()
    raw_rows = 0
    typed_rows = 0

    for manifest in sorted(
        manifests,
        key=lambda item: (item.min_app_receive_realtime_ns, item.chunk_id),
    ):
        if (
            manifest.content_type is not ContentType.PARQUET
            or manifest.max_app_receive_realtime_ns < cutoff_ns
        ):
            continue
        raw_path = normalizer._collector_root / manifest.data_path
        normalizer._verify_chunk(raw_path, manifest)
        parquet = pq.ParquetFile(raw_path)
        normalizer._verify_parquet(parquet, manifest, raw_path)
        for batch in parquet.iter_batches(batch_size=10_000):
            for raw_row in batch.to_pylist():
                observed_ns = int(raw_row["app_receive_realtime_ns"])
                if observed_ns < cutoff_ns:
                    continue
                raw_rows += 1
                typed = parse_typed_row(raw_row)
                if typed is None:
                    continue
                typed_rows += 1
                identity = logical_identity(typed)
                if identity is not None:
                    deduplicator.observe(
                        identity,
                        bytes(typed["payload_hash"]),
                        observed_ns,
                    )

    checkpoint = deduplicator.checkpoint(day_end_ns=day_end_ns)
    checkpoint_bytes = canonical_json_bytes(checkpoint)
    atomic_write_bytes(args.output, checkpoint_bytes)
    if args.publish is not None:
        atomic_write_bytes(args.publish, checkpoint_bytes)
    print(
        f"dedup boundary complete date={args.date} raw_rows={raw_rows} "
        f"typed_rows={typed_rows} entries={len(checkpoint['entries'])} "
        f"output={args.output} publish={args.publish}"
    )


if __name__ == "__main__":
    main()
