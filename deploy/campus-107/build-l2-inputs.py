#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

L2_COLUMNS = (
    "exchange_symbol",
    "stream_type",
    "connection_id",
    "receive_seq",
    "app_receive_realtime_ns",
    "app_receive_monotonic_ns",
    "exchange_event_time_ms",
    "exchange_transaction_time_ms",
    "payload_hash",
    "is_duplicate",
    "first_update_id",
    "final_update_id",
    "previous_final_update_id",
    "last_update_id",
    "bids",
    "asks",
)
PARTITION_COLUMNS = L2_COLUMNS[1:]
DEPTH_STREAMS = pa.array(("depth", "depth_snapshot"))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Partition normalized depth rows into one ordered Parquet file per symbol"
    )
    parser.add_argument("--derived-root", type=Path, required=True)
    parser.add_argument("--collector", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args()
    pa.set_cpu_count(int(os.environ.get("SLURM_CPUS_PER_TASK", "1")))
    if args.cleanup:
        removed = cleanup_l2_inputs(
            derived_root=args.derived_root,
            collector=args.collector,
            utc_date=args.date,
        )
        print(f"L2 inputs cleanup complete date={args.date} removed={removed}")
        return
    marker = build_l2_inputs(
        derived_root=args.derived_root,
        collector=args.collector,
        utc_date=args.date,
    )
    print(
        f"L2 inputs complete date={args.date} rows={marker['total_rows']} "
        f"bytes={marker['total_bytes']} output={marker['output_root']}"
    )


def build_l2_inputs(*, derived_root: Path, collector: str, utc_date: str) -> dict[str, Any]:
    typed_root = derived_root / "typed" / f"collector={collector}" / f"date={utc_date}"
    normalized_path = typed_root / "_NORMALIZED.json"
    normalized_bytes = normalized_path.read_bytes()
    normalized = json.loads(normalized_bytes)
    symbols = normalized.get("expected_symbols")
    if (
        normalized.get("collector_id") != collector
        or normalized.get("utc_date") != utc_date
        or not isinstance(symbols, list)
        or len(symbols) != 60
        or any(not isinstance(symbol, str) or not symbol for symbol in symbols)
        or len(set(symbols)) != 60
    ):
        raise ValueError(
            f"normalized marker has no authoritative 60-symbol universe: {normalized_path}"
        )

    output_root = derived_root / "l2-inputs" / f"collector={collector}" / f"date={utc_date}"
    source_hash = hashlib.sha256(normalized_bytes).hexdigest()
    existing = load_marker(output_root)
    if existing is not None:
        if existing.get("normalized_sha256") != source_hash:
            raise ValueError(f"L2 input cache source mismatch: {output_root}")
        validate_outputs(output_root, existing, tuple(symbols))
        return existing

    output_root.parent.mkdir(parents=True, exist_ok=True)
    build_root = Path(tempfile.mkdtemp(prefix=f".{output_root.name}.", dir=output_root.parent))
    writers: dict[str, pq.ParquetWriter] = {}
    row_counts = dict.fromkeys(symbols, 0)
    ignored_rows: dict[str, int] = {}
    try:
        for path in sorted(typed_root.glob("*.typed.parquet")):
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(batch_size=100_000, columns=L2_COLUMNS):
                depth_batch = batch.filter(
                    pc.is_in(batch.column("stream_type"), value_set=DEPTH_STREAMS)
                )
                if depth_batch.num_rows == 0:
                    continue
                for symbol, selected in group_by_symbol_preserving_order(depth_batch):
                    if symbol not in row_counts:
                        ignored_rows[symbol] = ignored_rows.get(symbol, 0) + selected.num_rows
                        continue
                    writer = writers.get(symbol)
                    if writer is None:
                        writer = pq.ParquetWriter(
                            build_root / f"symbol={symbol}.parquet",
                            selected.schema,
                            compression="zstd",
                            compression_level=1,
                            use_dictionary=True,
                            write_statistics=False,
                        )
                        writers[symbol] = writer
                    writer.write_batch(selected)
                    row_counts[symbol] += selected.num_rows
        for writer in writers.values():
            writer.close()
        writers.clear()
        missing = [symbol for symbol, count in row_counts.items() if count == 0]
        if missing:
            raise ValueError(f"symbols have no L2 input rows: {','.join(missing)}")
        files = {
            symbol: {
                "rows": row_counts[symbol],
                "size_bytes": (build_root / f"symbol={symbol}.parquet").stat().st_size,
                "sha256": sha256_file(build_root / f"symbol={symbol}.parquet"),
            }
            for symbol in symbols
        }
        marker = {
            "schema_version": 3,
            "layout": "PER_SYMBOL_L2_CAUSAL_V1",
            "persistent_for_downstream": True,
            "collector_id": collector,
            "utc_date": utc_date,
            "normalized_sha256": source_hash,
            "symbols": symbols,
            "files": files,
            "schedule": sorted(symbols, key=lambda symbol: (-row_counts[symbol], symbol)),
            "ignored_rows": ignored_rows,
            "total_rows": sum(row_counts.values()),
            "total_bytes": sum(item["size_bytes"] for item in files.values()),
            "output_root": str(output_root),
        }
        atomic_json(build_root / "_L2_INPUTS.json", marker)
        fsync_tree(build_root)
        try:
            build_root.rename(output_root)
        except FileExistsError:
            existing = load_marker(output_root)
            if existing is None or existing.get("normalized_sha256") != source_hash:
                raise
            validate_outputs(output_root, existing, tuple(symbols))
            return existing
        fsync_directory(output_root.parent)
        return marker
    finally:
        for writer in writers.values():
            writer.close()
        if build_root.exists():
            shutil.rmtree(build_root)


def group_by_symbol_preserving_order(
    batch: pa.RecordBatch,
) -> list[tuple[str, pa.RecordBatch]]:
    with_order = batch.append_column(
        "__row_order", pa.array(range(batch.num_rows), type=pa.int32())
    )
    order = pc.sort_indices(
        with_order,
        sort_keys=(("exchange_symbol", "ascending"), ("__row_order", "ascending")),
    )
    ordered = with_order.take(order)
    encoded = pc.run_end_encode(ordered.column("exchange_symbol"))
    ends = encoded.run_ends.to_pylist()
    symbols = encoded.values.to_pylist()
    starts = [0, *ends[:-1]]
    return [
        (symbol, ordered.slice(start, end - start).select(PARTITION_COLUMNS))
        for symbol, start, end in zip(symbols, starts, ends, strict=True)
    ]


def load_marker(output_root: Path) -> dict[str, Any] | None:
    path = output_root / "_L2_INPUTS.json"
    return json.loads(path.read_bytes()) if path.is_file() else None


def validate_outputs(
    output_root: Path, marker: dict[str, Any], expected_symbols: tuple[str, ...]
) -> None:
    if (
        marker.get("schema_version") != 3
        or marker.get("layout") != "PER_SYMBOL_L2_CAUSAL_V1"
        or marker.get("persistent_for_downstream") is not True
        or tuple(marker.get("symbols") or ()) != expected_symbols
    ):
        raise ValueError(f"L2 input cache universe mismatch: {output_root}")
    files = marker.get("files")
    if not isinstance(files, dict):
        raise ValueError(f"L2 input cache has no file inventory: {output_root}")
    for symbol in expected_symbols:
        path = output_root / f"symbol={symbol}.parquet"
        item = files.get(symbol)
        if (
            not isinstance(item, dict)
            or int(item.get("rows", 0)) <= 0
            or not path.is_file()
            or path.stat().st_size != int(item.get("size_bytes", -1))
            or sha256_file(path) != item.get("sha256")
        ):
            raise ValueError(f"invalid L2 input cache file: {path}")


def cleanup_l2_inputs(*, derived_root: Path, collector: str, utc_date: str) -> bool:
    output_root = derived_root / "l2-inputs" / f"collector={collector}" / f"date={utc_date}"
    marker = load_marker(output_root)
    if marker is None:
        return False
    typed_root = derived_root / "typed" / f"collector={collector}" / f"date={utc_date}"
    normalized_hash = hashlib.sha256((typed_root / "_NORMALIZED.json").read_bytes()).hexdigest()
    symbols = tuple(marker.get("symbols") or ())
    if (
        marker.get("collector_id") != collector
        or marker.get("utc_date") != utc_date
        or marker.get("normalized_sha256") != normalized_hash
        or len(symbols) != 60
    ):
        raise ValueError(f"refusing to remove invalid L2 input cache: {output_root}")
    validate_outputs(output_root, marker, symbols)
    shutil.rmtree(output_root)
    fsync_directory(output_root.parent)
    return True


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    content = (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode()
    temporary = path.with_name(f".{path.name}.partial")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as destination:
            destination.write(content)
            destination.flush()
            os.fsync(destination.fileno())
    finally:
        os.close(descriptor)
    temporary.rename(path)


def fsync_tree(root: Path) -> None:
    for path in root.glob("*.parquet"):
        with path.open("rb") as source:
            os.fsync(source.fileno())
    fsync_directory(root)


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


if __name__ == "__main__":
    main()
