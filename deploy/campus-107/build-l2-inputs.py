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

from miry.contracts.l2_projection import (
    L2_PROJECTION_COLUMNS,
    L2_SYMBOL_PROJECTION_SCHEMA_HASH,
    L2_SYMBOL_PROJECTION_SCHEMA_ID,
    content_hash,
    source_file_identity,
    validate_marker,
    validate_shard,
    validate_source_file_identities,
)
from miry.contracts.serde import sha256_file

L2_COLUMNS = ("exchange_symbol", *L2_PROJECTION_COLUMNS)
PARTITION_COLUMNS = L2_COLUMNS[1:]
DEPTH_STREAMS = pa.array(("depth", "depth_snapshot"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Partition normalized depth rows into one ordered Parquet file per symbol"
    )
    parser.add_argument("--derived-root", type=Path, required=True)
    parser.add_argument("--collector", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument(
        "--bootstrap-legacy-identities",
        action="store_true",
        help="one-time v0.5.9 backfill: hash typed files into an immutable sidecar",
    )
    args = parser.parse_args()
    pa.set_cpu_count(int(os.environ.get("SLURM_CPUS_PER_TASK", "1")))
    marker = build_l2_inputs(
        derived_root=args.derived_root,
        collector=args.collector,
        utc_date=args.date,
        bootstrap_legacy_identities=args.bootstrap_legacy_identities,
    )
    print(
        f"L2 inputs complete date={args.date} rows={marker['total_rows']} "
        f"bytes={marker['total_bytes']} output={marker['output_root']}"
    )


def build_l2_inputs(
    *,
    derived_root: Path,
    collector: str,
    utc_date: str,
    bootstrap_legacy_identities: bool = False,
) -> dict[str, Any]:
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
    source_hash = hashlib.sha256(normalized_bytes).hexdigest()
    typed_source_files = load_typed_source_files(
        typed_root=typed_root,
        normalized=normalized,
        normalized_sha256=source_hash,
        collector=collector,
        utc_date=utc_date,
        bootstrap_legacy_identities=bootstrap_legacy_identities,
    )
    data_root = derived_root.parent
    typed_paths = tuple(data_root / str(item["uri"]) for item in typed_source_files)
    if (
        tuple(sorted(typed_root.glob("*.typed.parquet"))) != typed_paths
        or any(
            not path.is_file() or path.stat().st_size != int(item["size_bytes"])
            for path, item in zip(typed_paths, typed_source_files, strict=True)
        )
    ):
        raise ValueError("normalized typed source inventory does not match actual files")

    output_root = (
        derived_root
        / "l2-symbol-projections"
        / f"collector={collector}"
        / f"date={utc_date}"
    )
    existing = load_marker(output_root)
    if existing is not None:
        if existing.get("normalized_sha256") != source_hash:
            raise ValueError(f"L2 input cache source mismatch: {output_root}")
        validate_outputs(
            output_root,
            existing,
            tuple(symbols),
            collector_id=collector,
            utc_date=utc_date,
            normalized_sha256=source_hash,
        )
        return existing

    output_root.parent.mkdir(parents=True, exist_ok=True)
    build_root = Path(tempfile.mkdtemp(prefix=f".{output_root.name}.", dir=output_root.parent))
    writers: dict[str, pq.ParquetWriter] = {}
    row_counts = dict.fromkeys(symbols, 0)
    ignored_rows: dict[str, int] = {}
    try:
        for path in typed_paths:
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
            "schema_id": L2_SYMBOL_PROJECTION_SCHEMA_ID,
            "schema_hash": L2_SYMBOL_PROJECTION_SCHEMA_HASH,
            "layout": "PER_SYMBOL_L2_CAUSAL_V1",
            "canonical_replay": False,
            "data_role": "PERFORMANCE_PROJECTION",
            "retention_policy": "BOUNDED_REGENERABLE",
            "minimum_retention_days": 7,
            "collector_id": collector,
            "utc_date": utc_date,
            "normalized_sha256": source_hash,
            "typed_source_files": typed_source_files,
            "typed_source_file_set_hash": content_hash(typed_source_files),
            "symbols": symbols,
            "files": files,
            "schedule": sorted(symbols, key=lambda symbol: (-row_counts[symbol], symbol)),
            "ignored_rows": ignored_rows,
            "total_rows": sum(row_counts.values()),
            "total_bytes": sum(item["size_bytes"] for item in files.values()),
            "output_root": str(output_root),
        }
        atomic_json(build_root / "_L2_SYMBOL_PROJECTION.json", marker)
        fsync_tree(build_root)
        try:
            build_root.rename(output_root)
        except FileExistsError:
            existing = load_marker(output_root)
            if existing is None or existing.get("normalized_sha256") != source_hash:
                raise
            validate_outputs(
                output_root,
                existing,
                tuple(symbols),
                collector_id=collector,
                utc_date=utc_date,
                normalized_sha256=source_hash,
            )
            return existing
        fsync_directory(output_root.parent)
        return marker
    finally:
        for writer in writers.values():
            writer.close()
        if build_root.exists():
            shutil.rmtree(build_root)


def load_typed_source_files(
    *,
    typed_root: Path,
    normalized: dict[str, Any],
    normalized_sha256: str,
    collector: str,
    utc_date: str,
    bootstrap_legacy_identities: bool,
) -> tuple[dict[str, Any], ...]:
    if "typed_source_files" in normalized or "typed_source_file_set_hash" in normalized:
        return validate_source_file_identities(
            normalized.get("typed_source_files"),
            expected_set_hash=normalized.get("typed_source_file_set_hash"),
        )
    sidecar_path = typed_root / "_TYPED_SOURCE_IDENTITIES.json"
    if sidecar_path.is_file():
        sidecar = json.loads(sidecar_path.read_bytes())
        if (
            set(sidecar)
            != {
                "schema_version",
                "collector_id",
                "utc_date",
                "normalized_sha256",
                "typed_source_files",
                "typed_source_file_set_hash",
            }
            or sidecar.get("schema_version") != 1
            or sidecar.get("collector_id") != collector
            or sidecar.get("utc_date") != utc_date
            or sidecar.get("normalized_sha256") != normalized_sha256
        ):
            raise ValueError("legacy typed source identity sidecar invalid")
        return validate_source_file_identities(
            sidecar.get("typed_source_files"),
            expected_set_hash=sidecar.get("typed_source_file_set_hash"),
        )
    if not bootstrap_legacy_identities:
        raise ValueError(
            "normalized marker lacks typed source identities; "
            "use the explicit legacy backfill workflow"
        )
    identities = tuple(
        source_file_identity(
            path,
            uri=f"derived/typed/collector={collector}/date={utc_date}/{path.name}",
        )
        for path in sorted(typed_root.glob("*.typed.parquet"))
    )
    if not identities:
        raise ValueError("legacy normalized day has no typed Parquet files")
    sidecar = {
        "schema_version": 1,
        "collector_id": collector,
        "utc_date": utc_date,
        "normalized_sha256": normalized_sha256,
        "typed_source_files": identities,
        "typed_source_file_set_hash": content_hash(identities),
    }
    atomic_json(sidecar_path, sidecar)
    fsync_directory(typed_root)
    return validate_source_file_identities(
        json.loads(sidecar_path.read_bytes())["typed_source_files"],
        expected_set_hash=sidecar["typed_source_file_set_hash"],
    )


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
    path = output_root / "_L2_SYMBOL_PROJECTION.json"
    return json.loads(path.read_bytes()) if path.is_file() else None


def validate_outputs(
    output_root: Path,
    marker: dict[str, Any],
    expected_symbols: tuple[str, ...],
    *,
    collector_id: str,
    utc_date: str,
    normalized_sha256: str,
) -> None:
    files = validate_marker(
        marker,
        collector_id=collector_id,
        utc_date=utc_date,
        normalized_sha256=normalized_sha256,
        expected_symbols=expected_symbols,
    )
    typed_sources = validate_source_file_identities(
        marker.get("typed_source_files"),
        expected_set_hash=marker.get("typed_source_file_set_hash"),
    )
    data_root = output_root.parents[2].parent
    for item in typed_sources:
        identity = {key: item.get(key) for key in ("uri", "size_bytes", "content_hash")}
        path = data_root / str(identity["uri"])
        if (
            not path.is_file()
            or path.stat().st_size != int(identity["size_bytes"])
        ):
            raise ValueError(f"L2 input cache typed source file mismatch: {path}")
    for symbol in expected_symbols:
        path = output_root / f"symbol={symbol}.parquet"
        item = files.get(symbol)
        if not isinstance(item, dict) or int(item.get("rows", 0)) <= 0:
            raise ValueError(f"invalid L2 symbol projection file record: {path}")
        validate_shard(path, item)


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
