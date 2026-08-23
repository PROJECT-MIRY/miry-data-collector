#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import logging
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run L2 reconstruction with vectorized symbol/stream filtering"
    )
    parser.add_argument("--mode", choices=("legacy", "current"), required=True)
    parser.add_argument("--derived-root", type=Path, required=True)
    parser.add_argument("--collector", required=True)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--symbol", required=True)
    args = parser.parse_args()

    if args.mode == "legacy":
        from ft_shadow_data_plane.central import l2 as l2_module
        from ft_shadow_data_plane.contracts.models import StreamType
    else:
        from miry.contracts.models import StreamType
        from miry.pipeline import l2 as l2_module

    install_incremental_bridge(l2_module)
    depth_values = (StreamType.DEPTH.value, StreamType.DEPTH_SNAPSHOT.value)
    input_files, input_mode = l2_input_files(
        derived_root=args.derived_root,
        collector=args.collector,
        utc_date=args.date,
        symbol=args.symbol,
    )

    def vectorized_depth_rows(reconstructor: Any) -> Iterator[dict[str, Any]]:
        yield from iter_symbol_depth_rows(
            input_files,
            symbol=reconstructor._symbol,
            depth_values=depth_values,
        )

    l2_module.L2DayReconstructor._depth_rows = vectorized_depth_rows
    changes, intervals = l2_module.L2DayReconstructor(
        derived_root=args.derived_root,
        collector_id=args.collector,
        utc_date=args.date,
        exchange_symbol=args.symbol,
    ).run()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.info(
        "vectorized L2 complete input_mode=%s state_changes=%d valid_intervals=%d",
        input_mode,
        changes,
        intervals,
    )


def install_incremental_bridge(l2_module: Any) -> None:
    original_on_diff = l2_module.ConnectionBook.on_diff

    def on_diff(book: Any, diff: Any) -> Any:
        anchor = book.anchor_last_update_id
        if book.state is l2_module.L2State.VALID or anchor is None:
            return original_on_diff(book, diff)
        identity = (
            diff.first_update_id,
            diff.final_update_id,
            diff.previous_final_update_id,
            diff.payload_hash,
        )
        if identity in book.seen_diffs:
            return None
        book.seen_diffs.add(identity)
        book.pending.append(diff)
        if not diff.first_update_id <= anchor <= diff.final_update_id:
            return None
        return book._try_bridge()

    l2_module.ConnectionBook.on_diff = on_diff


def l2_input_files(
    *, derived_root: Path, collector: str, utc_date: date, symbol: str
) -> tuple[list[Path], str]:
    typed_root = derived_root / "typed" / f"collector={collector}" / f"date={utc_date}"
    cache_root = derived_root / "l2-inputs" / f"collector={collector}" / f"date={utc_date}"
    marker_path = cache_root / "_L2_INPUTS.json"
    if not marker_path.is_file():
        return sorted(typed_root.glob("*.typed.parquet")), "typed"
    marker = json.loads(marker_path.read_bytes())
    normalized_path = typed_root / "_NORMALIZED.json"
    normalized_hash = hashlib.sha256(normalized_path.read_bytes()).hexdigest()
    item = (marker.get("files") or {}).get(symbol)
    path = cache_root / f"symbol={symbol}.parquet"
    if (
        marker.get("schema_version") != 1
        or marker.get("collector_id") != collector
        or marker.get("utc_date") != utc_date.isoformat()
        or marker.get("normalized_sha256") != normalized_hash
        or symbol not in (marker.get("symbols") or ())
        or not isinstance(item, dict)
        or int(item.get("rows", 0)) <= 0
        or not path.is_file()
        or path.stat().st_size != int(item.get("size_bytes", -1))
    ):
        raise ValueError(f"invalid L2 input cache for {utc_date}: {symbol}")
    return [path], "partitioned"


def iter_symbol_depth_rows(
    files: list[Path], *, symbol: str, depth_values: tuple[str, str]
) -> Iterator[dict[str, Any]]:
    columns = [
        "exchange_symbol",
        "stream_type",
        "connection_id",
        "receive_seq",
        "app_receive_realtime_ns",
        "payload_hash",
        "first_update_id",
        "final_update_id",
        "previous_final_update_id",
        "last_update_id",
        "bids",
        "asks",
    ]
    depth_set = pa.array(depth_values)
    for path in files:
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=10_000, columns=columns):
            symbol_mask = pc.equal(batch.column("exchange_symbol"), symbol)
            stream_mask = pc.is_in(batch.column("stream_type"), value_set=depth_set)
            selected = batch.filter(pc.and_kleene(symbol_mask, stream_mask))
            yield from selected.to_pylist()


if __name__ == "__main__":
    main()
