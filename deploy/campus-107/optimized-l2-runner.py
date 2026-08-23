#!/usr/bin/env python3
from __future__ import annotations

import argparse
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

    depth_values = (StreamType.DEPTH.value, StreamType.DEPTH_SNAPSHOT.value)

    def vectorized_depth_rows(reconstructor: Any) -> Iterator[dict[str, Any]]:
        yield from iter_symbol_depth_rows(
            sorted(reconstructor._typed_root.glob("*.typed.parquet")),
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
    logging.info("vectorized L2 complete state_changes=%d valid_intervals=%d", changes, intervals)


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
