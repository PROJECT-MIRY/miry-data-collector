#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import orjson
from ft_shadow_data_plane.central.clock_quality import build_clock_quality
from ft_shadow_data_plane.central.gap_ledger import build_transport_gap_ledger


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Finish legacy gap and clock outputs after an atomic normalize marker exists"
    )
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--derived-root", type=Path, required=True)
    parser.add_argument("--collector", required=True)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    args = parser.parse_args()

    typed_root = (
        args.derived_root
        / "typed"
        / f"collector={args.collector}"
        / f"date={args.date.isoformat()}"
    )
    marker_path = typed_root / "_NORMALIZED.json"
    marker = orjson.loads(marker_path.read_bytes())
    typed_files = tuple(typed_root.glob("*.typed.parquet"))
    if (
        marker.get("collector_id") != args.collector
        or marker.get("utc_date") != args.date.isoformat()
        or marker.get("output_files") != len(typed_files)
        or len(marker.get("expected_symbols") or ()) != 60
    ):
        raise ValueError("normalized marker does not match completed typed outputs")

    gap_path = build_transport_gap_ledger(
        raw_root=args.raw_root,
        derived_root=args.derived_root,
        collector_id=args.collector,
        utc_date=args.date,
    )
    clock_path = build_clock_quality(
        derived_root=args.derived_root,
        collector_id=args.collector,
        utc_date=args.date,
    )
    print(f"legacy normalize finish complete gaps={gap_path} clock={clock_path}")


if __name__ == "__main__":
    main()
