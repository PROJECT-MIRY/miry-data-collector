"""Run the audited historical L2 repair sequentially by day inside a Slurm allocation."""

from __future__ import annotations

import argparse
import json
import os
import runpy
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, timedelta
from importlib.metadata import version
from pathlib import Path

import pyarrow as pa

from miry.pipeline.day import reconstruct_l2_day
from miry.pipeline.quality import finalize_day


def rebuild_symbol(root: Path, collector: str, day: date, symbol: str) -> tuple[str, int, int]:
    pa.set_cpu_count(1)
    quality = root / "quality" / f"collector={collector}" / f"date={day}"
    if (quality / f"symbol={symbol}" / "l2-checkpoint.json").exists():
        return symbol, 0, 0
    result = reconstruct_l2_day(
        derived_root=root, collector_id=collector, utc_date=day, exchange_symbol=symbol
    )
    return symbol, *result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--derived-root", type=Path, required=True)
    parser.add_argument("--collector", required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--origin", type=date.fromisoformat)
    parser.add_argument("--through", type=date.fromisoformat, required=True)
    parser.add_argument("--input-builder", type=Path, required=True)
    args = parser.parse_args()
    if not os.environ.get("SLURM_JOB_ID"):
        parser.error("historical replay must run in Slurm, not on a login node")
    if version("miry-data-collector") != "0.5.13":
        parser.error("this recovery run requires the verified v0.5.13 runtime")
    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("reconcile-liveness-gaps.py")),
            "--derived-root",
            str(args.derived_root),
            "--collector",
            args.collector,
            "--start",
            str(args.start),
            "--origin",
            str(args.origin or args.start),
            "--through",
            str(args.through),
            "--apply",
        ],
        check=True,
    )
    builder = runpy.run_path(str(args.input_builder))["build_l2_inputs"]
    workers = min(32, int(os.environ["SLURM_CPUS_PER_TASK"]))
    pa.set_cpu_count(2)
    day = args.start
    while day <= args.through:
        quality = args.derived_root / "quality" / f"collector={args.collector}" / f"date={day}"
        markers = (quality / "_PROCESSED.json", quality / "_QUALITY_REJECTED.json")
        if not any(path.exists() for path in markers):
            print(f"recovery inputs date={day}", flush=True)
            marker = builder(
                derived_root=args.derived_root,
                collector=args.collector,
                utc_date=str(day),
                bootstrap_legacy_identities=True,
            )
            symbols = tuple(marker["schedule"])
            with ProcessPoolExecutor(max_workers=workers) as pool:
                futures = [
                    pool.submit(rebuild_symbol, args.derived_root, args.collector, day, symbol)
                    for symbol in symbols
                ]
                for future in as_completed(futures):
                    print(f"recovery L2 date={day} result={future.result()}", flush=True)
            try:
                finalize_day(
                    args.derived_root, collector_id=args.collector, utc_date=day, symbols=symbols
                )
            except ValueError:
                if not markers[1].exists():
                    raise
        marker_path = next(path for path in markers if path.exists())
        result = json.loads(marker_path.read_bytes())
        print(
            json.dumps(
                {
                    "date": str(day),
                    "quality": marker_path.name,
                    "rejected_symbols": result.get("rejected_symbols", []),
                }
            ),
            flush=True,
        )
        day += timedelta(days=1)
    print("historical recovery replay complete", flush=True)


if __name__ == "__main__":
    main()
