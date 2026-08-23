from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

from miry.contracts.symbols import validate_exchange_symbol
from miry.pipeline.day import audit_d0_day, normalize_day, reconstruct_l2_day
from miry.pipeline.quality import finalize_day


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate and process one sealed UTC day")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("normalize", "l2", "d0-audit", "finalize"):
        command = subparsers.add_parser(name)
        command.add_argument("--raw-root", type=Path, required=True)
        command.add_argument("--derived-root", type=Path, required=True)
        command.add_argument("--collector", required=True)
        command.add_argument("--date", type=date.fromisoformat, required=True)
        if name == "l2":
            command.add_argument("--symbol", required=True)
        if name == "finalize":
            command.add_argument("--symbols", required=True, help="comma-separated symbols")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    common = {
        "derived_root": args.derived_root,
        "collector_id": args.collector,
        "utc_date": args.date,
    }
    if args.command == "normalize":
        result = normalize_day(raw_root=args.raw_root, **common)
        logging.info("normalization complete %s", result)
    elif args.command == "l2":
        changes, intervals = reconstruct_l2_day(exchange_symbol=args.symbol, **common)
        logging.info("L2 complete state_changes=%d valid_intervals=%d", changes, intervals)
    elif args.command == "d0-audit":
        path = audit_d0_day(**common)
        logging.info("D0 audit complete path=%s", path)
    else:
        symbols = tuple(
            sorted(
                {
                    validate_exchange_symbol(value.upper())
                    for value in args.symbols.split(",")
                    if value
                }
            )
        )
        finalize_day(symbols=symbols, **common)


if __name__ == "__main__":
    main()
