from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

from miry.pipeline.retention import apply_retention, expired_unpinned_days


def main() -> None:
    parser = argparse.ArgumentParser(description="Garbage-collect expired, unpinned raw UTC days")
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--release-root", type=Path, required=True)
    parser.add_argument("--retention-days", type=int, default=90)
    parser.add_argument("--apply", action="store_true", help="delete; default is dry-run")
    args = parser.parse_args()
    paths = expired_unpinned_days(
        raw_root=args.raw_root,
        release_root=args.release_root,
        retention_days=args.retention_days,
        today=datetime.now(UTC).date(),
    )
    for path in paths:
        print(path)
    if args.apply:
        apply_retention(paths, raw_root=args.raw_root)


if __name__ == "__main__":
    main()
