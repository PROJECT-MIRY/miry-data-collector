from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
from pathlib import Path

from miry.contracts.models import DatasetRelease, DayManifest, DayReleaseRef
from miry.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    sha256_file,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Pin sealed raw days for a formal experiment")
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument(
        "--day",
        action="append",
        required=True,
        type=_day_reference,
        help="COLLECTOR:YYYY-MM-DD; repeat for each day",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    days = []
    for collector_id, utc_date in args.day:
        path = (
            args.raw_root
            / f"collector={collector_id}"
            / "day-manifests"
            / f"date={utc_date.isoformat()}"
            / "SEALED.json"
        )
        manifest = DayManifest.model_validate_json(path.read_bytes())
        if manifest.collector_id != collector_id or manifest.utc_date != utc_date:
            raise ValueError(f"sealed manifest identity mismatch: {path}")
        days.append(DayReleaseRef(
            collector_id=collector_id,
            utc_date=utc_date,
            sealed_manifest_sha256=sha256_file(path),
        ))
    release = DatasetRelease(
        release_id=args.release_id,
        created_at=datetime.now(UTC),
        days=tuple(sorted(days, key=lambda item: (item.collector_id, item.utc_date))),
    )
    atomic_write_bytes(args.output, canonical_json_bytes(release))


def _day_reference(value: str) -> tuple[str, date]:
    try:
        collector, raw_date = value.split(":", maxsplit=1)
        return collector, date.fromisoformat(raw_date)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("day must be COLLECTOR:YYYY-MM-DD") from exc


if __name__ == "__main__":
    main()
