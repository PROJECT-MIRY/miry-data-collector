from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

from miry.contracts.models import CandidateOverride
from miry.contracts.serde import atomic_write_bytes, canonical_json_bytes


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a candidate-only universe override")
    parser.add_argument("--effective-at", type=_utc_datetime, required=True)
    parser.add_argument("--boundary-file", type=Path, required=True)
    parser.add_argument("--probe-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    override = CandidateOverride(
        created_at=datetime.now(UTC),
        effective_at=args.effective_at,
        boundary=_members(args.boundary_file),
        probe=_members(args.probe_file),
    )
    atomic_write_bytes(args.output, canonical_json_bytes(override))


def _members(path: Path) -> tuple[str, ...]:
    return tuple(sorted(value.strip().upper() for value in path.read_text().splitlines() if value))


def _utc_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise argparse.ArgumentTypeError("timestamp must include UTC offset")
    return parsed


if __name__ == "__main__":
    main()
