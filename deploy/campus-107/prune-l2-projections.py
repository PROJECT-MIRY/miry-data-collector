#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from miry.contracts.l2_projection import (
    validate_marker,
    validate_shard,
    validate_source_file_identities,
)
from miry.contracts.serde import sha256_file

PIPELINE_JOB_PREFIXES = (
    "miry-norm-",
    "miry-normalize-",
    "miry-inputs-",
    "miry-l2-inputs-",
    "miry-l2-",
    "miry-finalize-",
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enforce a byte budget for bounded, regenerable L2 projections"
    )
    parser.add_argument("--derived-root", required=True, type=Path)
    parser.add_argument("--collector", required=True)
    parser.add_argument("--max-bytes", required=True, type=int)
    parser.add_argument("--minimum-days", type=int, default=7)
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=datetime.now(UTC).date(),
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="apply the verified plan; without this flag only print the plan",
    )
    args = parser.parse_args()
    if args.max_bytes < 1:
        parser.error("--max-bytes must be positive")
    if args.minimum_days < 7:
        parser.error("--minimum-days cannot be below the shared contract minimum of 7")

    root = (
        args.derived_root
        / "l2-symbol-projections"
        / f"collector={args.collector}"
    )
    plan = retention_plan(
        derived_root=args.derived_root,
        projection_root=root,
        collector=args.collector,
        max_bytes=args.max_bytes,
        minimum_days=args.minimum_days,
        as_of=args.as_of,
    )
    if args.delete and plan["delete"]:
        assert_pipeline_drained()
        for item in plan["delete"]:
            target = root / f"date={item['utc_date']}"
            validate_projection_day(
                derived_root=args.derived_root,
                projection_root=target,
                collector=args.collector,
                utc_date=item["utc_date"],
            )
        assert_pipeline_drained()
        for item in plan["delete"]:
            target = root / f"date={item['utc_date']}"
            if target.is_symlink() or target.parent != root or not target.is_dir():
                raise ValueError(f"unsafe projection deletion target: {target}")
            shutil.rmtree(target)
            fsync_directory(root)
        plan["deleted"] = plan["delete"]
    else:
        plan["deleted"] = []
    print(json.dumps(plan, ensure_ascii=False, separators=(",", ":"), sort_keys=True))


def retention_plan(
    *,
    derived_root: Path,
    projection_root: Path,
    collector: str,
    max_bytes: int,
    minimum_days: int,
    as_of: date,
) -> dict[str, Any]:
    days: list[tuple[date, Path, int]] = []
    for path in sorted(projection_root.glob("date=*")):
        if path.is_symlink() or not path.is_dir():
            raise ValueError(f"invalid projection day path: {path}")
        utc_date = date.fromisoformat(path.name.removeprefix("date="))
        days.append((utc_date, path, tree_bytes(path)))
    total_bytes = sum(item[2] for item in days)
    remaining_bytes = total_bytes
    delete: list[dict[str, Any]] = []
    cutoff = as_of - timedelta(days=minimum_days)
    for utc_date, path, size_bytes in days:
        if remaining_bytes <= max_bytes:
            break
        if utc_date > cutoff:
            continue
        validate_projection_day(
            derived_root=derived_root,
            projection_root=path,
            collector=collector,
            utc_date=utc_date.isoformat(),
        )
        delete.append({"utc_date": utc_date.isoformat(), "size_bytes": size_bytes})
        remaining_bytes -= size_bytes
    if remaining_bytes > max_bytes:
        raise ValueError(
            "projection byte budget cannot be met without violating minimum retention"
        )
    return {
        "as_of": as_of.isoformat(),
        "collector_id": collector,
        "minimum_retention_days": minimum_days,
        "max_bytes": max_bytes,
        "total_bytes": total_bytes,
        "projected_bytes": remaining_bytes,
        "delete": delete,
    }


def validate_projection_day(
    *,
    derived_root: Path,
    projection_root: Path,
    collector: str,
    utc_date: str,
) -> None:
    typed_root = derived_root / "typed" / f"collector={collector}" / f"date={utc_date}"
    normalized_bytes = (typed_root / "_NORMALIZED.json").read_bytes()
    normalized = json.loads(normalized_bytes)
    marker = json.loads((projection_root / "_L2_SYMBOL_PROJECTION.json").read_bytes())
    files = validate_marker(
        marker,
        collector_id=collector,
        utc_date=utc_date,
        normalized_sha256=hashlib.sha256(normalized_bytes).hexdigest(),
        expected_symbols=tuple(normalized.get("expected_symbols") or ()),
    )
    sources = validate_source_file_identities(
        marker.get("typed_source_files"),
        expected_set_hash=marker.get("typed_source_file_set_hash"),
    )
    data_root = derived_root.parent
    for item in sources:
        source = data_root / str(item["uri"])
        if (
            not source.is_file()
            or source.stat().st_size != int(item["size_bytes"])
            or "sha256:" + sha256_file(source) != item["content_hash"]
        ):
            raise ValueError(f"projection is not regenerable from typed source: {source}")
    for symbol, details in files.items():
        validate_shard(projection_root / f"symbol={symbol}.parquet", details)


def assert_pipeline_drained() -> None:
    user = os.environ.get("MIRY_SLURM_USER")
    if user is None:
        user = subprocess.check_output(["id", "-un"], text=True).strip()
    result = subprocess.run(
        ["squeue", "-h", "-u", user, "-o", "%j"],
        check=True,
        capture_output=True,
        text=True,
    )
    active = tuple(
        name.strip()
        for name in result.stdout.splitlines()
        if name.strip().startswith(PIPELINE_JOB_PREFIXES)
    )
    if active:
        raise ValueError(f"pipeline jobs must drain before projection deletion: {active}")


def tree_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


if __name__ == "__main__":
    main()
