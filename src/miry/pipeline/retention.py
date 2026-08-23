from __future__ import annotations

import shutil
from datetime import date, timedelta
from pathlib import Path

from miry.contracts.models import DatasetRelease, DayManifest
from miry.contracts.serde import sha256_file


def expired_unpinned_days(
    *,
    raw_root: Path,
    release_root: Path,
    retention_days: int,
    today: date,
) -> tuple[Path, ...]:
    if retention_days < 1:
        raise ValueError("retention_days must be positive")
    pinned = _pinned_days(raw_root, release_root)
    cutoff = today - timedelta(days=retention_days)
    expired = []
    for collector_root in sorted(raw_root.glob("collector=*")):
        if not collector_root.is_dir():
            continue
        collector_id = collector_root.name.removeprefix("collector=")
        for day_root in sorted(collector_root.glob("date=*")):
            try:
                utc_date = date.fromisoformat(day_root.name.removeprefix("date="))
            except ValueError:
                continue
            sealed = (
                collector_root
                / "day-manifests"
                / f"date={utc_date.isoformat()}"
                / "SEALED.json"
            )
            if utc_date < cutoff and sealed.exists() and (collector_id, utc_date) not in pinned:
                DayManifest.model_validate_json(sealed.read_bytes())
                expired.append(day_root)
    return tuple(expired)


def apply_retention(paths: tuple[Path, ...], *, raw_root: Path) -> None:
    resolved_root = raw_root.resolve()
    for path in paths:
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(resolved_root) or not path.name.startswith("date="):
            raise ValueError(f"unsafe retention target: {path}")
        collector_root = path.parent
        utc_date = date.fromisoformat(path.name.removeprefix("date="))
        shutil.rmtree(resolved)
        manifest_dir = collector_root / "day-manifests" / f"date={utc_date.isoformat()}"
        if manifest_dir.exists():
            shutil.rmtree(manifest_dir)


def _pinned_days(raw_root: Path, release_root: Path) -> set[tuple[str, date]]:
    pinned: set[tuple[str, date]] = set()
    for path in sorted(release_root.glob("*.json")):
        release = DatasetRelease.model_validate_json(path.read_bytes())
        for day in release.days:
            manifest = (
                raw_root
                / f"collector={day.collector_id}"
                / "day-manifests"
                / f"date={day.utc_date.isoformat()}"
                / "SEALED.json"
            )
            if not manifest.exists() or sha256_file(manifest) != day.sealed_manifest_sha256:
                raise ValueError(f"release references a missing or changed manifest: {path}")
            pinned.add((day.collector_id, day.utc_date))
    return pinned
