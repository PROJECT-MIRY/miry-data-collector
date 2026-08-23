from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from miry.contracts.models import (
    DatasetRelease,
    DayManifest,
    DayReleaseRef,
)
from miry.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    sha256_file,
)
from miry.pipeline.retention import apply_retention, expired_unpinned_days


def test_retention_removes_only_expired_unpinned_sealed_days(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    releases = tmp_path / "releases"
    releases.mkdir()
    pinned_day = date(2026, 1, 1)
    expired_day = date(2026, 1, 2)
    recent_day = date(2026, 8, 1)
    pinned_manifest = _make_day(raw, pinned_day)
    _make_day(raw, expired_day)
    _make_day(raw, recent_day)
    release = DatasetRelease(
        release_id="experiment-001",
        created_at=datetime(2026, 8, 10, tzinfo=UTC),
        days=(
            DayReleaseRef(
                collector_id="tokyo01",
                utc_date=pinned_day,
                sealed_manifest_sha256=sha256_file(pinned_manifest),
            ),
        ),
    )
    atomic_write_bytes(releases / "experiment-001.json", canonical_json_bytes(release))

    expired = expired_unpinned_days(
        raw_root=raw,
        release_root=releases,
        retention_days=90,
        today=date(2026, 8, 10),
    )
    assert [path.name for path in expired] == ["date=2026-01-02"]
    apply_retention(expired, raw_root=raw)
    assert (raw / "collector=tokyo01/date=2026-01-01").exists()
    assert not (raw / "collector=tokyo01/date=2026-01-02").exists()
    assert (raw / "collector=tokyo01/date=2026-08-01").exists()


def _make_day(raw: Path, utc_date: date) -> Path:
    collector = raw / "collector=tokyo01"
    data = collector / f"date={utc_date.isoformat()}" / "writer=depth" / "chunk.parquet"
    data.parent.mkdir(parents=True)
    data.write_bytes(b"raw")
    manifest_path = (
        collector
        / "day-manifests"
        / f"date={utc_date.isoformat()}"
        / "SEALED.json"
    )
    manifest = DayManifest(
        collector_id="tokyo01",
        utc_date=utc_date,
        sealed_at=datetime.combine(utc_date + timedelta(days=1), time.min, UTC),
        chunks=(),
    )
    atomic_write_bytes(manifest_path, canonical_json_bytes(manifest))
    return manifest_path
