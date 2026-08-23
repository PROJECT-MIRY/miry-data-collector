from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import orjson

from miry.contracts.serde import atomic_write_bytes, canonical_json_bytes


class CollectorLease:
    """Durable boot state used to recover gaps after non-graceful exits."""

    def __init__(self, data_root: Path, boot_id: str) -> None:
        self._path = data_root / "control" / "collector-lease.json"
        self._day_manifests = data_root / "ready" / "day-manifests"
        self._boot_id = boot_id
        self._sealed_floor_ns = self._latest_sealed_boundary_ns()

    def recovery_start_ns(self) -> int | None:
        if not self._path.exists():
            return None
        value = orjson.loads(self._path.read_bytes())
        if not isinstance(value, dict) or value.get("state") == "CLEAN":
            return None
        affected_from_ns = value.get("affected_from_realtime_ns")
        if not isinstance(affected_from_ns, int) or affected_from_ns < 0:
            raise ValueError("collector lease has an invalid recovery watermark")
        return max(affected_from_ns, self._sealed_floor_ns)

    def write_running(self, affected_from_ns: int) -> None:
        atomic_write_bytes(
            self._path,
            canonical_json_bytes(
                {
                    "schema_version": 1,
                    "boot_id": self._boot_id,
                    "state": "RUNNING",
                    "affected_from_realtime_ns": max(affected_from_ns, self._sealed_floor_ns),
                }
            ),
        )

    def write_clean(self, affected_from_ns: int) -> None:
        atomic_write_bytes(
            self._path,
            canonical_json_bytes(
                {
                    "schema_version": 1,
                    "boot_id": self._boot_id,
                    "state": "CLEAN",
                    "affected_from_realtime_ns": max(affected_from_ns, self._sealed_floor_ns),
                }
            ),
        )

    def record_sealed(self, utc_date: date) -> None:
        self._sealed_floor_ns = max(
            self._sealed_floor_ns,
            _midnight_ns(utc_date + timedelta(days=1)),
        )

    def _latest_sealed_boundary_ns(self) -> int:
        dates = []
        for path in self._day_manifests.glob("date=*/SEALED.json"):
            try:
                dates.append(date.fromisoformat(path.parent.name.removeprefix("date=")))
            except ValueError:
                continue
        if not dates:
            return 0
        return _midnight_ns(max(dates) + timedelta(days=1))


def _midnight_ns(value: date) -> int:
    return int(datetime.combine(value, time.min, UTC).timestamp() * 1_000_000_000)
