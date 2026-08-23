from __future__ import annotations

import os
from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from miry.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    fsync_directory,
)


def utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("transfer timestamps must be UTC")
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def transfer_event(
    event: str,
    *,
    occurred_at: datetime,
    event_id: str,
    **fields: Any,
) -> dict[str, Any]:
    return {
        "event": event,
        "event_id": event_id,
        "occurred_at": utc_text(occurred_at),
        **fields,
    }


class TransferJournal:
    """Append structured transfer evidence with one durable write per UTC-day batch."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def initialize(self) -> None:
        existed = self.root.exists()
        self.root.mkdir(parents=True, exist_ok=True)
        if not existed:
            fsync_directory(self.root.parent)
        fsync_directory(self.root)

    def append(self, events: Iterable[dict[str, Any]]) -> int:
        grouped: dict[str, list[bytes]] = defaultdict(list)
        count = 0
        for event in events:
            occurred_at = event.get("occurred_at")
            if not isinstance(occurred_at, str) or len(occurred_at) < 10:
                raise ValueError("transfer event requires an ISO UTC occurred_at")
            grouped[occurred_at[:10]].append(canonical_json_bytes(event))
            count += 1
        for utc_date, encoded in grouped.items():
            directory = self.root / f"date={utc_date}"
            directory_existed = directory.exists()
            directory.mkdir(parents=True, exist_ok=True)
            if not directory_existed:
                fsync_directory(self.root)
            destination = directory / "events.jsonl"
            descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o640)
            try:
                with os.fdopen(descriptor, "wb", closefd=False) as output:
                    output.write(b"".join(encoded))
                    output.flush()
                    os.fsync(output.fileno())
            finally:
                os.close(descriptor)
            fsync_directory(directory)
        return count


def write_transfer_status(path: Path, status: dict[str, Any]) -> None:
    atomic_write_bytes(path, canonical_json_bytes(status))
