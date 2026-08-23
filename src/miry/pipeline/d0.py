from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from miry.contracts.models import StreamType
from miry.contracts.serde import atomic_write_bytes, canonical_json_bytes


def build_d0_audit(
    *,
    derived_root: Path,
    collector_id: str,
    utc_date: date,
) -> Path:
    typed_root = (
        derived_root / "typed" / f"collector={collector_id}" / f"date={utc_date.isoformat()}"
    )
    trade = {
        StreamType.TRADE: _trade_totals(),
        StreamType.AGG_TRADE: _trade_totals(),
    }
    depth = {
        StreamType.DEPTH: {"events": 0, "level_updates": 0},
        StreamType.RPI_DEPTH: {"events": 0, "level_updates": 0},
    }
    columns = [
        "stream_type",
        "is_duplicate",
        "price",
        "quantity",
        "non_rpi_quantity",
        "bids",
        "asks",
    ]
    for path in sorted(typed_root.glob("*.typed.parquet")):
        for batch in pq.ParquetFile(path).iter_batches(batch_size=10_000, columns=columns):
            for row in batch.to_pylist():
                if row["is_duplicate"]:
                    continue
                stream = StreamType(str(row["stream_type"]))
                if stream in trade:
                    price = Decimal(str(row["price"]))
                    quantity = Decimal(str(row["quantity"]))
                    totals = trade[stream]
                    totals["events"] += 1
                    totals["quantity"] += quantity
                    totals["notional"] += price * quantity
                    if row["non_rpi_quantity"] is not None:
                        totals["non_rpi_quantity"] += Decimal(row["non_rpi_quantity"])
                elif stream in depth:
                    depth[stream]["events"] += 1
                    depth[stream]["level_updates"] += len(row["bids"] or []) + len(
                        row["asks"] or []
                    )
    report = {
        "schema_version": 1,
        "collector_id": collector_id,
        "utc_date": utc_date.isoformat(),
        "trade": {stream.value: _serialize_totals(values) for stream, values in trade.items()},
        "depth": {stream.value: values for stream, values in depth.items()},
        "default_decision": {
            "official_trade_source": StreamType.AGG_TRADE.value,
            "rpi_depth_enabled": False,
            "automatic_switching": False,
        },
    }
    destination = (
        derived_root
        / "quality"
        / f"collector={collector_id}"
        / f"date={utc_date.isoformat()}"
        / "D0Audit.json"
    )
    atomic_write_bytes(destination, canonical_json_bytes(report))
    return destination


def _trade_totals() -> dict[str, Any]:
    return {
        "events": 0,
        "quantity": Decimal(0),
        "notional": Decimal(0),
        "non_rpi_quantity": Decimal(0),
    }


def _serialize_totals(values: dict[str, Any]) -> dict[str, int | str]:
    return {key: value if isinstance(value, int) else str(value) for key, value in values.items()}
