from __future__ import annotations

from datetime import date
from pathlib import Path

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from miry.contracts.models import StreamType
from miry.contracts.typed import TYPED_EVENT_SCHEMA
from miry.pipeline.d0 import build_d0_audit
from miry.pipeline.parsing import parse_typed_row


def test_d0_audit_excludes_marked_overlap_duplicates(tmp_path: Path) -> None:
    utc_date = date(2026, 8, 10)
    payload = orjson.dumps(
        {
            "e": "aggTrade",
            "E": 1,
            "T": 2,
            "s": "BTCUSDT",
            "a": 3,
            "p": "100",
            "q": "2",
            "f": 4,
            "l": 5,
            "m": True,
        }
    )
    rows = []
    for connection_id, duplicate in (("old", False), ("replacement", True)):
        row = parse_typed_row(
            {
                "stream_type": StreamType.AGG_TRADE.value,
                "exchange_symbol": "BTCUSDT",
                "connection_id": connection_id,
                "receive_seq": 1,
                "app_receive_realtime_ns": 1,
                "app_receive_monotonic_ns": 1,
                "payload_bytes": payload,
            }
        )
        assert row is not None
        row["is_duplicate"] = duplicate
        rows.append(row)
    typed_root = tmp_path / "typed/collector=tokyo01/date=2026-08-10"
    typed_root.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=TYPED_EVENT_SCHEMA),
        typed_root / "market.typed.parquet",
    )

    report_path = build_d0_audit(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=utc_date,
    )

    report = orjson.loads(report_path.read_bytes())
    assert report["trade"]["agg_trade"] == {
        "events": 1,
        "non_rpi_quantity": "0",
        "notional": "200",
        "quantity": "2",
    }
