from __future__ import annotations

import runpy
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPAIR = runpy.run_path(str(Path(__file__).parents[1] / "scripts/reconcile-liveness-gaps.py"))
DAY = date(2026, 9, 15)
START = int(datetime(2026, 9, 15, tzinfo=UTC).timestamp()) * 10**9


def opening(**updates: object) -> dict[str, object]:
    return {
        "gap_id": "gap-orphan",
        "state": "OPEN",
        "reason": "CONNECTION_LOST_GAP",
        "connection_id": None,
        "exchange_symbols": ["BTCUSDT"],
        "stream_types": ["depth"],
        "detail": "public-1: no depth event for 30s",
        "affected_from_realtime_ns": START + 1,
        "observed_at_realtime_ns": START + 30,
        **updates,
    }


def continuation(**updates: object) -> dict[str, object]:
    return opening(
        state="CLOSED",
        detail="gap continues into the next UTC day",
        observed_at_realtime_ns=START + 86400 * 10**9 - 1,
        affected_from_realtime_ns=None,
        **updates,
    )


def test_repair_targets_only_unclosed_single_stream_liveness() -> None:
    assert REPAIR["candidates"]([opening(), continuation()]) == {"gap-orphan": opening()}
    for event in [
        opening(detail="public-1: no close frame received or sent"),
        opening(connection_id="real-connection"),
        opening(reason="L2_SEQUENCE_GAP"),
        opening(exchange_symbols=["BTCUSDT", "ETHUSDT"]),
        opening(stream_types=["depth", "book_ticker"]),
    ]:
        assert REPAIR["candidates"]([event, continuation()]) == {}
    assert (
        REPAIR["candidates"](
            [opening(), continuation(), opening(state="CLOSED", detail="fresh event")]
        )
        == {}
    )


def test_repair_requires_real_post_detection_event(tmp_path: Path) -> None:
    rows = [
        {
            "exchange_symbol": symbol,
            "stream_type": stream,
            "app_receive_realtime_ns": at,
            "connection_id": "recovered",
            "receive_seq": seq,
            "payload_hash": bytes(32),
        }
        for seq, (symbol, stream, at) in enumerate(
            [
                ("BTCUSDT", "depth", START + 29),
                ("BTCUSDT", "depth_snapshot", START + 31),
                ("ETHUSDT", "depth", START + 32),
                ("BTCUSDT", "depth", START + 40),
            ]
        )
    ]
    pq.write_table(pa.Table.from_pylist(rows), tmp_path / "test.typed.parquet")
    proof = REPAIR["find_evidence"](tmp_path, {"gap-orphan": opening()})
    assert proof["gap-orphan"]["app_receive_realtime_ns"] == START + 40
    assert proof["gap-orphan"]["receive_seq"] == 3
    assert len(proof["gap-orphan"]["typed_sha256"]) == 64
    assert (
        REPAIR["find_evidence"](
            tmp_path, {"gap-orphan": opening(observed_at_realtime_ns=START + 50)}
        )
        == {}
    )


def test_repair_keeps_true_gaps_and_removes_only_proven_continuations() -> None:
    real_gap = opening(gap_id="gap-real", connection_id="connection")
    proof = {"gap-orphan": {"app_receive_realtime_ns": START + 40}}
    fixed = REPAIR["reconcile"](
        [opening(), continuation(), real_gap], {"gap-orphan": opening()}, proof, DAY
    )
    assert real_gap in fixed
    assert opening() in fixed
    closed = [event for event in fixed if event["state"] == "CLOSED"]
    assert len(closed) == 1
    assert closed[0]["observed_at_realtime_ns"] == START + 40
    next_day = opening(
        observed_at_realtime_ns=START + 86400 * 10**9,
        affected_from_realtime_ns=START + 86400 * 10**9,
    )
    assert (
        REPAIR["reconcile"]([next_day], {"gap-orphan": opening()}, proof, date(2026, 9, 16)) == []
    )
