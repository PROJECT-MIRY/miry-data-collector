from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from miry.contracts.typed import TYPED_EVENT_SCHEMA
from miry.pipeline.l2 import (
    ConnectionBook,
    DepthDiff,
    DepthSnapshot,
    L2DayReconstructor,
    L2State,
    StateChange,
)


def _diff(
    sequence: int, first: int, final: int, previous: int, *, connection: str = "a"
) -> DepthDiff:
    return DepthDiff(
        connection,
        sequence,
        sequence * 100,
        first,
        final,
        previous,
        bytes([sequence]) * 32,
        (("100", "1"),),
        (("101", "1"),),
    )


def test_snapshot_bridge_duplicate_gap_and_reanchor() -> None:
    future_bridge = ConnectionBook("future")
    assert (
        future_bridge.on_snapshot(
            DepthSnapshot("future", 1, 100, 100, (("99", "1"),), (("102", "1"),))
        )
        is None
    )
    future_change = future_bridge.on_diff(_diff(2, 99, 101, 98, connection="future"))
    assert future_change is not None and future_change.state is L2State.VALID

    book = ConnectionBook("a")
    first = _diff(1, 100, 101, 99)
    assert book.on_diff(first) is None
    change = book.on_snapshot(DepthSnapshot("a", 2, 200, 100, (("99", "1"),), (("102", "1"),)))
    assert change.state is L2State.VALID
    assert book.previous_update_id == 101
    assert book.on_diff(first) is None

    change = book.on_diff(_diff(3, 102, 102, 999))
    assert change is not None and change.state is L2State.GAPPED
    assert book.on_diff(_diff(4, 103, 104, 102)) is None
    change = book.on_snapshot(DepthSnapshot("a", 5, 500, 103, (("99", "1"),), (("102", "1"),)))
    assert change.state is L2State.VALID
    assert book.previous_update_id == 104


def test_unbridged_anchor_does_not_rescan_pending_for_every_new_diff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    book = ConnectionBook(
        connection_id="a",
        anchor_last_update_id=100,
        anchor_received_ns=1,
    )
    calls = 0
    original = ConnectionBook._try_bridge

    def counted(candidate: ConnectionBook) -> StateChange | None:
        nonlocal calls
        calls += 1
        return original(candidate)

    monkeypatch.setattr(ConnectionBook, "_try_bridge", counted)
    for sequence in range(101, 10_101):
        book.on_diff(
            DepthDiff(
                connection_id="a",
                receive_seq=sequence,
                received_ns=sequence,
                first_update_id=sequence,
                final_update_id=sequence,
                previous_final_update_id=sequence - 1,
                payload_hash=sequence.to_bytes(8, "big"),
                bids=(),
                asks=(),
            )
        )

    assert calls == 0
    assert len(book.pending) == 10_000


def test_new_anchored_connection_is_explicit_authority_boundary(tmp_path: Path) -> None:
    typed_root = tmp_path / "typed" / "collector=tokyo01" / "date=2026-08-10"
    typed_root.mkdir(parents=True)
    rows = [
        _typed_depth("a", 1, 100, 100, 101, 99),
        _typed_snapshot("a", 2, 200, 100),
        _typed_depth("b", 1, 300, 200, 201, 199),
        _typed_snapshot("b", 2, 400, 200),
    ]
    pq.write_table(
        pa.Table.from_pylist(rows, schema=TYPED_EVENT_SCHEMA),
        typed_root / "depth.typed.parquet",
    )
    _write_normalized_marker(tmp_path, date(2026, 8, 10), first_formal_day=True)
    changes, intervals = L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=date(2026, 8, 10),
        exchange_symbol="BTCUSDT",
    ).run()
    assert changes == 2
    assert intervals == 2
    lines = (
        (tmp_path / "quality/collector=tokyo01/date=2026-08-10/symbol=BTCUSDT/l2-validity.jsonl")
        .read_text()
        .splitlines()
    )
    assert '"connection_id":"a"' in lines[0]
    assert '"valid_to_ns":400' in lines[0]
    assert '"connection_id":"b"' in lines[1]


def test_next_day_inherits_valid_checkpoint_and_continuous_diff(tmp_path: Path) -> None:
    previous_day = date(2026, 8, 10)
    current_day = previous_day + timedelta(days=1)
    midnight_ns = _midnight_ns(current_day)
    _write_typed_day(
        tmp_path,
        previous_day,
        [
            _typed_snapshot("a", 1, midnight_ns - 2_000_000_000, 100),
            _typed_depth("a", 2, midnight_ns - 1_000_000_000, 100, 101, 99),
        ],
    )
    L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=previous_day,
        exchange_symbol="BTCUSDT",
    ).run()
    _write_typed_day(
        tmp_path,
        current_day,
        [_typed_depth("a", 3, midnight_ns + 1_000_000_000, 102, 102, 101)],
    )

    changes, intervals = L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=current_day,
        exchange_symbol="BTCUSDT",
    ).run()

    assert changes == 0
    assert intervals == 1
    validity = _read_json_lines(
        tmp_path / "quality/collector=tokyo01/date=2026-08-11/symbol=BTCUSDT/l2-validity.jsonl"
    )
    assert validity[0]["valid_from_ns"] == midnight_ns
    checkpoint = json.loads(
        (
            tmp_path / "quality/collector=tokyo01/date=2026-08-11/symbol=BTCUSDT/l2-checkpoint.json"
        ).read_text()
    )
    assert checkpoint["state"] == "VALID"
    assert checkpoint["previous_update_id"] == 102


def test_continuation_day_requires_the_previous_l2_checkpoint(tmp_path: Path) -> None:
    utc_date = date(2026, 8, 11)
    _write_typed_day(
        tmp_path,
        utc_date,
        [_typed_snapshot("a", 1, _midnight_ns(utc_date) + 1, 100)],
    )
    _write_normalized_marker(tmp_path, utc_date, first_formal_day=False)
    _write_normalized_marker(
        tmp_path,
        utc_date - timedelta(days=1),
        first_formal_day=True,
        expected_symbols=("BTCUSDT",),
    )

    with pytest.raises(FileNotFoundError, match="previous L2 checkpoint is required"):
        L2DayReconstructor(
            derived_root=tmp_path,
            collector_id="tokyo01",
            utc_date=utc_date,
            exchange_symbol="BTCUSDT",
        ).run()


def test_new_universe_member_bootstraps_without_a_previous_checkpoint(tmp_path: Path) -> None:
    utc_date = date(2026, 8, 11)
    start_ns = _midnight_ns(utc_date)
    _write_typed_day(
        tmp_path,
        utc_date,
        [
            _typed_snapshot("new", 1, start_ns + 100, 100),
            _typed_depth("new", 2, start_ns + 200, 100, 101, 99),
        ],
    )
    _write_normalized_marker(tmp_path, utc_date, first_formal_day=False)
    _write_normalized_marker(
        tmp_path,
        utc_date - timedelta(days=1),
        first_formal_day=True,
        expected_symbols=("ETHUSDT",),
    )

    _, intervals = L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=utc_date,
        exchange_symbol="BTCUSDT",
    ).run()

    assert intervals == 1


def test_midnight_transport_gap_blocks_checkpoint_until_reanchor(tmp_path: Path) -> None:
    previous_day = date(2026, 8, 10)
    current_day = previous_day + timedelta(days=1)
    midnight_ns = _midnight_ns(current_day)
    _write_typed_day(
        tmp_path,
        previous_day,
        [
            _typed_snapshot("a", 1, midnight_ns - 2_000_000_000, 100),
            _typed_depth("a", 2, midnight_ns - 1_000_000_000, 100, 101, 99),
        ],
    )
    L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=previous_day,
        exchange_symbol="BTCUSDT",
    ).run()
    _write_typed_day(
        tmp_path,
        current_day,
        [
            _typed_depth("a", 3, midnight_ns + 1_000_000_000, 102, 102, 101),
            _typed_snapshot("a", 4, midnight_ns + 2_000_000_000, 102),
            _typed_depth("a", 5, midnight_ns + 3_000_000_000, 102, 103, 101),
        ],
    )
    _write_gap(
        tmp_path,
        current_day,
        state="OPEN",
        observed_ns=midnight_ns,
    )
    _write_gap(
        tmp_path,
        current_day,
        state="CLOSED",
        observed_ns=midnight_ns + 2_500_000_000,
    )

    _, intervals = L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=current_day,
        exchange_symbol="BTCUSDT",
    ).run()

    assert intervals == 1
    validity = _read_json_lines(
        tmp_path / "quality/collector=tokyo01/date=2026-08-11/symbol=BTCUSDT/l2-validity.jsonl"
    )
    assert validity[0]["valid_from_ns"] == midnight_ns + 2_500_000_000


def test_cross_day_sequence_discontinuity_ends_inherited_validity(tmp_path: Path) -> None:
    previous_day = date(2026, 8, 10)
    current_day = previous_day + timedelta(days=1)
    midnight_ns = _midnight_ns(current_day)
    _write_typed_day(
        tmp_path,
        previous_day,
        [
            _typed_snapshot("a", 1, midnight_ns - 2_000_000_000, 100),
            _typed_depth("a", 2, midnight_ns - 1_000_000_000, 100, 101, 99),
        ],
    )
    L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=previous_day,
        exchange_symbol="BTCUSDT",
    ).run()
    _write_typed_day(
        tmp_path,
        current_day,
        [
            _typed_depth("a", 3, midnight_ns + 1_000_000_000, 200, 201, 999),
            _typed_snapshot("a", 4, midnight_ns + 2_000_000_000, 200),
            _typed_depth("a", 5, midnight_ns + 3_000_000_000, 202, 202, 201),
        ],
    )

    _, intervals = L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=current_day,
        exchange_symbol="BTCUSDT",
    ).run()

    assert intervals == 2
    validity = _read_json_lines(
        tmp_path / "quality/collector=tokyo01/date=2026-08-11/symbol=BTCUSDT/l2-validity.jsonl"
    )
    assert validity[0]["valid_from_ns"] == midnight_ns
    assert validity[0]["valid_to_ns"] == midnight_ns + 1_000_000_000
    assert validity[0]["end_reason"] == "pu_discontinuity"
    assert validity[1]["valid_from_ns"] == midnight_ns + 2_000_000_000


def test_connection_snapshot_before_midnight_bridges_on_next_day(tmp_path: Path) -> None:
    previous_day = date(2026, 8, 10)
    current_day = previous_day + timedelta(days=1)
    midnight_ns = _midnight_ns(current_day)
    _write_typed_day(
        tmp_path,
        previous_day,
        [
            _typed_snapshot("old", 1, midnight_ns - 3_000_000_000, 100),
            _typed_depth("old", 2, midnight_ns - 2_000_000_000, 100, 101, 99),
            _typed_snapshot("new", 1, midnight_ns - 1_000_000_000, 200),
        ],
    )
    L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=previous_day,
        exchange_symbol="BTCUSDT",
    ).run()
    _write_typed_day(
        tmp_path,
        current_day,
        [_typed_depth("new", 2, midnight_ns + 1_000_000_000, 200, 201, 199)],
    )

    _, intervals = L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=current_day,
        exchange_symbol="BTCUSDT",
    ).run()

    assert intervals == 2
    validity = _read_json_lines(
        tmp_path / "quality/collector=tokyo01/date=2026-08-11/symbol=BTCUSDT/l2-validity.jsonl"
    )
    assert validity[0]["connection_id"] == "old"
    assert validity[0]["valid_to_ns"] == midnight_ns + 1_000_000_000
    assert validity[1]["connection_id"] == "new"


def test_transport_gap_on_same_connection_reopens_after_snapshot_bridge(tmp_path: Path) -> None:
    utc_date = date(2026, 8, 10)
    start_ns = _midnight_ns(utc_date)
    _write_typed_day(
        tmp_path,
        utc_date,
        [
            _typed_snapshot("a", 1, start_ns + 100, 100),
            _typed_depth("a", 2, start_ns + 200, 100, 101, 99),
            _typed_depth("a", 3, start_ns + 400, 102, 103, 101),
            _typed_snapshot("a", 4, start_ns + 500, 103),
            _typed_depth("a", 5, start_ns + 600, 103, 104, 102),
        ],
    )
    _write_gap(tmp_path, utc_date, state="OPEN", observed_ns=start_ns + 300)
    _write_gap(tmp_path, utc_date, state="CLOSED", observed_ns=start_ns + 550)

    _, intervals = L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=utc_date,
        exchange_symbol="BTCUSDT",
    ).run()

    assert intervals == 2
    validity = _read_json_lines(
        tmp_path / "quality/collector=tokyo01/date=2026-08-10/symbol=BTCUSDT/l2-validity.jsonl"
    )
    assert validity[0]["valid_to_ns"] == start_ns + 300
    assert validity[1]["valid_from_ns"] == start_ns + 550


def test_transport_recovery_does_not_restore_validity_before_snapshot_bridge(
    tmp_path: Path,
) -> None:
    utc_date = date(2026, 8, 10)
    start_ns = _midnight_ns(utc_date)
    _write_typed_day(
        tmp_path,
        utc_date,
        [
            _typed_snapshot("old", 1, start_ns + 100, 100),
            _typed_depth("old", 2, start_ns + 200, 100, 101, 99),
            _typed_depth("new", 1, start_ns + 400, 102, 102, 101),
            _typed_snapshot("new", 2, start_ns + 500, 102),
            _typed_depth("new", 3, start_ns + 600, 102, 103, 101),
        ],
    )
    _write_gap(tmp_path, utc_date, state="OPEN", observed_ns=start_ns + 300)
    _write_gap(tmp_path, utc_date, state="CLOSED", observed_ns=start_ns + 350)

    _, intervals = L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=utc_date,
        exchange_symbol="BTCUSDT",
    ).run()

    assert intervals == 2
    validity = _read_json_lines(
        tmp_path / "quality/collector=tokyo01/date=2026-08-10/symbol=BTCUSDT/l2-validity.jsonl"
    )
    assert validity[0]["valid_to_ns"] == start_ns + 300
    assert validity[1]["valid_from_ns"] == start_ns + 500


def test_liveness_gap_invalidates_from_last_proven_event_not_detection(tmp_path: Path) -> None:
    utc_date = date(2026, 8, 10)
    start_ns = _midnight_ns(utc_date)
    _write_typed_day(
        tmp_path,
        utc_date,
        [
            _typed_snapshot("a", 1, start_ns + 100, 100),
            _typed_depth("a", 2, start_ns + 200, 100, 101, 99),
            _typed_snapshot("a", 3, start_ns + 550, 102),
            _typed_depth("a", 4, start_ns + 700, 102, 102, 101),
        ],
    )
    _write_gap(
        tmp_path,
        utc_date,
        state="OPEN",
        observed_ns=start_ns + 500,
        affected_from_ns=start_ns + 300,
    )
    _write_gap(tmp_path, utc_date, state="CLOSED", observed_ns=start_ns + 600)

    L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=utc_date,
        exchange_symbol="BTCUSDT",
    ).run()

    validity = _read_json_lines(
        tmp_path / "quality/collector=tokyo01/date=2026-08-10/symbol=BTCUSDT/l2-validity.jsonl"
    )
    assert validity[0]["valid_to_ns"] == start_ns + 300
    assert validity[1]["valid_from_ns"] == start_ns + 700


def _write_typed_day(root: Path, utc_date: date, rows: list[dict[str, object]]) -> None:
    typed_root = root / "typed" / "collector=tokyo01" / f"date={utc_date.isoformat()}"
    typed_root.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=TYPED_EVENT_SCHEMA),
        typed_root / "depth.typed.parquet",
    )
    _write_normalized_marker(root, utc_date, first_formal_day=True)


def _write_normalized_marker(
    root: Path,
    utc_date: date,
    *,
    first_formal_day: bool,
    expected_symbols: tuple[str, ...] = ("BTCUSDT",),
) -> None:
    typed_root = root / "typed" / "collector=tokyo01" / f"date={utc_date.isoformat()}"
    typed_root.mkdir(parents=True, exist_ok=True)
    day_start_ns = _midnight_ns(utc_date)
    marker = {
        "formal_start_realtime_ns": day_start_ns + 1 if first_formal_day else None,
        "expected_symbols": list(expected_symbols),
    }
    (typed_root / "_NORMALIZED.json").write_text(json.dumps(marker), encoding="ascii")


def _write_gap(
    root: Path,
    utc_date: date,
    *,
    state: str,
    observed_ns: int,
    affected_from_ns: int | None = None,
) -> None:
    quality_root = root / "quality" / "collector=tokyo01" / f"date={utc_date.isoformat()}"
    quality_root.mkdir(parents=True, exist_ok=True)
    gap = {
        "schema_version": 2,
        "gap_id": "gap-cross-day-test",
        "state": state,
        "reason": "CONNECTION_LOST_GAP",
        "collector_id": "tokyo01",
        "connection_id": "a",
        "exchange_symbols": ["BTCUSDT"],
        "stream_types": ["depth"],
        "observed_at_realtime_ns": observed_ns,
        "affected_from_realtime_ns": affected_from_ns,
        "detail": "test gap",
    }
    with (quality_root / "transport-gaps.jsonl").open("a", encoding="ascii") as target:
        target.write(json.dumps(gap, separators=(",", ":")) + "\n")


def _read_json_lines(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def _midnight_ns(utc_date: date) -> int:
    return int(datetime.combine(utc_date, datetime.min.time(), UTC).timestamp() * 1_000_000_000)


def _typed_depth(
    connection: str, sequence: int, received: int, first: int, final: int, previous: int
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "exchange_symbol": "BTCUSDT",
        "stream_type": "depth",
        "connection_id": connection,
        "receive_seq": sequence,
        "app_receive_realtime_ns": received,
        "app_receive_monotonic_ns": received,
        "payload_hash": bytes([sequence]) * 32,
        "is_duplicate": False,
        "first_update_id": first,
        "final_update_id": final,
        "previous_final_update_id": previous,
        "bids": [{"price": "100", "quantity": "1"}],
        "asks": [{"price": "101", "quantity": "1"}],
    }


def _typed_snapshot(
    connection: str, sequence: int, received: int, last_update_id: int
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "exchange_symbol": "BTCUSDT",
        "stream_type": "depth_snapshot",
        "connection_id": connection,
        "receive_seq": sequence,
        "app_receive_realtime_ns": received,
        "app_receive_monotonic_ns": received,
        "payload_hash": bytes([sequence]) * 32,
        "is_duplicate": False,
        "last_update_id": last_update_id,
        "bids": [{"price": "99", "quantity": "1"}],
        "asks": [{"price": "102", "quantity": "1"}],
    }
