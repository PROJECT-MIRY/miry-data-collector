from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from miry.pipeline.quality import finalize_day


def test_finalize_rejects_empty_l2_validity(tmp_path: Path) -> None:
    utc_date = date(2026, 8, 10)
    symbols = _write_day_outputs(tmp_path, utc_date=utc_date, empty_symbol="BTCUSDT")

    with pytest.raises(ValueError, match="empty L2 validity"):
        finalize_day(
            tmp_path,
            collector_id="tokyo01",
            utc_date=utc_date,
            symbols=symbols,
        )

    assert not (tmp_path / "quality/collector=tokyo01/date=2026-08-10/_PROCESSED.json").exists()


def test_finalize_rejects_negligible_l2_coverage(tmp_path: Path) -> None:
    utc_date = date(2026, 8, 10)
    symbols = _write_day_outputs(tmp_path, utc_date=utc_date, short_symbol="BTCUSDT")

    with pytest.raises(ValueError, match="L2 coverage below"):
        finalize_day(
            tmp_path,
            collector_id="tokyo01",
            utc_date=utc_date,
            symbols=symbols,
        )

    assert not (tmp_path / "quality/collector=tokyo01/date=2026-08-10/_PROCESSED.json").exists()


def test_finalize_records_full_accounted_coverage(tmp_path: Path) -> None:
    utc_date = date(2026, 8, 10)
    symbols = _write_day_outputs(tmp_path, utc_date=utc_date)
    day_start_ns = int(
        datetime.combine(utc_date, datetime.min.time(), UTC).timestamp() * 1_000_000_000
    )
    gap_start = day_start_ns + 12 * 3_600 * 1_000_000_000
    gap_end = gap_start + 60 * 1_000_000_000
    validity_path = (
        tmp_path / "quality/collector=tokyo01/date=2026-08-10/symbol=BTCUSDT/l2-validity.jsonl"
    )
    validity_path.write_text(
        (
            '{"schema_version":1,"connection_id":"a","valid_from_ns":'
            f'{day_start_ns},"valid_to_ns":{gap_start}}}\n'
            '{"schema_version":1,"connection_id":"a","valid_from_ns":'
            f'{gap_end},"valid_to_ns":{day_start_ns + 86_400 * 1_000_000_000}}}\n'
        ),
        encoding="ascii",
    )
    gap_path = tmp_path / "quality/collector=tokyo01/date=2026-08-10/transport-gaps.jsonl"
    gap_path.write_text(
        (
            '{"gap_id":"gap-test","state":"OPEN","exchange_symbols":["BTCUSDT"],'
            f'"stream_types":["depth"],"observed_at_realtime_ns":{gap_start}}}\n'
            '{"gap_id":"gap-test","state":"CLOSED","exchange_symbols":["BTCUSDT"],'
            f'"stream_types":["depth"],"observed_at_realtime_ns":{gap_end}}}\n'
        ),
        encoding="ascii",
    )

    finalize_day(
        tmp_path,
        collector_id="tokyo01",
        utc_date=utc_date,
        symbols=symbols,
    )

    marker = json.loads(
        (tmp_path / "quality/collector=tokyo01/date=2026-08-10/_PROCESSED.json").read_text()
    )
    assert marker["quality_policy"] == "l2-coverage-v1"
    assert marker["l2_coverage"]["BTCUSDT"]["valid_ratio"] == "0.999305556"
    assert marker["l2_coverage"]["BTCUSDT"]["accounted_ratio"] == "1.000000000"


def test_finalize_rejects_operator_symbol_subset(tmp_path: Path) -> None:
    utc_date = date(2026, 8, 10)
    symbols = _write_day_outputs(tmp_path, utc_date=utc_date)

    with pytest.raises(ValueError, match="do not match sealed universe"):
        finalize_day(
            tmp_path,
            collector_id="tokyo01",
            utc_date=utc_date,
            symbols=symbols[:-1],
        )


def test_finalize_rejects_validity_that_overlaps_an_explicit_gap(tmp_path: Path) -> None:
    utc_date = date(2026, 8, 10)
    symbols = _write_day_outputs(tmp_path, utc_date=utc_date)
    day_start_ns = int(
        datetime.combine(utc_date, datetime.min.time(), UTC).timestamp() * 1_000_000_000
    )
    gap_start = day_start_ns + 12 * 3_600 * 1_000_000_000
    gap_end = gap_start + 1_000_000_000
    gap_path = tmp_path / "quality/collector=tokyo01/date=2026-08-10/transport-gaps.jsonl"
    gap_path.write_text(
        (
            '{"gap_id":"gap-conflict","state":"OPEN","exchange_symbols":["BTCUSDT"],'
            f'"stream_types":["depth"],"observed_at_realtime_ns":{gap_start}}}\n'
            '{"gap_id":"gap-conflict","state":"CLOSED","exchange_symbols":["BTCUSDT"],'
            f'"stream_types":["depth"],"observed_at_realtime_ns":{gap_end}}}\n'
        ),
        encoding="ascii",
    )

    with pytest.raises(ValueError, match="L2 coverage below"):
        finalize_day(tmp_path, collector_id="tokyo01", utc_date=utc_date, symbols=symbols)

    rejected = json.loads(
        (tmp_path / "quality/collector=tokyo01/date=2026-08-10/_QUALITY_REJECTED.json").read_text()
    )
    assert rejected["l2_coverage"]["BTCUSDT"]["conflicting_ns"] == 1_000_000_000


def test_finalize_accepts_the_first_formal_partial_day(tmp_path: Path) -> None:
    utc_date = date(2026, 8, 10)
    symbols = _write_day_outputs(tmp_path, utc_date=utc_date)
    day_start_ns = int(
        datetime.combine(utc_date, datetime.min.time(), UTC).timestamp() * 1_000_000_000
    )
    formal_start_ns = day_start_ns + 12 * 3_600 * 1_000_000_000
    typed_marker = tmp_path / "typed/collector=tokyo01/date=2026-08-10/_NORMALIZED.json"
    marker = json.loads(typed_marker.read_text())
    marker["collection_window_start_ns"] = formal_start_ns
    marker["formal_start_realtime_ns"] = formal_start_ns
    typed_marker.write_text(json.dumps(marker), encoding="ascii")
    for symbol in symbols:
        validity = (
            tmp_path
            / f"quality/collector=tokyo01/date=2026-08-10/symbol={symbol}/l2-validity.jsonl"
        )
        validity.write_text(
            '{"schema_version":1,"connection_id":"a","valid_from_ns":'
            f'{formal_start_ns},"valid_to_ns":{day_start_ns + 86_400 * 1_000_000_000}}}\n',
            encoding="ascii",
        )

    finalize_day(tmp_path, collector_id="tokyo01", utc_date=utc_date, symbols=symbols)

    processed = json.loads(
        (tmp_path / "quality/collector=tokyo01/date=2026-08-10/_PROCESSED.json").read_text()
    )
    assert processed["collection_window_start_ns"] == formal_start_ns
    assert processed["l2_coverage"]["BTCUSDT"]["valid_ratio"] == "1.000000000"


def _write_day_outputs(
    root: Path,
    *,
    utc_date: date,
    empty_symbol: str | None = None,
    short_symbol: str | None = None,
) -> tuple[str, ...]:
    symbols = tuple(sorted(("BTCUSDT", *(f"S{index:03}USDT" for index in range(59)))))
    day_start_ns = int(
        datetime.combine(utc_date, datetime.min.time(), UTC).timestamp() * 1_000_000_000
    )
    day_end_ns = day_start_ns + 86_400 * 1_000_000_000
    typed_root = root / f"typed/collector=tokyo01/date={utc_date.isoformat()}"
    typed_root.mkdir(parents=True)
    (typed_root / "_NORMALIZED.json").write_text(
        json.dumps(
            {
                "expected_symbols": list(symbols),
                "collection_window_start_ns": day_start_ns,
                "collection_window_end_ns": day_end_ns,
                "core_generation": 1,
                "candidate_revision": 0,
                "decision_sequence": 1,
                "universe_version": "1.0",
                "universe_hash": "a" * 64,
                "sealed_manifest_sha256": "b" * 64,
            }
        ),
        encoding="ascii",
    )
    for symbol in symbols:
        symbol_root = (
            root / f"quality/collector=tokyo01/date={utc_date.isoformat()}/symbol={symbol}"
        )
        symbol_root.mkdir(parents=True)
        if symbol == empty_symbol:
            validity = ""
        else:
            valid_to_ns = day_start_ns + 1_000_000_000 if symbol == short_symbol else day_end_ns
            validity = (
                '{"schema_version":1,"connection_id":"a","valid_from_ns":'
                f'{day_start_ns},"valid_to_ns":{valid_to_ns}}}\n'
            )
        (symbol_root / "l2-validity.jsonl").write_text(validity, encoding="ascii")
        (symbol_root / "l2-checkpoint.json").write_text(
            '{"schema_version":1,"collector_id":"tokyo01",'
            f'"utc_date":"{utc_date.isoformat()}","exchange_symbol":"{symbol}",'
            '"state":"UNANCHORED","connection_id":null,"previous_update_id":null,'
            '"valid_through_ns":null,"bids":[],"asks":[]}\n',
            encoding="ascii",
        )
    return symbols
