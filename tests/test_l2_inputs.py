from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from miry.contracts.typed import (
    L2_SYMBOL_PROJECTION_SCHEMA_HASH,
    L2_SYMBOL_PROJECTION_SCHEMA_ID,
)
from miry.pipeline.day import reconstruct_l2_day
from miry.pipeline.l2 import partitioned_l2_input

BUILDER = Path(__file__).parents[1] / "deploy/campus-107/build-l2-inputs.py"
SCHEMA = Path(__file__).parents[1] / "schemas/l2-symbol-projection-v1.schema.json"


def test_partitioned_l2_inputs_preserve_symbol_order(tmp_path: Path) -> None:
    symbols = [f"S{index:02d}USDT" for index in range(60)]
    rows = [_row(symbol, "depth", index + 1) for index, symbol in enumerate(symbols)]
    rows.append(_row(symbols[0], "depth_snapshot", 100))
    rows.append(_row("OUTUSDT", "depth", 101))
    derived_root = tmp_path / "derived"
    typed_root = derived_root / "typed/collector=tokyo01/date=2026-08-10"
    typed_root.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), typed_root / "chunk.typed.parquet")
    (typed_root / "_NORMALIZED.json").write_text(
        json.dumps(
            {
                "collector_id": "tokyo01",
                "utc_date": "2026-08-10",
                "expected_symbols": symbols,
            }
        ),
        encoding="ascii",
    )

    builder = _load_builder("build_l2_inputs")
    marker = builder.build_l2_inputs(
        derived_root=derived_root,
        collector="tokyo01",
        utc_date="2026-08-10",
    )
    path = partitioned_l2_input(
        derived_root=derived_root,
        collector_id="tokyo01",
        utc_date=date(2026, 8, 10),
        exchange_symbol=symbols[0],
    )
    assert path is not None
    selected = pq.read_table(path).to_pylist()

    assert marker["total_rows"] == 61
    assert marker["ignored_rows"] == {"OUTUSDT": 1}
    assert marker["schema_version"] == 3
    assert marker["schema_id"] == L2_SYMBOL_PROJECTION_SCHEMA_ID
    assert marker["schema_hash"] == L2_SYMBOL_PROJECTION_SCHEMA_HASH
    assert "sha256:" + hashlib.sha256(SCHEMA.read_bytes()).hexdigest() == marker["schema_hash"]
    assert marker["layout"] == "PER_SYMBOL_L2_CAUSAL_V1"
    assert marker["persistent_for_downstream"] is True
    assert len(marker["files"][symbols[0]]["sha256"]) == 64
    assert marker["typed_source_file_set_hash"].startswith("sha256:")
    assert len(marker["typed_source_files"]) == 1
    assert marker["schedule"][0] == symbols[0]
    assert "exchange_symbol" not in selected[0]
    assert [row["receive_seq"] for row in selected] == [1, 100]


def test_l2_reconstruction_opens_only_the_partitioned_symbol_file(
    tmp_path: Path, monkeypatch
) -> None:
    symbols = [f"S{index:02d}USDT" for index in range(60)]
    target = symbols[0]
    derived_root = tmp_path / "derived"
    typed_root = derived_root / "typed/collector=tokyo01/date=2026-08-10"
    typed_root.mkdir(parents=True)
    start_ns = int(datetime(2026, 8, 10, tzinfo=UTC).timestamp() * 1_000_000_000)
    for file_index in range(5):
        rows = [
            _row(symbol, "depth", file_index * 100 + index + 1)
            for index, symbol in enumerate(symbols)
        ]
        if file_index == 0:
            rows[0] = _row(target, "depth_snapshot", 1)
        elif file_index == 1:
            rows[0] = _row(target, "depth", 2)
        for row in rows:
            row["app_receive_realtime_ns"] = start_ns + int(row["receive_seq"])
        pq.write_table(pa.Table.from_pylist(rows), typed_root / f"chunk-{file_index}.typed.parquet")
    (typed_root / "_NORMALIZED.json").write_text(
        json.dumps(
            {
                "collector_id": "tokyo01",
                "utc_date": "2026-08-10",
                "expected_symbols": symbols,
                "formal_start_realtime_ns": start_ns,
            }
        ),
        encoding="ascii",
    )
    previous_root = typed_root.parent / f"date={(date(2026, 8, 10) - timedelta(days=1))}"
    previous_root.mkdir()
    (previous_root / "_NORMALIZED.json").write_text(
        json.dumps({"expected_symbols": []}), encoding="ascii"
    )
    builder = _load_builder("build_l2_inputs_open_count")
    builder.build_l2_inputs(
        derived_root=derived_root,
        collector="tokyo01",
        utc_date="2026-08-10",
    )
    parquet_opens = 0
    original = pq.ParquetFile

    def counted(*args, **kwargs):
        nonlocal parquet_opens
        parquet_opens += 1
        return original(*args, **kwargs)

    monkeypatch.setattr("miry.pipeline.l2.pq.ParquetFile", counted)

    reconstruct_l2_day(
        derived_root=derived_root,
        collector_id="tokyo01",
        utc_date=date(2026, 8, 10),
        exchange_symbol=target,
    )

    assert parquet_opens == 1


def test_partitioned_l2_input_rejects_same_size_rewrite(tmp_path: Path) -> None:
    symbols = [f"S{index:02d}USDT" for index in range(60)]
    derived_root = tmp_path / "derived"
    typed_root = derived_root / "typed/collector=tokyo01/date=2026-08-10"
    typed_root.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [_row(symbol, "depth", index + 1) for index, symbol in enumerate(symbols)]
        ),
        typed_root / "chunk.typed.parquet",
    )
    (typed_root / "_NORMALIZED.json").write_text(
        json.dumps(
            {
                "collector_id": "tokyo01",
                "utc_date": "2026-08-10",
                "expected_symbols": symbols,
            }
        ),
        encoding="ascii",
    )
    builder = _load_builder("build_l2_inputs_tamper")
    builder.build_l2_inputs(
        derived_root=derived_root, collector="tokyo01", utc_date="2026-08-10"
    )
    path = derived_root / "l2-inputs/collector=tokyo01/date=2026-08-10/symbol=S00USDT.parquet"
    payload = path.read_bytes()
    path.write_bytes(payload[:-1] + bytes((payload[-1] ^ 1,)))
    try:
        partitioned_l2_input(
            derived_root=derived_root,
            collector_id="tokyo01",
            utc_date=date(2026, 8, 10),
            exchange_symbol="S00USDT",
        )
    except ValueError:
        pass
    else:
        raise AssertionError("same-size L2 shard rewrite was accepted")


def _row(symbol: str, stream: str, sequence: int) -> dict[str, object]:
    return {
        "exchange_symbol": symbol,
        "stream_type": stream,
        "connection_id": "connection",
        "receive_seq": sequence,
        "app_receive_realtime_ns": sequence,
        "app_receive_monotonic_ns": sequence,
        "exchange_event_time_ms": sequence,
        "exchange_transaction_time_ms": sequence,
        "payload_hash": bytes([sequence % 256]),
        "is_duplicate": False,
        "first_update_id": sequence,
        "final_update_id": sequence,
        "previous_final_update_id": sequence - 1,
        "last_update_id": sequence,
        "bids": [{"price": "1", "quantity": "2"}],
        "asks": [{"price": "3", "quantity": "4"}],
    }


def _load_builder(name: str):
    import importlib.util

    spec = importlib.util.spec_from_file_location(name, BUILDER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
