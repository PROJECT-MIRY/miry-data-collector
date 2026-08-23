from __future__ import annotations

import importlib.util
import json
from datetime import date
from pathlib import Path
from types import ModuleType

import pyarrow as pa
import pyarrow.parquet as pq

RUNNER = Path(__file__).parents[1] / "deploy/campus-107/optimized-l2-runner.py"
BUILDER = Path(__file__).parents[1] / "deploy/campus-107/build-l2-inputs.py"


def test_vectorized_depth_filter_matches_scalar_filter(tmp_path: Path) -> None:
    rows = [
        _row("BTCUSDT", "depth", 1),
        _row("ETHUSDT", "depth", 2),
        _row("BTCUSDT", "agg_trade", 3),
        _row("BTCUSDT", "depth_snapshot", 4),
    ]
    path = tmp_path / "typed.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)

    module = _load_runner()
    actual = list(
        module.iter_symbol_depth_rows(
            [path],
            symbol="BTCUSDT",
            depth_values=("depth", "depth_snapshot"),
        )
    )

    assert actual == [rows[0], rows[3]]


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

    builder = _load_module(BUILDER, "build_l2_inputs")
    marker = builder.build_l2_inputs(
        derived_root=derived_root,
        collector="tokyo01",
        utc_date="2026-08-10",
    )
    runner = _load_runner()
    files, mode = runner.l2_input_files(
        derived_root=derived_root,
        collector="tokyo01",
        utc_date=date(2026, 8, 10),
        symbol=symbols[0],
    )
    selected = list(
        runner.iter_symbol_depth_rows(
            files,
            symbol=symbols[0],
            depth_values=("depth", "depth_snapshot"),
        )
    )

    assert marker["total_rows"] == 61
    assert marker["ignored_rows"] == {"OUTUSDT": 1}
    assert mode == "partitioned"
    assert [row["receive_seq"] for row in selected] == [1, 100]


def _row(symbol: str, stream: str, sequence: int) -> dict[str, object]:
    return {
        "exchange_symbol": symbol,
        "stream_type": stream,
        "connection_id": "connection",
        "receive_seq": sequence,
        "app_receive_realtime_ns": sequence,
        "payload_hash": bytes([sequence]),
        "first_update_id": sequence,
        "final_update_id": sequence,
        "previous_final_update_id": sequence - 1,
        "last_update_id": sequence,
        "bids": [{"price": "1", "quantity": "2"}],
        "asks": [{"price": "3", "quantity": "4"}],
    }


def _load_runner() -> ModuleType:
    return _load_module(RUNNER, "optimized_l2_runner")


def _load_module(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
