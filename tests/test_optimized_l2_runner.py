from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pyarrow as pa
import pyarrow.parquet as pq

RUNNER = Path(__file__).parents[1] / "deploy/campus-107/optimized-l2-runner.py"


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
    spec = importlib.util.spec_from_file_location("optimized_l2_runner", RUNNER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
