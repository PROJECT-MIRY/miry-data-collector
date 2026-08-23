from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pytest

from miry.cli.symbols import main
from miry.collector.polling import _eligible_instruments
from miry.collector.websocket import public_subscriptions
from miry.contracts.models import RawEvent, StreamType
from miry.contracts.symbols import is_exchange_symbol, validate_symbols
from miry.pipeline.l2 import L2DayReconstructor


def test_chinese_canonical_symbols_are_valid() -> None:
    assert is_exchange_symbol("币安人生USDT")
    assert is_exchange_symbol("我踏马来了USDT")
    assert is_exchange_symbol("龙虾USDT")
    assert validate_symbols(("币安人生USDT", "BTCUSDT")) == (
        "币安人生USDT",
        "BTCUSDT",
    )
    event = RawEvent(
        schema_version=1,
        exchange_symbol="币安人生USDT",
        stream_type=StreamType.DEPTH,
        collector_id="tokyo01",
        boot_id="boot",
        segment_id="segment",
        connection_id="connection",
        receive_seq=1,
        app_receive_realtime_ns=1,
        app_receive_monotonic_ns=1,
        payload_bytes=b"{}",
    )
    assert event.exchange_symbol == "币安人生USDT"


def test_chinese_contract_is_eligible_for_discovery() -> None:
    exchange_info = json.dumps(
        {
            "symbols": [
                {
                    "symbol": "币安人生USDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "onboardDate": 1_760_958_000_000,
                }
            ]
        },
        ensure_ascii=False,
    ).encode()

    assert _eligible_instruments(exchange_info) == {"币安人生USDT": 1_760_958_000_000}
    assert public_subscriptions(("币安人生USDT",), d0_enabled=False) == (
        "币安人生usdt@bookTicker",
        "币安人生usdt@depth@100ms",
    )


def test_chinese_derived_path_is_safe(tmp_path: Path) -> None:
    reconstructor = L2DayReconstructor(
        derived_root=tmp_path,
        collector_id="tokyo01",
        utc_date=date(2026, 8, 23),
        exchange_symbol="币安人生USDT",
    )
    assert reconstructor._quality_root.name == "symbol=币安人生USDT"

    with pytest.raises(ValueError, match="invalid canonical"):
        L2DayReconstructor(
            derived_root=tmp_path,
            collector_id="tokyo01",
            utc_date=date(2026, 8, 23),
            exchange_symbol="../币安人生USDT",
        )


@pytest.mark.parametrize(
    "value",
    ("btcusdt", "../BTCUSDT", "BTC/USDT", "BTC USDT", "BTC_USDT", " BTCUSDT"),
)
def test_unsafe_or_noncanonical_symbols_are_rejected(value: str) -> None:
    assert not is_exchange_symbol(value)
    with pytest.raises(ValueError, match="invalid canonical"):
        validate_symbols((value,))


def test_symbol_cli_emits_canonical_ascii(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    symbols_path = tmp_path / "symbols.txt"
    symbols_path.write_text("币安人生USDT\nETHUSDT\n", encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "miry-data-symbols",
            "--input",
            str(symbols_path),
        ],
    )

    main()

    assert capsys.readouterr().out == "币安人生USDT\nETHUSDT\n"
