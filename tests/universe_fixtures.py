from __future__ import annotations

from datetime import UTC, datetime, timedelta

import orjson

from miry.universe.models import DiscoverySnapshot

DAY_MS = 86_400_000


def symbols(start: int, stop: int) -> tuple[str, ...]:
    return tuple(f"S{index:03}USDT" for index in range(start, stop))


def formal_roles() -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    return symbols(0, 50), symbols(50, 55), symbols(65, 70)


def liquidity_snapshot(
    observed_at: datetime,
    *,
    inactive: str | None = None,
    incomplete: frozenset[str] = frozenset(),
    missing_depth: frozenset[str] = frozenset(),
    weak_market: frozenset[str] = frozenset(),
    spread_outlier: frozenset[str] = frozenset(),
    low_activity: frozenset[str] = frozenset(),
    volatile_activity: frozenset[str] = frozenset(),
    activity_factor: int = 1,
    activity_days: int = 0,
    activity_symbols: frozenset[str] | None = None,
) -> DiscoverySnapshot:
    cutoff = datetime.combine(observed_at.date(), datetime.min.time(), UTC)
    cutoff_ms = int(cutoff.timestamp() * 1000)
    start_ms = cutoff_ms - 35 * DAY_MS
    exchange_rows = []
    kline_rows: dict[str, dict[str, object]] = {}
    depth_rows: dict[str, list[dict[str, object]]] = {}
    book_rows = []
    for index, symbol in enumerate(symbols(0, 75)):
        age_days = 100 if index < 65 else 20 - (index - 65)
        exchange_rows.append(
            {
                "symbol": symbol,
                "contractType": "PERPETUAL",
                "status": "SETTLING" if symbol == inactive else "TRADING",
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "onboardDate": int((cutoff - timedelta(days=age_days)).timestamp() * 1000),
            }
        )
        base_volume = 1_000 if symbol in low_activity else 100_000_000 - index * 1_000_000
        base_trades = 10 if symbol in low_activity else 200_000 - index * 1_000
        first_full_day = cutoff_ms - age_days * DAY_MS
        bars = []
        for open_ms in range(max(start_ms, first_full_day), cutoff_ms, DAY_MS):
            volume = base_volume
            trades = base_trades
            selected_for_move = activity_symbols is None or symbol in activity_symbols
            if selected_for_move and open_ms >= cutoff_ms - activity_days * DAY_MS:
                volume *= activity_factor
                trades *= activity_factor
            if symbol in volatile_activity and open_ms == cutoff_ms - DAY_MS:
                volume *= 1_000
                trades *= 1_000
            bars.append(
                [
                    open_ms,
                    "1",
                    "1",
                    "1",
                    "1",
                    "1",
                    open_ms + DAY_MS - 1,
                    str(volume),
                    trades,
                ]
            )
        if symbol in incomplete:
            bars.pop()
        kline_rows[symbol] = {"payload": bars, "response_sha256": "a" * 64}
        if symbol not in missing_depth:
            bid_price, ask_price, quantity = (
                ("90", "110", "0.001")
                if symbol in weak_market
                else ("99.99", "100.01", "1000")
            )
            depth_rows[symbol] = [
                {
                    "payload": {
                        "lastUpdateId": sample,
                        "bids": [[bid_price, quantity]],
                        "asks": [[ask_price, quantity]],
                    },
                    "response_sha256": "b" * 64,
                    "round": sample,
                }
                for sample in range(1, 4)
            ]
        book_rows.append(
            {"symbol": symbol, "bidPrice": "99.99", "askPrice": "100.01"}
        )
    exchange_info = orjson.dumps({"symbols": exchange_rows})
    daily_klines = orjson.dumps(
        {
            "schema_version": 1,
            "window_start_ms": start_ms,
            "window_end_exclusive_ms": cutoff_ms,
            "symbols": kline_rows,
        }
    )
    liquidity_depth = orjson.dumps(
        {
            "schema_version": 1,
            "book_tickers": [
                {
                    "sample": sample,
                    "payload": [
                        {
                            **row,
                            **(
                                {"bidPrice": "50", "askPrice": "150"}
                                if sample == 1 and row["symbol"] in spread_outlier
                                else {}
                            ),
                        }
                        for row in book_rows
                    ],
                    "response_sha256": "c" * 64,
                }
                for sample in range(1, 22)
            ],
            "symbols": depth_rows,
        }
    )
    return DiscoverySnapshot(
        observed_at=observed_at,
        exchange_info=exchange_info,
        exchange_info_confirmation=exchange_info,
        market_tickers=b"[]",
        daily_klines=daily_klines,
        liquidity_depth=liquidity_depth,
    )
