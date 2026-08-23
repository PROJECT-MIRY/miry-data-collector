from __future__ import annotations

from typing import Any

from miry.contracts.serde import canonical_json_bytes, sha256_bytes

FORMAL_STREAMS = (
    "agg_trade",
    "book_ticker",
    "clock_sample",
    "contract_info",
    "daily_klines",
    "depth",
    "depth_snapshot",
    "exchange_info",
    "force_order",
    "formal_collection_started",
    "liquidity_depth",
    "mark_price",
    "market_tickers",
    "open_interest",
    "universe_decision",
)
D0_STREAMS = ("rpi_depth", "rpi_depth_snapshot", "trade")


def data_contract(
    *, d0_enabled: bool = False, open_interest_interval_seconds: int = 30
) -> bytes:
    streams = FORMAL_STREAMS + (D0_STREAMS if d0_enabled else ())
    payload: dict[str, Any] = {
        "depth_interval_ms": 100,
        "depth_snapshot_limit": 1000,
        "d0_enabled": d0_enabled,
        "exchange": "binance_usdm",
        "gap_schema": 2,
        "mark_price_interval_ms": 1000,
        "open_interest_interval_seconds": open_interest_interval_seconds,
        "raw_schema": 1,
        "streams": sorted(streams),
    }
    return canonical_json_bytes(payload)


def data_contract_hash(
    *, d0_enabled: bool = False, open_interest_interval_seconds: int = 30
) -> str:
    return sha256_bytes(
        data_contract(
            d0_enabled=d0_enabled,
            open_interest_interval_seconds=open_interest_interval_seconds,
        )
    )
