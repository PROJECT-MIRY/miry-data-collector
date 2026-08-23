from __future__ import annotations

import hashlib
from decimal import Decimal, InvalidOperation
from typing import Any

import orjson

from miry.contracts.models import StreamType

TYPED_STREAMS = frozenset(
    {
        StreamType.DEPTH,
        StreamType.RPI_DEPTH,
        StreamType.DEPTH_SNAPSHOT,
        StreamType.RPI_DEPTH_SNAPSHOT,
        StreamType.BOOK_TICKER,
        StreamType.AGG_TRADE,
        StreamType.TRADE,
        StreamType.MARK_PRICE,
        StreamType.FORCE_ORDER,
        StreamType.CONTRACT_INFO,
        StreamType.OPEN_INTEREST,
        StreamType.CLOCK_SAMPLE,
    }
)


def parse_typed_row(raw_row: dict[str, Any]) -> dict[str, Any] | None:
    stream = StreamType(str(raw_row["stream_type"]))
    if stream not in TYPED_STREAMS:
        value = orjson.loads(bytes(raw_row["payload_bytes"]))
        if stream is StreamType.MARKET_TICKERS and not isinstance(value, list):
            raise ValueError("market tickers payload must be an array")
        object_streams = {
            StreamType.EXCHANGE_INFO,
            StreamType.DAILY_KLINES,
            StreamType.LIQUIDITY_DEPTH,
            StreamType.FORMAL_COLLECTION_STARTED,
            StreamType.UNIVERSE_DECISION,
            StreamType.WS_CONTROL,
        }
        if stream in object_streams and not isinstance(value, dict):
            raise ValueError(f"{stream.value} payload must be an object")
        return None
    payload = _json_object(bytes(raw_row["payload_bytes"]))
    data = payload.get("data", payload)
    if not isinstance(data, dict):
        raise ValueError("Binance event data must be an object")
    symbol_value = raw_row.get("exchange_symbol") or _event_symbol(data)
    symbol = str(symbol_value).upper() if symbol_value else None
    if not symbol and stream is not StreamType.CLOCK_SAMPLE:
        raise ValueError(f"{stream.value} event has no symbol")

    row: dict[str, Any] = {
        "schema_version": 1,
        "exchange_symbol": symbol,
        "stream_type": stream.value,
        "connection_id": str(raw_row["connection_id"]),
        "receive_seq": int(raw_row["receive_seq"]),
        "app_receive_realtime_ns": int(raw_row["app_receive_realtime_ns"]),
        "app_receive_monotonic_ns": int(raw_row["app_receive_monotonic_ns"]),
        "request_realtime_ns": raw_row.get("request_realtime_ns"),
        "exchange_event_time_ms": None,
        "exchange_transaction_time_ms": None,
        "payload_hash": hashlib.sha256(orjson.dumps(data, option=orjson.OPT_SORT_KEYS)).digest(),
        "is_duplicate": False,
    }

    if stream in {StreamType.DEPTH, StreamType.RPI_DEPTH}:
        _require_event(data, "depthUpdate")
        row.update(
            exchange_event_time_ms=_int(data, "E"),
            exchange_transaction_time_ms=_int(data, "T"),
            first_update_id=_int(data, "U"),
            final_update_id=_int(data, "u"),
            previous_final_update_id=_int(data, "pu"),
            bids=_levels(data, "b"),
            asks=_levels(data, "a"),
        )
    elif stream in {StreamType.DEPTH_SNAPSHOT, StreamType.RPI_DEPTH_SNAPSHOT}:
        row.update(
            exchange_event_time_ms=_optional_int(data, "E"),
            exchange_transaction_time_ms=_optional_int(data, "T"),
            last_update_id=_int(data, "lastUpdateId"),
            bids=_levels(data, "bids"),
            asks=_levels(data, "asks"),
        )
    elif stream is StreamType.BOOK_TICKER:
        _require_event(data, "bookTicker")
        row.update(
            exchange_event_time_ms=_int(data, "E"),
            exchange_transaction_time_ms=_int(data, "T"),
            update_id=_int(data, "u"),
            bid_price=_decimal(data, "b"),
            bid_quantity=_decimal(data, "B", allow_zero=True),
            ask_price=_decimal(data, "a"),
            ask_quantity=_decimal(data, "A", allow_zero=True),
        )
    elif stream in {StreamType.AGG_TRADE, StreamType.TRADE}:
        _require_event(data, "aggTrade" if stream is StreamType.AGG_TRADE else "trade")
        row.update(
            exchange_event_time_ms=_int(data, "E"),
            exchange_transaction_time_ms=_int(data, "T"),
            price=_decimal(data, "p"),
            quantity=_decimal(data, "q"),
            buyer_is_maker=_bool(data, "m"),
        )
        if stream is StreamType.AGG_TRADE:
            row.update(
                aggregate_trade_id=_int(data, "a"),
                first_trade_id=_int(data, "f"),
                last_trade_id=_int(data, "l"),
                non_rpi_quantity=_optional_decimal(data, "nq"),
            )
        else:
            row["trade_id"] = _int(data, "t")
    elif stream is StreamType.MARK_PRICE:
        _require_event(data, "markPriceUpdate")
        row.update(
            exchange_event_time_ms=_int(data, "E"),
            mark_price=_decimal(data, "p"),
            index_price=_decimal(data, "i"),
            estimated_settle_price=_decimal(data, "P", allow_zero=True),
            funding_rate=_decimal(data, "r", allow_zero=True, allow_negative=True),
            next_funding_time_ms=_int(data, "T"),
        )
    elif stream is StreamType.FORCE_ORDER:
        _require_event(data, "forceOrder")
        order = data.get("o")
        if not isinstance(order, dict):
            raise ValueError("forceOrder payload has no order object")
        row.update(
            exchange_event_time_ms=_int(data, "E"),
            exchange_transaction_time_ms=_int(order, "T"),
            side=str(order["S"]),
            order_type=str(order["o"]),
            time_in_force=str(order["f"]),
            order_status=str(order["X"]),
            price=_decimal(order, "p", allow_zero=True),
            quantity=_decimal(order, "q"),
            average_price=_decimal(order, "ap", allow_zero=True),
            last_filled_quantity=_decimal(order, "l", allow_zero=True),
            accumulated_filled_quantity=_decimal(order, "z", allow_zero=True),
        )
    elif stream is StreamType.CONTRACT_INFO:
        _require_event(data, "contractInfo")
        row.update(
            exchange_event_time_ms=_int(data, "E"),
            contract_type=str(data["ct"]),
            delivery_date_ms=_int(data, "dt"),
            onboard_date_ms=_int(data, "ot"),
            contract_status=str(data["cs"]),
        )
    elif stream is StreamType.OPEN_INTEREST:
        row.update(
            exchange_event_time_ms=_int(data, "time"),
            open_interest=_decimal(data, "openInterest", allow_zero=True),
        )
    elif stream is StreamType.CLOCK_SAMPLE:
        row["exchange_event_time_ms"] = _int(data, "serverTime")
    return row


def logical_identity(row: dict[str, Any]) -> tuple[object, ...] | None:
    stream = StreamType(str(row["stream_type"]))
    symbol = str(row["exchange_symbol"])
    if stream is StreamType.AGG_TRADE:
        return stream, symbol, int(row["aggregate_trade_id"])
    if stream is StreamType.TRADE:
        return stream, symbol, int(row["trade_id"])
    if stream in {StreamType.DEPTH, StreamType.RPI_DEPTH}:
        return (
            stream,
            symbol,
            int(row["first_update_id"]),
            int(row["final_update_id"]),
            int(row["previous_final_update_id"]),
        )
    if stream is StreamType.BOOK_TICKER:
        return stream, symbol, int(row["update_id"])
    if stream is StreamType.MARK_PRICE:
        return (
            stream,
            symbol,
            int(row["exchange_event_time_ms"]),
            bytes(row["payload_hash"]),
        )
    if stream is StreamType.FORCE_ORDER:
        return (
            stream,
            symbol,
            int(row["exchange_event_time_ms"]),
            int(row["exchange_transaction_time_ms"]),
            bytes(row["payload_hash"]),
        )
    if stream is StreamType.CONTRACT_INFO:
        return (
            stream,
            symbol,
            int(row["exchange_event_time_ms"]),
            bytes(row["payload_hash"]),
        )
    return None


def _event_symbol(data: dict[str, Any]) -> str:
    if isinstance(data.get("o"), dict):
        return str(data["o"].get("s", ""))
    return str(data.get("s", ""))


def _json_object(raw: bytes) -> dict[str, Any]:
    value = orjson.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("Binance payload must be a JSON object")
    return value


def _require_event(data: dict[str, Any], expected: str) -> None:
    if data.get("e") != expected:
        raise ValueError(f"expected Binance event {expected!r}, got {data.get('e')!r}")


def _int(data: dict[str, Any], key: str) -> int:
    value = data[key]
    if isinstance(value, bool):
        raise ValueError(f"{key} must be an integer")
    return int(value)


def _optional_int(data: dict[str, Any], key: str) -> int | None:
    return _int(data, key) if key in data else None


def _bool(data: dict[str, Any], key: str) -> bool:
    value = data[key]
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be a boolean")
    return value


def _decimal(
    data: dict[str, Any],
    key: str,
    *,
    allow_zero: bool = False,
    allow_negative: bool = False,
) -> str:
    try:
        value = Decimal(str(data[key]))
    except (InvalidOperation, KeyError) as exc:
        raise ValueError(f"{key} must be a decimal") from exc
    if not value.is_finite():
        raise ValueError(f"{key} must be finite")
    if value < 0 and not allow_negative:
        raise ValueError(f"{key} cannot be negative")
    if value == 0 and not allow_zero:
        raise ValueError(f"{key} must be positive")
    return str(value)


def _levels(data: dict[str, Any], key: str) -> list[dict[str, str]]:
    raw_levels = data.get(key)
    if not isinstance(raw_levels, list):
        raise ValueError(f"{key} must be an array")
    levels = []
    for level in raw_levels:
        if not isinstance(level, list) or len(level) < 2:
            raise ValueError(f"invalid level in {key}")
        values = {"p": level[0], "q": level[1]}
        levels.append(
            {
                "price": _decimal(values, "p"),
                "quantity": _decimal(values, "q", allow_zero=True),
            }
        )
    return levels


def _optional_decimal(data: dict[str, Any], key: str) -> str | None:
    return _decimal(data, key, allow_zero=True) if key in data else None
