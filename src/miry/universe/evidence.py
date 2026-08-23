from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import orjson

from miry.contracts.symbols import is_exchange_symbol
from miry.universe.models import DiscoverySnapshot, RollingPolicy
from miry.universe.ranking import rank_cross_section
from miry.universe.regime import (
    CONFIRMATION_DAYS,
    ActivitySeries,
    MarketContext,
    evaluate_market_context,
)

DAY_MS = 86_400_000
TEN_BPS = Decimal("0.001")
FIFTY_BPS = Decimal("0.005")


@dataclass(frozen=True, slots=True)
class _HistoricalRow:
    symbol: str
    onboard_time_ms: int
    age_days: int
    volumes: tuple[Decimal, ...]
    trades: tuple[int, ...]
    market_volumes: tuple[Decimal, ...]
    market_trades: tuple[int, ...]
    expected_probe_days: int
    observed_probe_days: int
    q25_quote_volume: Decimal
    q25_trades: Decimal


@dataclass(frozen=True, slots=True)
class _MarketRow:
    history: _HistoricalRow
    depth_sample_count: int
    thin_depth_10bps: Decimal
    thin_depth_50bps: Decimal
    book_ticker_sample_count: int
    spread_q95_bps: Decimal

    @property
    def symbol(self) -> str:
        return self.history.symbol

    @property
    def onboard_time_ms(self) -> int:
        return self.history.onboard_time_ms

    @property
    def age_days(self) -> int:
        return self.history.age_days

    @property
    def q25_quote_volume(self) -> Decimal:
        return self.history.q25_quote_volume

    @property
    def q25_trades(self) -> Decimal:
        return self.history.q25_trades


@dataclass(frozen=True, slots=True)
class _LiquidityMetrics:
    depth_sample_count: int
    thin_depth_10bps: Decimal
    thin_depth_50bps: Decimal
    book_ticker_sample_count: int
    spread_q95_bps: Decimal


def _market_rows(
    snapshot: DiscoverySnapshot,
    *,
    tracked: tuple[str, ...],
    policy: RollingPolicy,
) -> tuple[list[_MarketRow], tuple[str, ...]]:
    first_info = _symbol_info(snapshot.exchange_info)
    confirmed_info = _symbol_info(snapshot.exchange_info_confirmation)
    eligible = {
        symbol: confirmation
        for symbol, raw in first_info.items()
        if (confirmation := confirmed_info.get(symbol)) is not None
        and _eligibility_reason(raw, symbol) is None
        and _eligibility_reason(confirmation, symbol) is None
    }
    inactive = tuple(sorted(symbol for symbol in tracked if symbol not in eligible))
    klines, cutoff_ms = _daily_klines(snapshot.daily_klines)
    histories = _historical_rows(
        eligible,
        klines,
        cutoff_ms=cutoff_ms,
        policy=policy,
    )
    depth = _depth_metrics(snapshot.liquidity_depth)
    missing_metrics = _LiquidityMetrics(
        depth_sample_count=0,
        thin_depth_10bps=Decimal(),
        thin_depth_50bps=Decimal(),
        book_ticker_sample_count=0,
        spread_q95_bps=Decimal("Infinity"),
    )
    rows: list[_MarketRow] = []
    for history in histories:
        metrics = depth.get(history.symbol, missing_metrics)
        rows.append(
            _MarketRow(
                history=history,
                depth_sample_count=metrics.depth_sample_count,
                thin_depth_10bps=metrics.thin_depth_10bps,
                thin_depth_50bps=metrics.thin_depth_50bps,
                book_ticker_sample_count=metrics.book_ticker_sample_count,
                spread_q95_bps=metrics.spread_q95_bps,
            )
        )
    return rows, inactive


def _historical_rows(
    eligible: dict[str, dict[str, Any]],
    klines: dict[str, list[Any]],
    *,
    cutoff_ms: int,
    policy: RollingPolicy,
) -> list[_HistoricalRow]:
    selection_start_ms = cutoff_ms - policy.liquidity_window_days * DAY_MS
    context_days = policy.market_context_baseline_days + CONFIRMATION_DAYS
    context_start_ms = cutoff_ms - context_days * DAY_MS
    context_expected = tuple(range(context_start_ms, cutoff_ms, DAY_MS))
    evidence_days = max(policy.liquidity_window_days, context_days)
    evidence_start_ms = cutoff_ms - evidence_days * DAY_MS
    evidence_expected_set = set(range(evidence_start_ms, cutoff_ms, DAY_MS))
    rows: list[_HistoricalRow] = []
    for symbol, raw in eligible.items():
        onboard_ms = _positive_int(raw.get("onboardDate"), f"{symbol}.onboardDate")
        bars: dict[int, Any] = {}
        for value in klines.get(symbol, []):
            if not isinstance(value, list) or len(value) < 9:
                raise ValueError(f"invalid daily kline for {symbol}")
            open_ms = _positive_int(value[0], f"{symbol}.openTime", allow_zero=True)
            close_ms = _positive_int(value[6], f"{symbol}.closeTime", allow_zero=True)
            if open_ms in evidence_expected_set and close_ms < cutoff_ms:
                bars[open_ms] = value
        opens = tuple(open_ms for open_ms in sorted(bars) if open_ms >= selection_start_ms)
        volumes = tuple(
            _decimal(bars[open_ms][7], f"{symbol}.quoteVolume", allow_zero=True)
            for open_ms in opens
        )
        trades = tuple(
            _positive_int(bars[open_ms][8], f"{symbol}.trades", allow_zero=True)
            for open_ms in opens
        )
        market_opens = tuple(open_ms for open_ms in context_expected if open_ms in bars)
        market_volumes = tuple(
            _decimal(bars[open_ms][7], f"{symbol}.quoteVolume", allow_zero=True)
            for open_ms in market_opens
        )
        market_trades = tuple(
            _positive_int(bars[open_ms][8], f"{symbol}.trades", allow_zero=True)
            for open_ms in market_opens
        )
        first_full_day = ((onboard_ms + DAY_MS - 1) // DAY_MS) * DAY_MS
        first_probe_day = max(first_full_day, selection_start_ms)
        expected_probe = (
            tuple(range(first_probe_day, cutoff_ms, DAY_MS))
            if first_probe_day < cutoff_ms
            else ()
        )
        observed_probe = sum(open_ms in bars for open_ms in expected_probe)
        if not volumes:
            continue
        rows.append(
            _HistoricalRow(
                symbol=symbol,
                onboard_time_ms=onboard_ms,
                age_days=max(0, (cutoff_ms - onboard_ms) // DAY_MS),
                volumes=volumes,
                trades=trades,
                market_volumes=market_volumes,
                market_trades=market_trades,
                expected_probe_days=len(expected_probe),
                observed_probe_days=observed_probe,
                q25_quote_volume=_percentile(values=volumes, numerator=1, denominator=4),
                q25_trades=_percentile(
                    values=tuple(Decimal(value) for value in trades),
                    numerator=1,
                    denominator=4,
                ),
            )
        )
    return rows


def _mature_pool(rows: list[_MarketRow], policy: RollingPolicy) -> list[_MarketRow]:
    return _rank_market_rows(
        [
            row
            for row in rows
            if row.age_days >= policy.core_minimum_age_days
            and len(row.history.volumes) == policy.liquidity_window_days
            and _complete_market_evidence(row, policy)
        ]
    )


def _probe_pool(rows: list[_MarketRow], policy: RollingPolicy) -> list[_MarketRow]:
    eligible = [
        row
        for row in rows
        if row.history.expected_probe_days >= policy.probe_minimum_complete_days
        and row.history.observed_probe_days == row.history.expected_probe_days
        and _complete_market_evidence(row, policy)
    ]
    recent = [row for row in eligible if row.age_days < policy.core_minimum_age_days]
    fallback = [row for row in eligible if row.age_days >= policy.core_minimum_age_days]
    return [*_rank_market_rows(recent), *_rank_market_rows(fallback)]


def _complete_market_evidence(row: _MarketRow, policy: RollingPolicy) -> bool:
    return (
        row.depth_sample_count == policy.liquidity_depth_samples
        and row.book_ticker_sample_count == policy.liquidity_book_ticker_samples
        and row.spread_q95_bps.is_finite()
        and row.spread_q95_bps >= 0
        and row.thin_depth_10bps.is_finite()
        and row.thin_depth_10bps >= 0
        and row.thin_depth_50bps.is_finite()
        and row.thin_depth_50bps >= 0
    )


def _rank_historical_rows(rows: list[_HistoricalRow]) -> list[_HistoricalRow]:
    return rank_cross_section(
        rows,
        symbol=lambda row: row.symbol,
        metrics=(
            (lambda row: row.q25_quote_volume, True),
            (lambda row: row.q25_trades, True),
        ),
    )


def _rank_market_rows(rows: list[_MarketRow]) -> list[_MarketRow]:
    return rank_cross_section(
        rows,
        symbol=lambda row: row.symbol,
        metrics=(
            (lambda row: row.q25_quote_volume, True),
            (lambda row: row.q25_trades, True),
            (lambda row: row.thin_depth_10bps, True),
            (lambda row: row.thin_depth_50bps, True),
            (lambda row: row.spread_q95_bps, False),
        ),
    )


def _market_context(rows: list[_MarketRow], policy: RollingPolicy) -> MarketContext:
    return evaluate_market_context(
        [
            ActivitySeries(
                quote_volumes=row.history.market_volumes,
                trade_counts=row.history.market_trades,
            )
            for row in rows
        ],
        baseline_days=policy.market_context_baseline_days,
        change_ratio=policy.market_context_change_ratio,
        breadth_ratio=policy.market_context_breadth_ratio,
        minimum_instruments=policy.market_context_minimum_instruments,
    )


def _depth_metrics(
    raw: bytes,
) -> dict[str, _LiquidityMetrics]:
    payload = orjson.loads(raw)
    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), dict):
        raise ValueError("liquidity depth evidence must contain a symbols object")
    book_samples = payload.get("book_tickers")
    if not isinstance(book_samples, list):
        raise ValueError("liquidity depth evidence must contain book_tickers")
    book_spreads: dict[str, list[Decimal]] = {}
    for sample in book_samples:
        if not isinstance(sample, dict) or not isinstance(sample.get("payload"), list):
            raise ValueError("invalid book ticker evidence")
        for ticker in sample["payload"]:
            if not isinstance(ticker, dict) or not isinstance(ticker.get("symbol"), str):
                raise ValueError("invalid book ticker row")
            bid = _decimal(ticker.get("bidPrice"), "bookTicker.bidPrice")
            ask = _decimal(ticker.get("askPrice"), "bookTicker.askPrice")
            midpoint = (bid + ask) / 2
            book_spreads.setdefault(str(ticker["symbol"]), []).append(
                (ask - bid) / midpoint * Decimal(10_000)
            )

    result: dict[str, _LiquidityMetrics] = {}
    for symbol, samples in payload["symbols"].items():
        if not isinstance(symbol, str) or not isinstance(samples, list):
            raise ValueError("invalid liquidity depth evidence")
        thin_depths: list[Decimal] = []
        thin_depths_50bps: list[Decimal] = []
        for sample in samples:
            if not isinstance(sample, dict) or not isinstance(sample.get("payload"), dict):
                raise ValueError(f"invalid depth sample for {symbol}")
            depth = sample["payload"]
            bids = _book_levels(depth, "bids", symbol)
            asks = _book_levels(depth, "asks", symbol)
            best_bid, best_ask = bids[0][0], asks[0][0]
            midpoint = (best_bid + best_ask) / 2
            bid_floor = midpoint * (1 - TEN_BPS)
            ask_ceiling = midpoint * (1 + TEN_BPS)
            bid_depth = sum(
                (price * quantity for price, quantity in bids if price >= bid_floor),
                Decimal(),
            )
            ask_depth = sum(
                (price * quantity for price, quantity in asks if price <= ask_ceiling),
                Decimal(),
            )
            thin_depths.append(min(bid_depth, ask_depth))
            bid_floor_50bps = midpoint * (1 - FIFTY_BPS)
            ask_ceiling_50bps = midpoint * (1 + FIFTY_BPS)
            bid_depth_50bps = sum(
                (
                    price * quantity
                    for price, quantity in bids
                    if price >= bid_floor_50bps
                ),
                Decimal(),
            )
            ask_depth_50bps = sum(
                (
                    price * quantity
                    for price, quantity in asks
                    if price <= ask_ceiling_50bps
                ),
                Decimal(),
            )
            thin_depths_50bps.append(min(bid_depth_50bps, ask_depth_50bps))
        if thin_depths:
            ticker_spreads = book_spreads.get(symbol, [])
            result[symbol] = _LiquidityMetrics(
                depth_sample_count=len(thin_depths),
                thin_depth_10bps=min(thin_depths),
                thin_depth_50bps=min(thin_depths_50bps),
                book_ticker_sample_count=len(ticker_spreads),
                spread_q95_bps=_percentile(
                    values=tuple(ticker_spreads),
                    numerator=19,
                    denominator=20,
                )
                if ticker_spreads
                else Decimal("Infinity"),
            )
    return result


def _book_levels(
    payload: dict[str, Any], key: str, symbol: str
) -> list[tuple[Decimal, Decimal]]:
    values = payload.get(key)
    if not isinstance(values, list) or not values:
        raise ValueError(f"depth sample has no {key} for {symbol}")
    result: list[tuple[Decimal, Decimal]] = []
    for value in values:
        if not isinstance(value, list) or len(value) < 2:
            raise ValueError(f"invalid {key} level for {symbol}")
        result.append(
            (
                _decimal(value[0], f"{symbol}.{key}.price"),
                _decimal(value[1], f"{symbol}.{key}.quantity", allow_zero=True),
            )
        )
    return result


def _reconcile_bucket(
    current: tuple[str, ...],
    preferred: list[str],
    *,
    forbidden: set[str],
    inactive: set[str],
    member_since: dict[str, datetime],
    effective_at: datetime,
    minimum_dwell: timedelta,
    normal_replacement_limit: int,
) -> tuple[str, ...]:
    preferred = list(dict.fromkeys(symbol for symbol in preferred if symbol not in forbidden))
    result = [symbol for symbol in current if symbol not in forbidden and symbol not in inactive]
    forced_vacancies = 5 - len(result)
    for symbol in preferred:
        if len(result) >= 5:
            break
        if symbol not in result:
            result.append(symbol)
    normal_changes = 0
    for symbol in preferred:
        if symbol in result or normal_changes >= normal_replacement_limit:
            continue
        removable = [
            incumbent
            for incumbent in result
            if incumbent not in preferred[:5]
            and _dwell_complete(
                member_since.get(incumbent, effective_at), effective_at, minimum_dwell
            )
        ]
        if not removable:
            continue
        result.remove(removable[-1])
        result.append(symbol)
        normal_changes += 1
    if len(result) != 5:
        raise ValueError("not enough qualified instruments to fill candidate role")
    if forced_vacancies == 0 and len(set(current) - set(result)) > normal_replacement_limit:
        raise ValueError("candidate replacement limit exceeded")
    return tuple(sorted(result))


def _take_first(values: list[str], *, forbidden: set[str]) -> str | None:
    return next((value for value in values if value not in forbidden), None)


def _dwell_complete(joined: datetime, effective: datetime, required: timedelta) -> bool:
    return joined <= effective - required


def _symbol_info(raw: bytes) -> dict[str, dict[str, Any]]:
    payload = orjson.loads(raw)
    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), list):
        raise ValueError("exchangeInfo must contain a symbols array")
    result: dict[str, dict[str, Any]] = {}
    for value in payload["symbols"]:
        if not isinstance(value, dict) or not isinstance(value.get("symbol"), str):
            raise ValueError("exchangeInfo contains an invalid symbol row")
        symbol = str(value["symbol"])
        if symbol in result:
            raise ValueError(f"exchangeInfo contains duplicate symbol: {symbol}")
        result[symbol] = value
    return result


def _daily_klines(raw: bytes) -> tuple[dict[str, list[Any]], int]:
    payload = orjson.loads(raw)
    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), dict):
        raise ValueError("daily kline evidence must contain a symbols object")
    cutoff_ms = _positive_int(
        payload.get("window_end_exclusive_ms"), "dailyKlines.windowEndExclusive"
    )
    if cutoff_ms % DAY_MS:
        raise ValueError("daily kline evidence cutoff must be 00:00 UTC")
    result: dict[str, list[Any]] = {}
    for symbol, value in payload["symbols"].items():
        if (
            not isinstance(symbol, str)
            or not isinstance(value, dict)
            or not isinstance(value.get("payload"), list)
        ):
            raise ValueError("invalid daily kline evidence")
        result[symbol] = value["payload"]
    return result, cutoff_ms


def _eligibility_reason(raw: dict[str, Any], symbol: str) -> str | None:
    if raw.get("status") != "TRADING":
        return "not_trading"
    if raw.get("contractType") != "PERPETUAL":
        return "not_perpetual"
    if raw.get("quoteAsset") != "USDT" or raw.get("marginAsset") != "USDT":
        return "not_usdt"
    if not is_exchange_symbol(symbol):
        return "invalid_symbol"
    return None


def _positive_int(value: object, label: str, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be an integer")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be an integer") from exc
    if parsed < 0 or (parsed == 0 and not allow_zero):
        raise ValueError(f"{label} must be {'non-negative' if allow_zero else 'positive'}")
    return parsed


def _decimal(value: object, label: str, *, allow_zero: bool = False) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{label} must be a decimal") from exc
    if not parsed.is_finite() or parsed < 0 or (parsed == 0 and not allow_zero):
        raise ValueError(f"{label} must be {'non-negative' if allow_zero else 'positive'}")
    return parsed


def _median(values: tuple[Decimal, ...]) -> Decimal:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2


def _percentile(
    *, values: tuple[Decimal, ...], numerator: int, denominator: int
) -> Decimal:
    ordered = sorted(values)
    position_numerator = (len(ordered) - 1) * numerator
    lower = position_numerator // denominator
    remainder = position_numerator % denominator
    if remainder == 0:
        return ordered[lower]
    upper = lower + 1
    return (
        ordered[lower] * (denominator - remainder) + ordered[upper] * remainder
    ) / denominator
