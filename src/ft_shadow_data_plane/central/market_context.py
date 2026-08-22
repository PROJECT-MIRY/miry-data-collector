from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

PENDING_DAYS = 3
CONFIRMATION_DAYS = 7


class MarketState(StrEnum):
    NORMAL = "NORMAL"
    SHOCK_PENDING = "ACTIVITY_SHOCK_PENDING"
    SHIFT_CONFIRMED = "ACTIVITY_SHIFT_CONFIRMED"


@dataclass(frozen=True, slots=True)
class MarketContext:
    state: MarketState
    horizon_days: int
    panel_count: int
    quote_volume_factor: Decimal
    trade_count_factor: Decimal
    quote_volume_breadth: Decimal
    trade_count_breadth: Decimal


@dataclass(frozen=True, slots=True)
class ActivitySeries:
    quote_volumes: tuple[Decimal, ...]
    trade_counts: tuple[int, ...]


def evaluate_market_context(
    series: list[ActivitySeries],
    *,
    baseline_days: int,
    change_ratio: Decimal,
    breadth_ratio: Decimal,
    minimum_instruments: int,
) -> MarketContext:
    expected_days = baseline_days + CONFIRMATION_DAYS
    panel = [
        row
        for row in series
        if len(row.quote_volumes) == expected_days
        and len(row.trade_counts) == expected_days
        and _median(row.quote_volumes[:baseline_days]) > 0
        and _median(tuple(Decimal(value) for value in row.trade_counts[:baseline_days])) > 0
    ]
    if len(panel) < minimum_instruments:
        return MarketContext(
            state=MarketState.NORMAL,
            horizon_days=0,
            panel_count=len(panel),
            quote_volume_factor=Decimal(1),
            trade_count_factor=Decimal(1),
            quote_volume_breadth=Decimal(),
            trade_count_breadth=Decimal(),
        )
    contexts = {
        horizon: _evaluate_horizon(panel, horizon, baseline_days, change_ratio)
        for horizon in (1, PENDING_DAYS, CONFIRMATION_DAYS)
    }
    confirmed = contexts[CONFIRMATION_DAYS]
    if _is_broad_move(confirmed, change_ratio, breadth_ratio):
        return _with_state(confirmed, MarketState.SHIFT_CONFIRMED)
    for horizon in (PENDING_DAYS, 1):
        context = contexts[horizon]
        if _is_broad_move(context, change_ratio, breadth_ratio):
            return _with_state(context, MarketState.SHOCK_PENDING)
    return contexts[1]


def _evaluate_horizon(
    rows: list[ActivitySeries],
    horizon: int,
    baseline_days: int,
    change_ratio: Decimal,
) -> MarketContext:
    volume_factors: list[Decimal] = []
    trade_factors: list[Decimal] = []
    for row in rows:
        baseline_volume = _median(row.quote_volumes[:baseline_days])
        recent_volume = _median(row.quote_volumes[-horizon:])
        baseline_trades = _median(
            tuple(Decimal(value) for value in row.trade_counts[:baseline_days])
        )
        recent_trades = _median(tuple(Decimal(value) for value in row.trade_counts[-horizon:]))
        volume_factors.append(recent_volume / baseline_volume)
        trade_factors.append(recent_trades / baseline_trades)
    volume_factor = _median(tuple(volume_factors))
    trade_factor = _median(tuple(trade_factors))
    upward = volume_factor >= change_ratio
    threshold = change_ratio if upward else Decimal(1) / change_ratio
    return MarketContext(
        state=MarketState.NORMAL,
        horizon_days=horizon,
        panel_count=len(rows),
        quote_volume_factor=volume_factor,
        trade_count_factor=trade_factor,
        quote_volume_breadth=_directional_breadth(
            volume_factors, threshold, upward=upward
        ),
        trade_count_breadth=_directional_breadth(trade_factors, threshold, upward=upward),
    )


def _directional_breadth(
    values: list[Decimal], threshold: Decimal, *, upward: bool
) -> Decimal:
    matching = (
        sum(value >= threshold for value in values)
        if upward
        else sum(value <= threshold for value in values)
    )
    return Decimal(matching) / len(values)


def _is_broad_move(
    context: MarketContext, change_ratio: Decimal, breadth_ratio: Decimal
) -> bool:
    upward = (
        context.quote_volume_factor >= change_ratio
        and context.trade_count_factor >= change_ratio
    )
    downward_limit = Decimal(1) / change_ratio
    downward = (
        context.quote_volume_factor <= downward_limit
        and context.trade_count_factor <= downward_limit
    )
    return (
        (upward or downward)
        and context.quote_volume_breadth >= breadth_ratio
        and context.trade_count_breadth >= breadth_ratio
    )


def _with_state(context: MarketContext, state: MarketState) -> MarketContext:
    return MarketContext(
        state=state,
        horizon_days=context.horizon_days,
        panel_count=context.panel_count,
        quote_volume_factor=context.quote_volume_factor,
        trade_count_factor=context.trade_count_factor,
        quote_volume_breadth=context.quote_volume_breadth,
        trade_count_breadth=context.trade_count_breadth,
    )


def _median(values: tuple[Decimal, ...]) -> Decimal:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2
