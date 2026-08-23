from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

type Metric[Row] = tuple[Callable[[Row], Decimal], bool]


def rank_cross_section[Row](
    rows: list[Row],
    *,
    symbol: Callable[[Row], str],
    metrics: tuple[Metric[Row], ...],
) -> list[Row]:
    """Rank by weakest metric first, then aggregate rank, with deterministic ties."""
    metric_ranks = tuple(
        _metric_ranks(
            {symbol(row): value(row) for row in rows},
            descending=descending,
        )
        for value, descending in metrics
    )
    return sorted(
        rows,
        key=lambda row: _aggregate_key(
            tuple(ranks[symbol(row)] for ranks in metric_ranks), symbol(row)
        ),
    )


def _metric_ranks(values: dict[str, Decimal], *, descending: bool) -> dict[str, int]:
    ordered = sorted(
        values.items(),
        key=lambda item: ((-item[1] if descending else item[1]), item[0]),
    )
    ranks: dict[str, int] = {}
    previous: Decimal | None = None
    rank = 0
    for position, (symbol, value) in enumerate(ordered, start=1):
        if previous is None or value != previous:
            rank = position
            previous = value
        ranks[symbol] = rank
    return ranks


def _aggregate_key(ranks: tuple[int, ...], symbol: str) -> tuple[object, ...]:
    return (max(ranks), sum(ranks), ranks, symbol)
