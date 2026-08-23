from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from miry.contracts.serde import sha256_bytes
from miry.universe.regime import MarketContext


@dataclass(frozen=True, slots=True)
class RollingPolicy:
    liquidity_window_days: int = 14
    probe_minimum_complete_days: int = 7
    market_context_baseline_days: int = 28
    market_context_change_ratio: Decimal = Decimal("1.25")
    market_context_breadth_ratio: Decimal = Decimal("0.70")
    market_context_minimum_instruments: int = 60
    liquidity_depth_samples: int = 3
    liquidity_book_ticker_samples: int = 21
    depth_mature_candidate_count: int = 200
    depth_probe_candidate_count: int = 100
    candidate_minimum_dwell_hours: int = 48
    core_minimum_dwell_days: int = 14
    candidate_daily_replacements: int = 2
    core_weekly_replacements: int = 5
    core_minimum_age_days: int = 30
    core_entry_rank: int = 45
    core_retain_rank: int = 55
    boundary_retain_rank: int = 10
    mature_pool_warning_size: int = 65


@dataclass(frozen=True, slots=True)
class DiscoverySnapshot:
    observed_at: datetime
    exchange_info: bytes
    exchange_info_confirmation: bytes
    market_tickers: bytes
    daily_klines: bytes
    liquidity_depth: bytes

    @property
    def source_hashes(self) -> tuple[str, ...]:
        return tuple(
            sha256_bytes(value)
            for value in (
                self.exchange_info,
                self.exchange_info_confirmation,
                self.market_tickers,
                self.daily_klines,
                self.liquidity_depth,
            )
        )


@dataclass(frozen=True, slots=True)
class SelectionResult:
    core: tuple[str, ...]
    boundary: tuple[str, ...]
    probe: tuple[str, ...]
    inactive: tuple[str, ...]
    source_hashes: tuple[str, ...]
    mature_pool_count: int
    probe_pool_count: int
    market_context: MarketContext | None = None
    decision_frozen_reason: str | None = None
