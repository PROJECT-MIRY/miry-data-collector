from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from universe_fixtures import formal_roles, liquidity_snapshot, symbols

from miry.contracts.models import UniverseDecision, UniverseDecisionReason
from miry.contracts.serde import universe_hash
from miry.universe.evidence import _depth_metrics
from miry.universe.models import RollingPolicy
from miry.universe.ranking import rank_cross_section
from miry.universe.regime import MarketState
from miry.universe.selection import (
    select_bootstrap_universe,
    select_rolling_universe,
    validate_bootstrap_universe,
    write_formal_bundle,
)


def test_bootstrap_selects_fifty_five_five_from_complete_evidence() -> None:
    observed = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)

    result = select_bootstrap_universe(
        liquidity_snapshot(observed), policy=RollingPolicy()
    )

    assert (result.core, result.boundary, result.probe) == formal_roles()
    assert len(set((*result.core, *result.boundary, *result.probe))) == 60
    assert len(result.source_hashes) == 5


def test_formal_bundle_writes_chinese_canonical_symbol_as_utf8(tmp_path: Path) -> None:
    observed = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)
    core, boundary, probe = formal_roles()
    core = tuple(sorted(("币安人生USDT", *core[1:])))
    decision = _decision(core, boundary, probe, observed)

    write_formal_bundle(decision, tmp_path, snapshot=liquidity_snapshot(observed))

    members = (tmp_path / "formal-60.members.txt").read_text(encoding="utf-8").splitlines()
    assert "币安人生USDT" in members


def test_bootstrap_replaces_recent_candidate_without_complete_history() -> None:
    observed = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)

    result = select_bootstrap_universe(
        liquidity_snapshot(observed, incomplete=frozenset({"S069USDT"})),
        policy=RollingPolicy(),
    )

    assert "S069USDT" not in result.probe
    assert "S070USDT" in result.probe


def test_bootstrap_reserves_top_mature_ranks_before_probe_fallback() -> None:
    observed = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)

    result = select_bootstrap_universe(
        liquidity_snapshot(observed, recent_count=2),
        policy=RollingPolicy(),
    )

    assert result.core == symbols(0, 50)
    assert result.boundary == symbols(50, 55)
    assert result.probe == tuple(sorted((*symbols(55, 58), *symbols(73, 75))))


def test_bootstrap_validation_rejects_probe_first_role_assignment() -> None:
    observed = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)
    snapshot = liquidity_snapshot(observed, recent_count=2)

    with pytest.raises(ValueError, match="deterministic role allocation"):
        validate_bootstrap_universe(
            snapshot,
            core=symbols(3, 53),
            boundary=symbols(53, 58),
            probe=tuple(sorted((*symbols(0, 3), *symbols(73, 75)))),
            policy=RollingPolicy(),
        )


def test_low_and_volatile_activity_are_ranked_instead_of_rejected() -> None:
    observed = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)

    result = select_bootstrap_universe(
        liquidity_snapshot(
            observed,
            low_activity=frozenset({"S064USDT"}),
            volatile_activity=frozenset({"S000USDT"}),
        ),
        policy=RollingPolicy(),
    )

    assert result.mature_pool_count == 65
    assert "S000USDT" in result.core


def test_wide_spread_and_shallow_depth_are_ranked_instead_of_rejected() -> None:
    observed = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)

    result = select_bootstrap_universe(
        liquidity_snapshot(observed, weak_market=frozenset({"S000USDT"})),
        policy=RollingPolicy(),
    )

    assert result.mature_pool_count == 65
    assert "S000USDT" not in result.core


def test_spread_q95_ignores_one_extreme_book_ticker_sample() -> None:
    observed = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)
    snapshot = liquidity_snapshot(
        observed,
        spread_outlier=frozenset({"S000USDT"}),
    )

    metrics = _depth_metrics(snapshot.liquidity_depth)

    assert metrics["S000USDT"].spread_q95_bps == Decimal("2")


def test_monday_core_rotation_uses_robust_rank_and_hysteresis() -> None:
    effective = datetime(2026, 8, 17, tzinfo=UTC)
    active = _decision(
        tuple(sorted((*symbols(1, 50), "S064USDT"))),
        symbols(50, 55),
        symbols(65, 70),
        effective - timedelta(days=30),
    )

    result = select_rolling_universe(
        active,
        liquidity_snapshot(effective - timedelta(minutes=10)),
        effective_at=effective,
        member_since={symbol: effective - timedelta(days=30) for symbol in active.members},
        core_since={symbol: effective - timedelta(days=30) for symbol in active.core},
        policy=RollingPolicy(),
    )

    assert "S000USDT" in result.core
    assert "S064USDT" not in result.core


def test_rolling_probe_fallback_does_not_consume_stable_roles() -> None:
    effective = datetime(2026, 8, 18, tzinfo=UTC)
    active = _decision(
        symbols(0, 50),
        symbols(50, 55),
        tuple(sorted((*symbols(60, 63), *symbols(73, 75)))),
        effective - timedelta(days=30),
    )

    result = select_rolling_universe(
        active,
        liquidity_snapshot(effective - timedelta(minutes=10), recent_count=2),
        effective_at=effective,
        member_since={symbol: effective - timedelta(days=30) for symbol in active.members},
        core_since={symbol: effective - timedelta(days=30) for symbol in active.core},
        policy=RollingPolicy(),
    )

    assert set(result.probe).isdisjoint((*active.core, *active.boundary))
    assert "S055USDT" in result.probe


def test_boundary_hysteresis_uses_candidate_relative_top_ten() -> None:
    effective = datetime(2026, 8, 18, tzinfo=UTC)
    active = _decision(
        symbols(0, 50),
        symbols(56, 61),
        symbols(65, 70),
        effective - timedelta(days=30),
    )

    result = select_rolling_universe(
        active,
        liquidity_snapshot(effective - timedelta(minutes=10)),
        effective_at=effective,
        member_since={symbol: effective - timedelta(days=3) for symbol in active.members},
        core_since={symbol: effective - timedelta(days=30) for symbol in active.core},
        policy=RollingPolicy(),
    )

    assert set(symbols(56, 60)).issubset(result.boundary)
    assert "S060USDT" not in result.boundary
    assert len(set(active.boundary) - set(result.boundary)) == 1


def test_rolling_selection_freezes_when_active_evidence_is_incomplete() -> None:
    effective = datetime(2026, 8, 18, tzinfo=UTC)
    core, boundary, probe = formal_roles()
    active = _decision(core, boundary, probe, effective - timedelta(days=30))

    result = select_rolling_universe(
        active,
        liquidity_snapshot(
            effective - timedelta(minutes=10),
            missing_depth=frozenset({"S000USDT"}),
        ),
        effective_at=effective,
        member_since={symbol: effective - timedelta(days=30) for symbol in active.members},
        core_since={symbol: effective - timedelta(days=30) for symbol in active.core},
        policy=RollingPolicy(),
    )

    assert (result.core, result.boundary, result.probe) == (
        active.core,
        active.boundary,
        active.probe,
    )
    assert result.inactive == ()
    assert result.mature_pool_count == 64
    assert result.decision_frozen_reason == "active_member_evidence_incomplete:S000USDT"


@pytest.mark.parametrize("activity_days", [1, 3])
def test_broad_activity_shock_freezes_normal_rotation(activity_days: int) -> None:
    effective = datetime(2026, 8, 17, tzinfo=UTC)
    active = _decision(
        tuple(sorted((*symbols(1, 50), "S064USDT"))),
        symbols(50, 55),
        symbols(65, 70),
        effective - timedelta(days=30),
    )

    result = select_rolling_universe(
        active,
        liquidity_snapshot(
            effective - timedelta(minutes=10),
            activity_factor=2,
            activity_days=activity_days,
        ),
        effective_at=effective,
        member_since={symbol: effective - timedelta(days=30) for symbol in active.members},
        core_since={symbol: effective - timedelta(days=30) for symbol in active.core},
        policy=RollingPolicy(),
    )

    assert result.core == active.core
    assert result.market_context is not None
    assert result.market_context.state == MarketState.SHOCK_PENDING
    assert result.market_context.horizon_days == activity_days
    assert result.decision_frozen_reason == "market_activity_shock_pending"


def test_seven_day_activity_shift_allows_normal_rotation() -> None:
    effective = datetime(2026, 8, 17, tzinfo=UTC)
    active = _decision(
        tuple(sorted((*symbols(1, 50), "S064USDT"))),
        symbols(50, 55),
        symbols(65, 70),
        effective - timedelta(days=30),
    )

    result = select_rolling_universe(
        active,
        liquidity_snapshot(
            effective - timedelta(minutes=10), activity_factor=2, activity_days=7
        ),
        effective_at=effective,
        member_since={symbol: effective - timedelta(days=30) for symbol in active.members},
        core_since={symbol: effective - timedelta(days=30) for symbol in active.core},
        policy=RollingPolicy(),
    )

    assert "S000USDT" in result.core
    assert result.market_context is not None
    assert result.market_context.state == MarketState.SHIFT_CONFIRMED
    assert result.decision_frozen_reason is None


def test_inactive_member_is_replaced_during_activity_shock() -> None:
    effective = datetime(2026, 8, 18, tzinfo=UTC)
    core, boundary, probe = formal_roles()
    active = _decision(core, boundary, probe, effective - timedelta(days=30))

    result = select_rolling_universe(
        active,
        liquidity_snapshot(
            effective - timedelta(minutes=10),
            inactive=boundary[0],
            activity_factor=2,
            activity_days=3,
        ),
        effective_at=effective,
        member_since={symbol: effective - timedelta(days=30) for symbol in active.members},
        core_since={symbol: effective - timedelta(days=30) for symbol in active.core},
        policy=RollingPolicy(),
    )

    assert boundary[0] not in (*result.core, *result.boundary, *result.probe)
    assert result.decision_frozen_reason == "market_activity_shock_pending"


@dataclass(frozen=True)
class _RankRow:
    symbol: str
    values: tuple[Decimal, ...]


def test_cross_section_protects_weakest_dimension_and_is_deterministic() -> None:
    rows = [
        _RankRow("A", tuple(map(Decimal, (100, 100, 100, 100, 1)))),
        _RankRow("B", tuple(map(Decimal, (80, 80, 80, 80, 80)))),
        _RankRow("C", tuple(map(Decimal, (70, 70, 70, 70, 70)))),
        _RankRow("D", tuple(map(Decimal, (60, 60, 60, 60, 60)))),
        _RankRow("E", tuple(map(Decimal, (50, 50, 50, 50, 50)))),
    ]
    metrics = tuple(
        (lambda row, index=index: row.values[index], True) for index in range(5)
    )

    forward = rank_cross_section(rows, symbol=lambda row: row.symbol, metrics=metrics)
    reverse = rank_cross_section(
        list(reversed(rows)), symbol=lambda row: row.symbol, metrics=metrics
    )

    assert forward[0].symbol == "B"
    assert [row.symbol for row in forward] == [row.symbol for row in reverse]


def _decision(
    core: tuple[str, ...],
    boundary: tuple[str, ...],
    probe: tuple[str, ...],
    effective_at: datetime,
) -> UniverseDecision:
    return UniverseDecision(
        core_generation=1,
        candidate_revision=0,
        decision_sequence=1,
        universe_version="1.0",
        created_at=effective_at,
        effective_at=effective_at,
        reason=UniverseDecisionReason.FORMAL_BOOTSTRAP,
        core=tuple(sorted(core)),
        boundary=tuple(sorted(boundary)),
        probe=tuple(sorted(probe)),
        universe_hash=universe_hash(core, boundary, probe),
    )
