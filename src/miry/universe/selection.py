from __future__ import annotations

import gzip
from datetime import datetime, timedelta
from pathlib import Path

from miry.contracts.models import UniverseDecision
from miry.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
)
from miry.universe.evidence import (
    _complete_market_evidence,
    _daily_klines,
    _dwell_complete,
    _eligibility_reason,
    _historical_rows,
    _market_context,
    _market_rows,
    _MarketRow,
    _mature_pool,
    _probe_pool,
    _rank_historical_rows,
    _reconcile_bucket,
    _symbol_info,
    _take_first,
)
from miry.universe.models import DiscoverySnapshot, RollingPolicy, SelectionResult
from miry.universe.regime import (
    MarketContext,
    MarketState,
)


def select_bootstrap_universe(
    snapshot: DiscoverySnapshot,
    *,
    policy: RollingPolicy,
) -> SelectionResult:
    rows, inactive = _market_rows(snapshot, tracked=(), policy=policy)
    return _bootstrap_result(rows, inactive, snapshot, policy)


def _bootstrap_result(
    rows: list[_MarketRow],
    inactive: tuple[str, ...],
    snapshot: DiscoverySnapshot,
    policy: RollingPolicy,
) -> SelectionResult:
    mature_pool = _mature_pool(rows, policy)
    if len(mature_pool) < 55:
        raise ValueError(f"only {len(mature_pool)} mature candidates have complete evidence")
    core = tuple(sorted(row.symbol for row in mature_pool[:50]))
    boundary = tuple(sorted(row.symbol for row in mature_pool[50:55]))
    stable = set((*core, *boundary))
    probe_pool = [row for row in _probe_pool(rows, policy) if row.symbol not in stable]
    if len(probe_pool) < 5:
        raise ValueError(
            f"only {len(probe_pool)} probe candidates remain after reserving stable roles"
        )
    probe = tuple(sorted(row.symbol for row in probe_pool[:5]))
    return SelectionResult(
        core,
        boundary,
        probe,
        inactive,
        snapshot.source_hashes,
        len(mature_pool),
        len(probe_pool),
    )


def validate_bootstrap_universe(
    snapshot: DiscoverySnapshot,
    *,
    core: tuple[str, ...],
    boundary: tuple[str, ...],
    probe: tuple[str, ...],
    policy: RollingPolicy,
) -> SelectionResult:
    rows, inactive = _market_rows(
        snapshot,
        tracked=tuple(sorted((*core, *boundary, *probe))),
        policy=policy,
    )
    mature_symbols = {row.symbol for row in _mature_pool(rows, policy)}
    probe_symbols = {row.symbol for row in _probe_pool(rows, policy)}
    missing_mature = sorted(set((*core, *boundary)) - mature_symbols)
    missing_probe = sorted(set(probe) - probe_symbols)
    if missing_mature or missing_probe:
        raise ValueError(
            "configured bootstrap members lack complete cross-sectional evidence: "
            f"mature={missing_mature} probe={missing_probe}"
        )
    expected = _bootstrap_result(rows, inactive, snapshot, policy)
    configured = (tuple(sorted(core)), tuple(sorted(boundary)), tuple(sorted(probe)))
    selected = (expected.core, expected.boundary, expected.probe)
    if configured != selected:
        mismatched = tuple(
            role
            for role, actual, wanted in zip(
                ("core", "boundary", "probe"), configured, selected, strict=True
            )
            if actual != wanted
        )
        raise ValueError(
            "configured bootstrap members do not match deterministic role allocation: "
            + ",".join(mismatched)
        )
    return expected


def liquidity_validation_symbols(
    exchange_info: bytes,
    daily_klines: bytes,
    *,
    tracked: tuple[str, ...] = (),
    policy: RollingPolicy,
) -> tuple[str, ...]:
    info = _symbol_info(exchange_info)
    eligible = {
        symbol: raw
        for symbol, raw in info.items()
        if _eligibility_reason(raw, symbol) is None
    }
    klines, cutoff_ms = _daily_klines(daily_klines)
    histories = _historical_rows(
        eligible,
        klines,
        cutoff_ms=cutoff_ms,
        policy=policy,
    )
    mature = _rank_historical_rows(
        [
            row
            for row in histories
            if row.age_days >= policy.core_minimum_age_days
            and len(row.volumes) == policy.liquidity_window_days
        ]
    )[: policy.depth_mature_candidate_count]
    probes = sorted(
        (
            row
            for row in histories
            if row.expected_probe_days >= policy.probe_minimum_complete_days
            and row.observed_probe_days == row.expected_probe_days
        ),
        key=lambda row: (
            row.age_days >= policy.core_minimum_age_days,
            -row.onboard_time_ms,
            row.symbol,
        ),
    )[: policy.depth_probe_candidate_count]
    return tuple(sorted({*tracked, *(row.symbol for row in (*mature, *probes))}))


def select_rolling_universe(
    active: UniverseDecision,
    snapshot: DiscoverySnapshot,
    *,
    effective_at: datetime,
    member_since: dict[str, datetime],
    core_since: dict[str, datetime],
    policy: RollingPolicy,
) -> SelectionResult:
    rows, inactive = _market_rows(snapshot, tracked=active.members, policy=policy)
    mature_pool = _mature_pool(rows, policy)
    mature_rank = {row.symbol: index for index, row in enumerate(mature_pool, start=1)}
    probe_pool = _probe_pool(rows, policy)
    market_context = _market_context(rows, policy)
    inactive_set = set(inactive)
    core_or_boundary = set((*active.core, *active.boundary))
    active_probe = set(active.probe)
    rows_by_symbol = {row.symbol: row for row in rows}
    incomplete_active = sorted(
        symbol
        for symbol in active.members
        if symbol not in inactive_set
        and (
            (row := rows_by_symbol.get(symbol)) is None
            or not _complete_market_evidence(row, policy)
            or (
                symbol in core_or_boundary
                and (
                    row.age_days < policy.core_minimum_age_days
                    or len(row.history.volumes) != policy.liquidity_window_days
                )
            )
            or (
                symbol in active_probe
                and (
                    row.history.expected_probe_days < policy.probe_minimum_complete_days
                    or row.history.observed_probe_days != row.history.expected_probe_days
                )
            )
        )
    )
    if incomplete_active:
        return _preserve_active(
            active,
            snapshot,
            inactive,
            mature_pool_count=len(mature_pool),
            probe_pool_count=len(probe_pool),
            market_context=market_context,
            reason="active_member_evidence_incomplete:" + ",".join(incomplete_active),
        )
    if len(mature_pool) < 55 or len(probe_pool) < 5:
        return _preserve_active(
            active,
            snapshot,
            inactive,
            mature_pool_count=len(mature_pool),
            probe_pool_count=len(probe_pool),
            market_context=market_context,
            reason="cross_sectional_reserve_incomplete",
        )

    freeze_reason: str | None = None
    allow_normal_changes = True
    if market_context.panel_count < policy.market_context_minimum_instruments:
        freeze_reason = "market_context_panel_incomplete"
        allow_normal_changes = False
    elif market_context.state == MarketState.SHOCK_PENDING:
        freeze_reason = "market_activity_shock_pending"
        allow_normal_changes = False
    if not allow_normal_changes and not inactive:
        return _preserve_active(
            active,
            snapshot,
            inactive,
            mature_pool_count=len(mature_pool),
            probe_pool_count=len(probe_pool),
            market_context=market_context,
            reason=freeze_reason,
        )

    core = list(active.core)
    replacement_pool = [row.symbol for row in mature_pool if row.symbol not in core]
    for symbol in (symbol for symbol in core if symbol in inactive):
        replacement = _take_first(replacement_pool, forbidden=set(core))
        if replacement is None:
            raise ValueError("not enough mature instruments to replace inactive core")
        core[core.index(symbol)] = replacement

    if allow_normal_changes and effective_at.weekday() == 0:
        changes = len(set(active.core) - set(core))
        promotable = [
            symbol
            for symbol in replacement_pool
            if symbol not in core
            and mature_rank.get(symbol, 10**9) <= policy.core_entry_rank
        ]
        while promotable and changes < policy.core_weekly_replacements:
            challenger = promotable.pop(0)
            removable = [
                symbol
                for symbol in core
                if mature_rank.get(symbol, 10**9) > policy.core_retain_rank
                and _dwell_complete(
                    core_since.get(symbol, active.effective_at),
                    effective_at,
                    timedelta(days=policy.core_minimum_dwell_days),
                )
            ]
            if not removable:
                break
            incumbent = max(removable, key=lambda symbol: mature_rank.get(symbol, 10**9))
            if mature_rank.get(challenger, 10**9) >= mature_rank.get(incumbent, 10**9):
                break
            core[core.index(incumbent)] = challenger
            changes += 1

    core_set = set(core)
    probe_preferred = [
        row.symbol
        for row in probe_pool
        if row.symbol not in core_set and row.symbol not in core_or_boundary
    ]
    if len(probe_preferred) < 5:
        return _preserve_active(
            active,
            snapshot,
            inactive,
            mature_pool_count=len(mature_pool),
            probe_pool_count=len(probe_preferred),
            market_context=market_context,
            reason="probe_reserve_incomplete",
        )
    probe = _reconcile_bucket(
        active.probe,
        probe_preferred,
        forbidden=core_set,
        inactive=set(inactive),
        member_since=member_since,
        effective_at=effective_at,
        minimum_dwell=timedelta(hours=policy.candidate_minimum_dwell_hours),
        normal_replacement_limit=1 if allow_normal_changes else 0,
    )

    probe_set = set(probe)
    boundary_ranked = [
        row.symbol
        for row in mature_pool
        if row.symbol not in core_set and row.symbol not in probe_set
    ]
    if len(boundary_ranked) < 5:
        return _preserve_active(
            active,
            snapshot,
            inactive,
            mature_pool_count=len(mature_pool),
            probe_pool_count=len(probe_preferred),
            market_context=market_context,
            reason="boundary_reserve_incomplete",
        )
    boundary_rank = {
        symbol: index for index, symbol in enumerate(boundary_ranked, start=1)
    }
    protected_boundary = sorted(
        (
            symbol
            for symbol in active.boundary
            if symbol not in inactive
            and symbol not in core_set
            and symbol not in probe_set
            and boundary_rank.get(symbol, 10**9) <= policy.boundary_retain_rank
        ),
        key=lambda symbol: boundary_rank[symbol],
    )
    boundary = _reconcile_bucket(
        active.boundary,
        [*protected_boundary, *boundary_ranked],
        forbidden=core_set | probe_set,
        inactive=set(inactive),
        member_since=member_since,
        effective_at=effective_at,
        minimum_dwell=timedelta(hours=policy.candidate_minimum_dwell_hours),
        normal_replacement_limit=(
            max(1, policy.candidate_daily_replacements - 1) if allow_normal_changes else 0
        ),
    )
    return SelectionResult(
        core=tuple(sorted(core)),
        boundary=tuple(sorted(boundary)),
        probe=tuple(sorted(probe)),
        inactive=tuple(sorted(inactive)),
        source_hashes=snapshot.source_hashes,
        mature_pool_count=len(mature_pool),
        probe_pool_count=len(probe_preferred),
        market_context=market_context,
        decision_frozen_reason=freeze_reason,
    )


def _preserve_active(
    active: UniverseDecision,
    snapshot: DiscoverySnapshot,
    inactive: tuple[str, ...],
    *,
    mature_pool_count: int,
    probe_pool_count: int,
    market_context: MarketContext,
    reason: str | None,
) -> SelectionResult:
    return SelectionResult(
        core=active.core,
        boundary=active.boundary,
        probe=active.probe,
        inactive=tuple(sorted(inactive)),
        source_hashes=snapshot.source_hashes,
        mature_pool_count=mature_pool_count,
        probe_pool_count=probe_pool_count,
        market_context=market_context,
        decision_frozen_reason=reason,
    )


def write_formal_bundle(
    decision: UniverseDecision,
    output_dir: Path,
    *,
    snapshot: DiscoverySnapshot,
) -> None:
    atomic_write_bytes(output_dir / "decision.json", canonical_json_bytes(decision), mode=0o644)
    atomic_write_bytes(
        output_dir / "formal-60.members.txt",
        ("\n".join(decision.members) + "\n").encode("utf-8"),
        mode=0o644,
    )
    for name, content in (
        ("exchange-info.json.gz", snapshot.exchange_info),
        ("exchange-info-confirmation.json.gz", snapshot.exchange_info_confirmation),
        ("market-tickers.json.gz", snapshot.market_tickers),
        ("daily-klines.json.gz", snapshot.daily_klines),
        ("liquidity-depth.json.gz", snapshot.liquidity_depth),
    ):
        atomic_write_bytes(
            output_dir / "sources" / name,
            gzip.compress(content, mtime=0),
            mode=0o644,
        )
