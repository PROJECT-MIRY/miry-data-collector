from __future__ import annotations

import gzip
import logging
from datetime import UTC, datetime, time, timedelta
from pathlib import Path

import orjson

from ft_shadow_data_plane.central.selector import (
    DiscoverySnapshot,
    SelectionResult,
    select_rolling_universe,
    validate_bootstrap_universe,
)
from ft_shadow_data_plane.contracts.models import (
    CandidateOverride,
    UniverseDecision,
    UniverseDecisionReason,
)
from ft_shadow_data_plane.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    universe_hash,
)
from ft_shadow_data_plane.edge.config import UniversePolicyConfig

logger = logging.getLogger(__name__)


class UniverseStore:
    def __init__(self, data_root: Path, policy: UniversePolicyConfig) -> None:
        self._root = data_root / "control" / "universe"
        self._active_path = self._root / "active.json"
        self._pending_path = self._root / "pending.json"
        self._state_path = self._root / "membership-state.json"
        self._observations = self._root / "observations"
        self._decisions = self._root / "decisions"
        self._evaluations = self._root / "evaluations"
        self._overrides = self._root / "candidate-overrides"
        self._policy = policy
        self._active: UniverseDecision | None = None
        self._member_since: dict[str, datetime] = {}
        self._core_since: dict[str, datetime] = {}

    @property
    def active(self) -> UniverseDecision:
        if self._active is None:
            raise RuntimeError("universe store has not been initialized")
        return self._active

    def initialize(self, now: datetime | None = None) -> UniverseDecision:
        now = now or datetime.now(UTC)
        for path in (
            self._observations,
            self._decisions,
            self._evaluations,
            self._overrides,
        ):
            path.mkdir(parents=True, exist_ok=True)
        if self._active_path.exists():
            self._active = UniverseDecision.model_validate_json(self._active_path.read_bytes())
            self._load_state()
            return self._active
        policy = self._policy
        self._active = UniverseDecision(
            core_generation=policy.core_generation,
            candidate_revision=policy.candidate_revision,
            decision_sequence=policy.decision_sequence,
            universe_version=f"{policy.core_generation}.{policy.candidate_revision}",
            created_at=now,
            effective_at=now,
            reason=UniverseDecisionReason.FORMAL_BOOTSTRAP,
            core=policy.core,
            boundary=policy.boundary,
            probe=policy.probe,
            source_hashes=(policy.bootstrap_evidence_sha256,),
            universe_hash=universe_hash(policy.core, policy.boundary, policy.probe),
        )
        self._member_since = {symbol: now for symbol in self._active.members}
        self._core_since = {symbol: now for symbol in self._active.core}
        self._write_active(self._active)
        self._write_state()
        return self._active

    def observe_and_plan(
        self,
        snapshot: DiscoverySnapshot,
        *,
        now: datetime | None = None,
    ) -> UniverseDecision | None:
        now = now or snapshot.observed_at
        self._write_observation(snapshot)
        effective_at = _next_effective_at(now, self._policy.decision_cutoff_minute_utc)
        formal_start = self._root.parent / "formal-start.json"
        if not formal_start.exists():
            verified = validate_bootstrap_universe(
                snapshot,
                core=self.active.core,
                boundary=self.active.boundary,
                probe=self.active.probe,
                policy=self._policy.rolling_policy(),
            )
            self._active = self.active.model_copy(
                update={
                    "source_hashes": (
                        self._policy.bootstrap_evidence_sha256,
                        *snapshot.source_hashes,
                    )
                }
            )
            self._write_active(self.active)
            self._write_evaluation(verified, now=now, effective_at=now, kind="bootstrap")
            return None

        result = select_rolling_universe(
            self.active,
            snapshot,
            effective_at=effective_at,
            member_since=self._member_since,
            core_since=self._core_since,
            policy=self._policy.rolling_policy(),
        )
        self._write_evaluation(result, now=now, effective_at=effective_at, kind="rolling")
        if not self._policy.automation_enabled:
            self._pending_path.unlink(missing_ok=True)
            logger.info("automatic universe decisions are paused by configuration")
            return None
        reason = _decision_reason(self.active, result, effective_at)
        if (
            result.core == self.active.core
            and result.boundary == self.active.boundary
            and result.probe == self.active.probe
        ):
            self._pending_path.unlink(missing_ok=True)
            return None
        core_generation, candidate_revision = _next_version(self.active, result.core)
        decision = UniverseDecision(
            core_generation=core_generation,
            candidate_revision=candidate_revision,
            decision_sequence=self.active.decision_sequence + 1,
            universe_version=f"{core_generation}.{candidate_revision}",
            created_at=now,
            effective_at=effective_at,
            reason=reason,
            core=result.core,
            boundary=result.boundary,
            probe=result.probe,
            source_hashes=result.source_hashes,
            universe_hash=universe_hash(result.core, result.boundary, result.probe),
        )
        atomic_write_bytes(self._pending_path, canonical_json_bytes(decision))
        return decision

    def _write_evaluation(
        self,
        result: SelectionResult,
        *,
        now: datetime,
        effective_at: datetime,
        kind: str,
    ) -> None:
        if result.mature_pool_count < self._policy.mature_pool_warning_size:
            logger.warning(
                "mature evidence pool below reserve target count=%d target=%d",
                result.mature_pool_count,
                self._policy.mature_pool_warning_size,
            )
        evaluation = {
            "active_core_generation": self.active.core_generation,
            "active_candidate_revision": self.active.candidate_revision,
            "active_decision_sequence": self.active.decision_sequence,
            "active_universe_version": self.active.universe_version,
            "boundary": list(result.boundary),
            "core": list(result.core),
            "effective_at": effective_at.isoformat(),
            "evaluated_at": now.isoformat(),
            "inactive": list(result.inactive),
            "kind": kind,
            "decision_frozen_reason": result.decision_frozen_reason,
            "mature_pool_count": result.mature_pool_count,
            "probe": list(result.probe),
            "probe_pool_count": result.probe_pool_count,
            "source_hashes": list(result.source_hashes),
        }
        if result.market_context is not None:
            evaluation["market_context"] = {
                "horizon_days": result.market_context.horizon_days,
                "panel_count": result.market_context.panel_count,
                "quote_volume_breadth": str(result.market_context.quote_volume_breadth),
                "quote_volume_factor": str(result.market_context.quote_volume_factor),
                "state": result.market_context.state.value,
                "trade_count_breadth": str(result.market_context.trade_count_breadth),
                "trade_count_factor": str(result.market_context.trade_count_factor),
            }
        evaluation_name = f"{now.strftime('%Y%m%dT%H%M%S.%fZ')}.evaluation.json"
        atomic_write_bytes(self._evaluations / evaluation_name, canonical_json_bytes(evaluation))

    def has_due(self, now: datetime) -> bool:
        return self._select_due(now) is not None

    def apply_due(self, now: datetime | None = None) -> UniverseDecision | None:
        now = now or datetime.now(UTC)
        selected = self._select_due(now)
        if selected is None:
            return None
        decision, source_path = selected
        previous = self.active
        self._active = decision
        for symbol in decision.members:
            self._member_since.setdefault(symbol, decision.effective_at)
        self._member_since = {
            symbol: joined
            for symbol, joined in self._member_since.items()
            if symbol in decision.members
        }
        for symbol in decision.core:
            if symbol not in previous.core:
                self._core_since[symbol] = decision.effective_at
            else:
                self._core_since.setdefault(symbol, decision.effective_at)
        self._core_since = {
            symbol: joined for symbol, joined in self._core_since.items() if symbol in decision.core
        }
        self._write_active(decision)
        self._write_state()
        source_path.unlink(missing_ok=True)
        self._pending_path.unlink(missing_ok=True)
        return decision

    def _select_due(self, now: datetime) -> tuple[UniverseDecision, Path] | None:
        candidates: list[tuple[UniverseDecision, Path]] = []
        if self._policy.automation_enabled and self._pending_path.exists():
            pending = UniverseDecision.model_validate_json(self._pending_path.read_bytes())
            if (
                pending.decision_sequence > self.active.decision_sequence
                and pending.effective_at <= now
            ):
                candidates.append((pending, self._pending_path))
        for path in self._overrides.glob("*.override.json"):
            try:
                override = CandidateOverride.model_validate_json(path.read_bytes())
                decision = self._decision_from_override(override)
            except ValueError:
                logger.exception("rejecting invalid candidate override path=%s", path)
                path.replace(path.with_suffix(".rejected.json"))
                continue
            if (
                decision.decision_sequence > self.active.decision_sequence
                and decision.effective_at <= now
            ):
                candidates.append((decision, path))
        return (
            max(
                candidates,
                key=lambda item: (
                    item[0].decision_sequence,
                    item[0].effective_at,
                    item[0].created_at,
                    item[1].name,
                ),
            )
            if candidates
            else None
        )

    def _decision_from_override(self, override: CandidateOverride) -> UniverseDecision:
        proposed = set((*override.boundary, *override.probe))
        if proposed & set(self.active.core):
            raise ValueError("candidate override cannot include active core members")
        current = set((*self.active.boundary, *self.active.probe))
        if len(proposed - current) > self._policy.candidate_daily_replacements:
            raise ValueError("candidate override exceeds daily replacement limit")
        minimum_joined_at = override.effective_at - timedelta(
            hours=self._policy.candidate_minimum_dwell_hours
        )
        too_young = [
            symbol
            for symbol in current - proposed
            if self._member_since.get(symbol, override.effective_at) > minimum_joined_at
        ]
        if too_young:
            raise ValueError(f"candidate override violates dwell: {too_young}")
        return UniverseDecision(
            core_generation=self.active.core_generation,
            candidate_revision=self.active.candidate_revision + 1,
            decision_sequence=self.active.decision_sequence + 1,
            universe_version=(
                f"{self.active.core_generation}.{self.active.candidate_revision + 1}"
            ),
            created_at=override.created_at,
            effective_at=override.effective_at,
            reason=UniverseDecisionReason.MANUAL_CANDIDATE_OVERRIDE,
            core=self.active.core,
            boundary=override.boundary,
            probe=override.probe,
            universe_hash=universe_hash(self.active.core, override.boundary, override.probe),
        )

    def _write_observation(self, snapshot: DiscoverySnapshot) -> None:
        root = self._observations / snapshot.observed_at.date().isoformat()
        atomic_write_bytes(root / "exchange-info.json.gz", gzip.compress(snapshot.exchange_info))
        atomic_write_bytes(
            root / "exchange-info-confirmation.json.gz",
            gzip.compress(snapshot.exchange_info_confirmation),
        )
        atomic_write_bytes(root / "market-tickers.json.gz", gzip.compress(snapshot.market_tickers))
        atomic_write_bytes(root / "daily-klines.json.gz", gzip.compress(snapshot.daily_klines))
        atomic_write_bytes(
            root / "liquidity-depth.json.gz", gzip.compress(snapshot.liquidity_depth)
        )
        atomic_write_bytes(
            root / "observation.json",
            canonical_json_bytes(
                {
                    "observed_at": snapshot.observed_at.isoformat(),
                    "source_hashes": list(snapshot.source_hashes),
                }
            ),
        )

    def _load_state(self) -> None:
        raw = orjson.loads(self._state_path.read_bytes())
        self._member_since = {
            symbol: datetime.fromisoformat(value) for symbol, value in raw["member_since"].items()
        }
        self._core_since = {
            symbol: datetime.fromisoformat(value) for symbol, value in raw["core_since"].items()
        }

    def _write_active(self, decision: UniverseDecision) -> None:
        atomic_write_bytes(self._active_path, canonical_json_bytes(decision))
        atomic_write_bytes(
            self._decisions
            / (
                f"sequence-{decision.decision_sequence:08d}."
                f"version-{decision.universe_version}.decision.json"
            ),
            canonical_json_bytes(decision),
        )

    def _write_state(self) -> None:
        atomic_write_bytes(
            self._state_path,
            canonical_json_bytes(
                {
                    "core_since": {
                        symbol: joined.isoformat()
                        for symbol, joined in sorted(self._core_since.items())
                    },
                    "member_since": {
                        symbol: joined.isoformat()
                        for symbol, joined in sorted(self._member_since.items())
                    },
                }
            ),
        )


def _next_effective_at(now: datetime, cutoff_minute: int) -> datetime:
    effective = datetime.combine(now.date() + timedelta(days=1), time.min, UTC)
    if now.hour == 23 and now.minute > cutoff_minute:
        effective += timedelta(days=1)
    return effective


def _decision_reason(
    active: UniverseDecision, result: SelectionResult, effective_at: datetime
) -> UniverseDecisionReason:
    if result.inactive:
        return UniverseDecisionReason.INACTIVE_REPLACEMENT
    if result.core != active.core and effective_at.weekday() == 0:
        return UniverseDecisionReason.WEEKLY_CORE
    return UniverseDecisionReason.DAILY_CANDIDATE


def _next_version(active: UniverseDecision, core: tuple[str, ...]) -> tuple[int, int]:
    if core != active.core:
        return active.core_generation + 1, 0
    return active.core_generation, active.candidate_revision + 1
