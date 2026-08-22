from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ft_shadow_data_plane.contracts.models import HASH_PATTERN, SYMBOL_PATTERN

if TYPE_CHECKING:
    from ft_shadow_data_plane.central.selector import RollingPolicy


class UniversePolicyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    experiment_id: str = Field(min_length=8, max_length=160)
    bootstrap_evidence_sha256: str
    core_generation: int = Field(default=1, ge=1)
    candidate_revision: int = Field(default=0, ge=0)
    decision_sequence: int = Field(default=1, ge=1)
    core: tuple[str, ...] = Field(min_length=50, max_length=50)
    boundary: tuple[str, ...] = Field(min_length=5, max_length=5)
    probe: tuple[str, ...] = Field(min_length=5, max_length=5)
    discovery_hour_utc: int = Field(default=23, ge=0, le=23)
    discovery_minute_utc: int = Field(default=50, ge=0, le=59)
    decision_cutoff_minute_utc: int = Field(default=55, ge=0, le=59)
    automation_enabled: bool = True
    liquidity_window_days: int = Field(default=14, ge=14, le=30)
    probe_minimum_complete_days: int = Field(default=7, ge=1, le=14)
    market_context_baseline_days: int = Field(default=28, ge=14, le=60)
    market_context_change_ratio: Decimal = Field(default=Decimal("1.25"), gt=1)
    market_context_breadth_ratio: Decimal = Field(
        default=Decimal("0.70"), gt=Decimal("0.5"), le=1
    )
    market_context_minimum_instruments: int = Field(default=60, ge=60)
    liquidity_depth_samples: int = Field(default=3, ge=3, le=5)
    liquidity_book_ticker_samples: int = Field(default=5, ge=3, le=10)
    depth_mature_candidate_count: int = Field(default=200, ge=60, le=200)
    depth_probe_candidate_count: int = Field(default=100, ge=10, le=100)
    liquidity_request_interval_seconds: float = Field(default=0.25, ge=0.1, le=2)
    candidate_minimum_dwell_hours: int = Field(default=48, ge=1)
    core_minimum_dwell_days: int = Field(default=14, ge=1)
    candidate_daily_replacements: int = Field(default=2, ge=1, le=2)
    core_weekly_replacements: int = Field(default=5, ge=1, le=5)
    core_minimum_age_days: int = Field(default=30, ge=1)
    core_entry_rank: int = Field(default=45, ge=1, le=50)
    core_retain_rank: int = Field(default=55, ge=50)
    boundary_retain_rank: int = Field(default=10, ge=5)
    mature_pool_warning_size: int = Field(default=65, ge=55)

    @field_validator("core", "boundary", "probe")
    @classmethod
    def validate_role(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted(value.upper() for value in values))
        if any(not SYMBOL_PATTERN.fullmatch(value) for value in normalized):
            raise ValueError("universe role contains an invalid symbol")
        if len(set(normalized)) != len(normalized):
            raise ValueError("universe role contains duplicates")
        return normalized

    @field_validator("bootstrap_evidence_sha256")
    @classmethod
    def validate_bootstrap_evidence_sha256(cls, value: str) -> str:
        if not HASH_PATTERN.fullmatch(value):
            raise ValueError("bootstrap evidence must be a lowercase SHA-256")
        return value

    @model_validator(mode="after")
    def validate_roles(self) -> UniversePolicyConfig:
        members = (*self.core, *self.boundary, *self.probe)
        if len(set(members)) != 60:
            raise ValueError("universe roles must contain 60 distinct symbols")
        if self.decision_cutoff_minute_utc <= self.discovery_minute_utc:
            raise ValueError("decision cutoff must follow discovery in the same UTC hour")
        if self.probe_minimum_complete_days > self.liquidity_window_days:
            raise ValueError("probe history cannot exceed the liquidity window")
        return self

    @property
    def members(self) -> tuple[str, ...]:
        return tuple(sorted((*self.core, *self.boundary, *self.probe)))

    def rolling_policy(self) -> RollingPolicy:
        from ft_shadow_data_plane.central.selector import RollingPolicy

        return RollingPolicy(
            liquidity_window_days=self.liquidity_window_days,
            probe_minimum_complete_days=self.probe_minimum_complete_days,
            market_context_baseline_days=self.market_context_baseline_days,
            market_context_change_ratio=self.market_context_change_ratio,
            market_context_breadth_ratio=self.market_context_breadth_ratio,
            market_context_minimum_instruments=self.market_context_minimum_instruments,
            liquidity_depth_samples=self.liquidity_depth_samples,
            liquidity_book_ticker_samples=self.liquidity_book_ticker_samples,
            depth_mature_candidate_count=self.depth_mature_candidate_count,
            depth_probe_candidate_count=self.depth_probe_candidate_count,
            candidate_minimum_dwell_hours=self.candidate_minimum_dwell_hours,
            core_minimum_dwell_days=self.core_minimum_dwell_days,
            candidate_daily_replacements=self.candidate_daily_replacements,
            core_weekly_replacements=self.core_weekly_replacements,
            core_minimum_age_days=self.core_minimum_age_days,
            core_entry_rank=self.core_entry_rank,
            core_retain_rank=self.core_retain_rank,
            boundary_retain_rank=self.boundary_retain_rank,
            mature_pool_warning_size=self.mature_pool_warning_size,
        )


class EdgeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    collector_id: str = Field(pattern=r"^[A-Za-z0-9._-]{1,100}$")
    data_root: Path
    universe: UniversePolicyConfig
    public_ws_url: str
    market_ws_url: str
    rest_url: str
    public_connection_shards: int = Field(default=4, ge=1, le=4)
    public_symbol_load_weights: dict[str, int] = Field(default_factory=dict)
    connection_rotation_seconds: int = Field(default=82_800, ge=3_600, le=86_000)
    connection_overlap_seconds: int = Field(default=15, ge=1, le=120)
    websocket_receive_timeout_seconds: float = Field(default=30.0, ge=5, le=300)
    websocket_ping_interval_seconds: float = Field(default=20.0, ge=5, le=60)
    websocket_ping_timeout_seconds: float = Field(default=20.0, ge=5, le=60)
    subscription_audit_seconds: float = Field(default=60.0, ge=10, le=300)
    subscription_audit_timeout_seconds: float = Field(default=20.0, ge=1, le=30)
    subscription_audit_failures_before_reconnect: int = Field(default=3, ge=1, le=5)
    refresh_failures_before_reconnect: int = Field(default=2, ge=1, le=5)
    websocket_max_queue: int = Field(default=16, ge=1, le=16)
    websocket_max_message_bytes: int = Field(default=2 * 1024**2, ge=1024**2)
    public_stream_liveness_seconds: float = Field(default=30.0, ge=10, le=300)
    mark_price_liveness_seconds: float = Field(default=15.0, ge=3, le=30)
    day_seal_grace_seconds: float = Field(default=150.0, ge=10, le=600)
    lease_heartbeat_seconds: float = Field(default=30.0, ge=5, le=300)
    open_interest_interval_seconds: int = Field(default=30, ge=10, le=300)
    open_interest_startup_spread_seconds: float = Field(default=5.0, ge=1, le=30)
    clock_sample_interval_seconds: int = Field(default=60, ge=10, le=300)
    snapshot_request_interval_seconds: float = Field(default=0.75, ge=0.5, le=10)
    snapshot_request_concurrency: int = Field(default=4, ge=1, le=8)
    queue_max_bytes: int = Field(default=64 * 1024**2, ge=16 * 1024**2)
    queue_warn_ratio: float = Field(default=0.70, gt=0, lt=1)
    queue_resume_ratio: float = Field(default=0.50, gt=0, lt=1)
    chunk_max_seconds: int = Field(default=60, ge=5, le=300)
    chunk_max_bytes: int = Field(default=256 * 1024**2, ge=1024**2)
    chunk_max_events: int = Field(default=1_000_000, ge=1_000)
    writer_batch_events: int = Field(default=2_000, ge=100)
    writer_batch_bytes: int = Field(default=2 * 1024**2, ge=64 * 1024)
    spool_max_bytes: int = Field(default=10 * 1024**3, ge=1024**3)
    minimum_free_bytes: int = Field(default=2 * 1024**3, ge=1024**3)
    storage_check_seconds: int = Field(default=5, ge=1, le=60)
    d0_enabled: bool = False
    log_level: str = "INFO"

    @field_validator("public_symbol_load_weights")
    @classmethod
    def validate_public_symbol_load_weights(cls, values: dict[str, int]) -> dict[str, int]:
        normalized = {symbol.upper(): weight for symbol, weight in values.items()}
        if len(normalized) != len(values):
            raise ValueError("public symbol load weights contain duplicate symbols")
        if any(not SYMBOL_PATTERN.fullmatch(symbol) for symbol in normalized):
            raise ValueError("public symbol load weights contain an invalid symbol")
        if any(weight <= 0 for weight in normalized.values()):
            raise ValueError("public symbol load weights must be positive")
        return normalized

    @model_validator(mode="after")
    def validate_ratios(self) -> EdgeConfig:
        if self.public_symbol_load_weights and set(self.public_symbol_load_weights) != set(
            self.universe.members
        ):
            raise ValueError("public symbol load weights must cover the configured universe")
        if self.queue_resume_ratio >= self.queue_warn_ratio:
            raise ValueError("queue_resume_ratio must be below queue_warn_ratio")
        if self.day_seal_grace_seconds <= self.public_stream_liveness_seconds:
            raise ValueError("day seal grace must exceed public stream liveness timeout")
        if self.subscription_audit_seconds <= self.websocket_receive_timeout_seconds:
            raise ValueError("subscription audit must exceed websocket receive timeout")
        if self.subscription_audit_seconds <= self.subscription_audit_timeout_seconds:
            raise ValueError("subscription audit interval must exceed its response timeout")
        audit_detection_seconds = self.subscription_audit_seconds + (
            self.subscription_audit_timeout_seconds
            * self.subscription_audit_failures_before_reconnect
        )
        if self.day_seal_grace_seconds <= audit_detection_seconds:
            raise ValueError("day seal grace must exceed the subscription audit detection window")
        return self


def load_edge_config(path: Path) -> EdgeConfig:
    with path.open("rb") as source:
        raw = yaml.safe_load(source)
    return EdgeConfig.model_validate(raw)
