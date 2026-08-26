from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from miry.contracts.symbols import is_exchange_symbol

HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{8,160}$")


class StreamType(StrEnum):
    DEPTH = "depth"
    RPI_DEPTH = "rpi_depth"
    DEPTH_SNAPSHOT = "depth_snapshot"
    RPI_DEPTH_SNAPSHOT = "rpi_depth_snapshot"
    BOOK_TICKER = "book_ticker"
    AGG_TRADE = "agg_trade"
    TRADE = "trade"
    MARK_PRICE = "mark_price"
    FORCE_ORDER = "force_order"
    CONTRACT_INFO = "contract_info"
    OPEN_INTEREST = "open_interest"
    EXCHANGE_INFO = "exchange_info"
    MARKET_TICKERS = "market_tickers"
    DAILY_KLINES = "daily_klines"
    LIQUIDITY_DEPTH = "liquidity_depth"
    UNIVERSE_DECISION = "universe_decision"
    FORMAL_COLLECTION_STARTED = "formal_collection_started"
    CLOCK_SAMPLE = "clock_sample"
    WS_CONTROL = "ws_control"
    UNKNOWN = "unknown"


class WriterGroup(StrEnum):
    DEPTH = "depth"
    TRADES_MARKET = "trades_market"
    METADATA = "metadata"
    CONTROL = "control"


class ContentType(StrEnum):
    PARQUET = "application/vnd.apache.parquet"
    GAP_JSON = "application/vnd.ft-shadow.gap+json"


class GapReason(StrEnum):
    INGEST_OVERLOAD = "INGEST_OVERLOAD_GAP"
    STORAGE_EXHAUSTED = "STORAGE_EXHAUSTED_GAP"
    CONNECTION_LOST = "CONNECTION_LOST_GAP"
    L2_SEQUENCE = "L2_SEQUENCE_GAP"
    L2_REANCHOR = "L2_REANCHOR_GAP"
    PLANNED_BOUNDARY = "PLANNED_BOUNDARY_GAP"
    COLLECTOR_STOPPED = "COLLECTOR_STOPPED_GAP"


class GapState(StrEnum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class UniverseDecisionReason(StrEnum):
    FORMAL_BOOTSTRAP = "formal_bootstrap"
    DAILY_CANDIDATE = "daily_candidate"
    WEEKLY_CORE = "weekly_core"
    INACTIVE_REPLACEMENT = "inactive_replacement"
    MANUAL_CANDIDATE_OVERRIDE = "manual_candidate_override"


@dataclass(frozen=True, slots=True)
class RawEvent:
    schema_version: int
    exchange_symbol: str | None
    stream_type: StreamType
    collector_id: str
    boot_id: str
    segment_id: str
    connection_id: str
    receive_seq: int
    app_receive_realtime_ns: int
    app_receive_monotonic_ns: int
    payload_bytes: bytes
    request_id: str | None = None
    request_realtime_ns: int | None = None

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("raw event requires schema_version=1")
        if self.exchange_symbol is not None and not is_exchange_symbol(self.exchange_symbol):
            raise ValueError("invalid exchange_symbol")
        if not self.collector_id or not self.boot_id or not self.segment_id:
            raise ValueError("collector, boot, and segment identities are required")
        if not self.connection_id:
            raise ValueError("connection_id is required")
        if self.receive_seq < 1:
            raise ValueError("receive_seq must be positive")
        if min(self.app_receive_realtime_ns, self.app_receive_monotonic_ns) < 0:
            raise ValueError("receive timestamps cannot be negative")
        if not self.payload_bytes:
            raise ValueError("payload_bytes cannot be empty")
        if (self.request_id is None) != (self.request_realtime_ns is None):
            raise ValueError("REST request identity and timestamp must be supplied together")

    @property
    def approximate_size_bytes(self) -> int:
        return len(self.payload_bytes) + 256

    @property
    def writer_group(self) -> WriterGroup:
        if self.stream_type in {
            StreamType.DEPTH,
            StreamType.RPI_DEPTH,
            StreamType.DEPTH_SNAPSHOT,
            StreamType.RPI_DEPTH_SNAPSHOT,
        }:
            return WriterGroup.DEPTH
        if self.stream_type in {
            StreamType.BOOK_TICKER,
            StreamType.AGG_TRADE,
            StreamType.TRADE,
            StreamType.MARK_PRICE,
            StreamType.FORCE_ORDER,
        }:
            return WriterGroup.TRADES_MARKET
        return WriterGroup.METADATA


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ChunkRef(FrozenModel):
    chunk_id: str = Field(min_length=8, max_length=160)
    data_path: str = Field(min_length=1, max_length=500)
    sha256: str
    size_bytes: int = Field(gt=0)
    content_type: ContentType

    @field_validator("chunk_id")
    @classmethod
    def validate_chunk_id(cls, value: str) -> str:
        if not ID_PATTERN.fullmatch(value):
            raise ValueError("chunk_id contains unsafe characters")
        return value

    @field_validator("sha256")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        if not HASH_PATTERN.fullmatch(value):
            raise ValueError("sha256 must be lowercase hexadecimal")
        return value

    @field_validator("data_path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("data_path must be a safe relative POSIX path")
        return value


class ChunkManifest(ChunkRef):
    schema_version: Literal[1] = 1
    collector_id: str = Field(min_length=1, max_length=100)
    writer_group: WriterGroup
    utc_date: date
    event_count: int = Field(gt=0)
    min_app_receive_realtime_ns: int = Field(ge=0)
    max_app_receive_realtime_ns: int = Field(ge=0)
    data_contract_hash: str
    universe_hash: str
    created_at: datetime

    @field_validator("data_contract_hash", "universe_hash")
    @classmethod
    def validate_contract_hash(cls, value: str) -> str:
        if not HASH_PATTERN.fullmatch(value):
            raise ValueError("contract hashes must be lowercase SHA-256")
        return value

    @field_validator("created_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("created_at must be UTC")
        return value

    @model_validator(mode="after")
    def validate_time_range(self) -> ChunkManifest:
        if self.max_app_receive_realtime_ns < self.min_app_receive_realtime_ns:
            raise ValueError("chunk timestamp range is reversed")
        return self

    def as_ref(self) -> ChunkRef:
        return ChunkRef.model_validate(
            self.model_dump(
                include={"chunk_id", "data_path", "sha256", "size_bytes", "content_type"}
            )
        )


class Ack(FrozenModel):
    schema_version: Literal[1] = 1
    chunk_id: str = Field(min_length=8, max_length=160)
    sha256: str
    durable_at: datetime

    @field_validator("chunk_id")
    @classmethod
    def validate_chunk_id(cls, value: str) -> str:
        if not ID_PATTERN.fullmatch(value):
            raise ValueError("chunk_id contains unsafe characters")
        return value

    @field_validator("sha256")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        if not HASH_PATTERN.fullmatch(value):
            raise ValueError("sha256 must be lowercase hexadecimal")
        return value

    @field_validator("durable_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("durable_at must be UTC")
        return value


class DayManifest(FrozenModel):
    schema_version: Literal[1] = 1
    collector_id: str = Field(min_length=1, max_length=100)
    utc_date: date
    sealed_at: datetime
    chunks: tuple[ChunkRef, ...]

    @field_validator("sealed_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("sealed_at must be UTC")
        return value

    @model_validator(mode="after")
    def validate_unique_chunks(self) -> DayManifest:
        identities = [(chunk.chunk_id, chunk.sha256) for chunk in self.chunks]
        if len(identities) != len(set(identities)):
            raise ValueError("day manifest contains duplicate chunk identities")
        if self.sealed_at.date() <= self.utc_date:
            raise ValueError("a UTC day cannot be sealed before it ends")
        return self


class GapEvent(FrozenModel):
    schema_version: Literal[2] = 2
    gap_id: str = Field(min_length=8, max_length=160)
    state: GapState
    reason: GapReason
    collector_id: str = Field(min_length=1, max_length=100)
    connection_id: str | None = Field(default=None, max_length=160)
    exchange_symbols: tuple[str, ...] = ()
    stream_types: tuple[StreamType, ...] = ()
    observed_at_realtime_ns: int = Field(ge=0)
    affected_from_realtime_ns: int | None = Field(default=None, ge=0)
    detail: str = Field(default="", max_length=500)

    @field_validator("exchange_symbols")
    @classmethod
    def validate_symbols(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not is_exchange_symbol(value) for value in values):
            raise ValueError("gap contains an invalid exchange symbol")
        return values

    @model_validator(mode="after")
    def validate_affected_from(self) -> GapEvent:
        if (
            self.affected_from_realtime_ns is not None
            and self.affected_from_realtime_ns > self.observed_at_realtime_ns
        ):
            raise ValueError("gap affected_from cannot be after observation")
        if self.state is GapState.CLOSED and self.affected_from_realtime_ns is not None:
            raise ValueError("closed gap events cannot define affected_from")
        return self


class UniverseDecision(FrozenModel):
    core_generation: int = Field(ge=1)
    candidate_revision: int = Field(ge=0)
    decision_sequence: int = Field(ge=1)
    universe_version: str = Field(pattern=r"^[1-9][0-9]*\.[0-9]+$")
    created_at: datetime
    effective_at: datetime
    reason: UniverseDecisionReason
    core: tuple[str, ...] = Field(min_length=50, max_length=50)
    boundary: tuple[str, ...] = Field(min_length=5, max_length=5)
    probe: tuple[str, ...] = Field(min_length=5, max_length=5)
    source_hashes: tuple[str, ...] = ()
    universe_hash: str

    @field_validator("created_at", "effective_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        return ChunkManifest.require_utc(value)

    @field_validator("core", "boundary", "probe")
    @classmethod
    def validate_members(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(value.upper() for value in values)
        if any(not is_exchange_symbol(value) for value in normalized):
            raise ValueError("universe contains an invalid exchange symbol")
        if len(normalized) != len(set(normalized)):
            raise ValueError("universe role contains duplicate members")
        if normalized != tuple(sorted(normalized)):
            raise ValueError("universe role members must be sorted")
        return normalized

    @field_validator("source_hashes")
    @classmethod
    def validate_source_hashes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not HASH_PATTERN.fullmatch(value) for value in values):
            raise ValueError("source hashes must be lowercase SHA-256")
        return values

    @property
    def members(self) -> tuple[str, ...]:
        return tuple(sorted((*self.core, *self.boundary, *self.probe)))

    @model_validator(mode="after")
    def validate_schedule_version_and_hash(self) -> UniverseDecision:
        if self.universe_version != f"{self.core_generation}.{self.candidate_revision}":
            raise ValueError("universe_version does not match its integer components")
        if self.effective_at < self.created_at:
            raise ValueError("decision cannot be effective before creation")
        all_members = (*self.core, *self.boundary, *self.probe)
        if len(set(all_members)) != 60:
            raise ValueError("universe roles must contain 60 distinct members")
        if self.reason is not UniverseDecisionReason.FORMAL_BOOTSTRAP and any(
            (
                self.effective_at.hour,
                self.effective_at.minute,
                self.effective_at.second,
                self.effective_at.microsecond,
            )
        ):
            raise ValueError("universe changes must become effective at 00:00 UTC")
        from miry.contracts.serde import universe_hash

        if self.universe_hash != universe_hash(self.core, self.boundary, self.probe):
            raise ValueError("universe_hash does not match role membership")
        return self


class CandidateOverride(FrozenModel):
    created_at: datetime
    effective_at: datetime
    boundary: tuple[str, ...] = Field(min_length=5, max_length=5)
    probe: tuple[str, ...] = Field(min_length=5, max_length=5)
    reason: Literal["manual_candidate_override"] = "manual_candidate_override"

    @field_validator("created_at", "effective_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        return ChunkManifest.require_utc(value)

    @field_validator("boundary", "probe")
    @classmethod
    def validate_members(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted(value.upper() for value in values))
        if any(not is_exchange_symbol(value) for value in normalized):
            raise ValueError("override contains an invalid exchange symbol")
        if len(set(normalized)) != len(normalized):
            raise ValueError("override role contains duplicate members")
        return normalized

    @model_validator(mode="after")
    def validate_override(self) -> CandidateOverride:
        if self.effective_at < self.created_at:
            raise ValueError("override cannot be effective before creation")
        if any(
            (
                self.effective_at.hour,
                self.effective_at.minute,
                self.effective_at.second,
                self.effective_at.microsecond,
            )
        ):
            raise ValueError("candidate override must become effective at 00:00 UTC")
        if len(set((*self.boundary, *self.probe))) != 10:
            raise ValueError("override roles must contain 10 distinct members")
        return self


class DayReleaseRef(FrozenModel):
    collector_id: str = Field(min_length=1, max_length=100)
    utc_date: date
    sealed_manifest_sha256: str

    @field_validator("sealed_manifest_sha256")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        if not HASH_PATTERN.fullmatch(value):
            raise ValueError("sealed manifest hash must be lowercase SHA-256")
        return value


class DatasetRelease(FrozenModel):
    schema_version: Literal[1] = 1
    release_id: str = Field(min_length=8, max_length=160)
    created_at: datetime
    days: tuple[DayReleaseRef, ...] = Field(min_length=1)

    @field_validator("release_id")
    @classmethod
    def validate_release_id(cls, value: str) -> str:
        if not ID_PATTERN.fullmatch(value):
            raise ValueError("release_id contains unsafe characters")
        return value

    @field_validator("created_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("created_at must be UTC")
        return value

    @model_validator(mode="after")
    def validate_unique_days(self) -> DatasetRelease:
        identities = [(day.collector_id, day.utc_date) for day in self.days]
        if len(identities) != len(set(identities)):
            raise ValueError("release contains duplicate UTC days")
        return self
