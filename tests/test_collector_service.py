from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import BaseModel

from miry.collector.service import CollectorService
from miry.collector.sources import SourceUpdateError
from miry.collector.spool import AckApplyResult, SpoolStatus
from miry.contracts.models import GapReason, StreamType


class OnlineSources:
    running = True
    ready_for_rebalance = True

    def __init__(self) -> None:
        self.updates: list[tuple[str, ...]] = []
        self.rebalances = 0

    async def update_instruments(self, members: tuple[str, ...]) -> None:
        self.updates.append(members)

    async def rebalance_public_routes(self) -> bool:
        self.rebalances += 1
        return True


class RebalanceTimeoutSources(OnlineSources):
    async def rebalance_public_routes(self) -> bool:
        self.rebalances += 1
        raise SourceUpdateError("subscription update acknowledgement timed out")


class MembershipTimeoutSources(OnlineSources):
    def __init__(self) -> None:
        super().__init__()
        self.fail_updates = True

    async def update_instruments(self, members: tuple[str, ...]) -> None:
        self.updates.append(members)
        if self.fail_updates:
            raise SourceUpdateError("membership subscription update timed out")


class StorageSources:
    def __init__(self, *, running: bool, ready_error: BaseException | None = None) -> None:
        self.running = running
        self.ready_error = ready_error
        self.start_calls = 0
        self.stop_calls = 0
        self.ready_calls = 0

    def raise_if_failed(self) -> None:
        return None

    async def start(self, _members: tuple[str, ...]) -> None:
        if self.running:
            raise RuntimeError("sources already running")
        self.start_calls += 1
        self.running = True

    async def stop(self) -> None:
        self.stop_calls += 1
        self.running = False

    async def wait_ready(self) -> None:
        self.ready_calls += 1
        if self.ready_error is not None:
            raise self.ready_error


class ReadinessSources:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def wait_ready(self) -> None:
        self.calls.append("realtime")

    async def wait_discovery_ready(self) -> None:
        self.calls.append("discovery")


class StorageSpool:
    def __init__(self, status: SpoolStatus, stop: asyncio.Event) -> None:
        self._status = status
        self._stop = stop

    def apply_acks(self) -> AckApplyResult:
        return AckApplyResult()

    def status(self) -> SpoolStatus:
        self._stop.set()
        return self._status


class StorageGaps:
    def __init__(self) -> None:
        self.opened: list[GapReason] = []
        self.closed: list[tuple[str, GapReason]] = []

    async def open(self, reason: GapReason, **_kwargs: object) -> str:
        self.opened.append(reason)
        return "gap-new"

    async def close(self, gap_id: str, reason: GapReason, **_kwargs: object) -> None:
        self.closed.append((gap_id, reason))


class BoundaryGaps:
    def __init__(self) -> None:
        self.opened_symbols: list[tuple[str, ...]] = []
        self.affected_from_ns: list[int | None] = []
        self.closed_symbols: list[tuple[str, ...]] = []
        self.rollovers: list[datetime] = []
        self.universe_hashes: list[str] = []

    async def open(self, *args: object, **kwargs: object) -> str:
        self.opened_symbols.append(kwargs["exchange_symbols"])  # type: ignore[arg-type]
        self.affected_from_ns.append(kwargs.get("affected_from_realtime_ns"))  # type: ignore[arg-type]
        return "gap-boundary"

    async def close(self, *args: object, **kwargs: object) -> None:
        self.closed_symbols.append(kwargs["exchange_symbols"])  # type: ignore[arg-type]

    async def rollover(self, boundary: datetime) -> None:
        self.rollovers.append(boundary)

    def set_universe_hash(self, value: str) -> None:
        self.universe_hashes.append(value)


class Universe:
    def __init__(self, previous: object, decision: object | None) -> None:
        self.active = previous
        self._decision = decision

    def apply_due(self, now: datetime) -> object | None:
        if self._decision is not None:
            self.active = self._decision
        return self._decision


class RecordingIngest:
    def __init__(self) -> None:
        self.rotations: list[str] = []

    async def rotate(self, *, universe_hash: str) -> None:
        self.rotations.append(universe_hash)


class RecordingDayIndex:
    def __init__(self) -> None:
        self.seals: list[date] = []

    async def seal(self, utc_date: date, *, sealed_at: datetime) -> None:
        self.seals.append(utc_date)


class RecordingLease:
    def __init__(self) -> None:
        self.seals: list[date] = []

    def record_sealed(self, utc_date: date) -> None:
        self.seals.append(utc_date)

    def write_running(self, _watermark: int) -> None:
        return None


class FakeDecision(BaseModel):
    members: tuple[str, ...]
    universe_hash: str


@pytest.mark.asyncio
async def test_established_collection_recovery_does_not_wait_for_discovery(
    tmp_path: Path,
) -> None:
    service: Any = object.__new__(CollectorService)
    service._sources = ReadinessSources()
    service._formal_start_path = tmp_path / "formal-start.json"
    service._formal_start_path.write_text("{}", encoding="ascii")

    await service._wait_source_readiness()

    assert service._sources.calls == ["realtime"]


@pytest.mark.asyncio
async def test_new_collection_waits_for_discovery_before_formal_start(tmp_path: Path) -> None:
    service: Any = object.__new__(CollectorService)
    service._sources = ReadinessSources()
    service._formal_start_path = tmp_path / "formal-start.json"

    await service._wait_source_readiness()

    assert service._sources.calls == ["realtime", "discovery"]


@pytest.mark.asyncio
async def test_midnight_without_change_keeps_all_sources_online() -> None:
    previous = FakeDecision(members=_members(), universe_hash="a" * 64)
    service = _service(previous, None)
    midnight = datetime(2026, 8, 11, tzinfo=UTC)

    await service._apply_midnight_boundary(midnight)

    assert service._sources.updates == []  # type: ignore[attr-defined]
    assert service._sources.rebalances == 0  # type: ignore[attr-defined]
    assert service._rebalance_requested.is_set()  # type: ignore[attr-defined]
    assert service._gaps.opened_symbols == []  # type: ignore[attr-defined]
    assert service._ingest.rotations == ["a" * 64, "a" * 64]  # type: ignore[attr-defined]
    assert service._day_index.seals == [date(2026, 8, 10)]  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_midnight_rebalance_timeout_is_recoverable() -> None:
    previous = FakeDecision(members=_members(), universe_hash="a" * 64)
    service = _service(previous, None)
    service._sources = RebalanceTimeoutSources()

    await service._apply_midnight_boundary(datetime(2026, 8, 11, tzinfo=UTC))
    assert await service._try_public_rebalance() is None

    assert service._sources.running
    assert service._sources.rebalances == 1
    assert not service._stop.is_set()
    assert service._ingest.rotations == ["a" * 64, "a" * 64]
    assert service._control_events == [StreamType.UNIVERSE_DECISION]
    assert service._day_index.seals == [date(2026, 8, 10)]
    assert service._lease.seals == [date(2026, 8, 10)]


@pytest.mark.asyncio
async def test_rebalance_loop_converges_in_bounded_steps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(FakeDecision(members=_members(), universe_hash="a" * 64), None)
    outcomes = iter((True, True, False))
    calls = 0

    async def try_rebalance() -> bool:
        nonlocal calls
        calls += 1
        outcome = next(outcomes)
        if outcome is False:
            service._stop.set()
        return outcome

    service._try_public_rebalance = try_rebalance
    service._rebalance_requested.set()
    monkeypatch.setattr("miry.collector.service.PUBLIC_REBALANCE_COOLDOWN_SECONDS", 0)

    await asyncio.wait_for(service._rebalance_loop(), timeout=0.5)

    assert calls == 3


@pytest.mark.asyncio
async def test_one_symbol_change_only_marks_changed_symbols() -> None:
    previous_members = _members()
    next_members = tuple(sorted((*previous_members[:-1], "NEWUSDT")))
    previous = FakeDecision(members=previous_members, universe_hash="a" * 64)
    decision = FakeDecision(members=next_members, universe_hash="b" * 64)
    service = _service(previous, decision)

    await service._apply_midnight_boundary(datetime(2026, 8, 11, tzinfo=UTC))

    assert service._sources.updates == [next_members]  # type: ignore[attr-defined]
    assert service._sources.rebalances == 0  # type: ignore[attr-defined]
    assert service._rebalance_requested.is_set()  # type: ignore[attr-defined]
    assert service._gaps.opened_symbols == [("NEWUSDT", previous_members[-1])]  # type: ignore[attr-defined]
    assert service._gaps.closed_symbols == [("NEWUSDT", previous_members[-1])]  # type: ignore[attr-defined]
    assert service._gaps.affected_from_ns == [1_786_406_400_000_000_000]  # type: ignore[attr-defined]
    assert service._ingest.rotations == ["b" * 64, "b" * 64]  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_membership_timeout_keeps_only_changed_gap_open_and_seals_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_members = _members()
    next_members = tuple(sorted((*previous_members[:-1], "NEWUSDT")))
    previous = FakeDecision(members=previous_members, universe_hash="a" * 64)
    decision = FakeDecision(members=next_members, universe_hash="b" * 64)
    service = _service(previous, decision)
    sources = MembershipTimeoutSources()
    service._sources = sources

    await service._apply_midnight_boundary(datetime(2026, 8, 11, tzinfo=UTC))

    assert not service._stop.is_set()
    assert service._pending_membership == (
        next_members,
        "gap-boundary",
        ("NEWUSDT", previous_members[-1]),
    )
    assert service._gaps.closed_symbols == []  # type: ignore[attr-defined]
    assert service._day_index.seals == [date(2026, 8, 10)]  # type: ignore[attr-defined]

    original_close = service._gaps.close  # type: ignore[attr-defined]

    async def close_and_stop(*args: object, **kwargs: object) -> None:
        await original_close(*args, **kwargs)
        service._stop.set()

    sources.fail_updates = False
    service._gaps.close = close_and_stop  # type: ignore[attr-defined]
    monkeypatch.setattr("miry.collector.service.MEMBERSHIP_RETRY_SECONDS", (0,))

    await asyncio.wait_for(service._membership_loop(), timeout=0.5)

    assert service._pending_membership is None
    assert service._gaps.closed_symbols == [  # type: ignore[attr-defined]
        ("NEWUSDT", previous_members[-1])
    ]


@pytest.mark.asyncio
async def test_recovered_storage_gap_does_not_restart_running_sources() -> None:
    service = _storage_service(
        SpoolStatus(used_bytes=100, free_bytes=1_000, hard_limited=False),
        sources_running=True,
        storage_gap_id="gap-stale",
    )

    await service._storage_loop()

    assert service._sources.start_calls == 0
    assert service._sources.stop_calls == 0
    assert service._gaps.closed == [("gap-stale", GapReason.STORAGE_EXHAUSTED)]
    assert service._storage_gap_id is None


@pytest.mark.asyncio
async def test_existing_storage_gap_stops_sources_when_limit_is_still_hard() -> None:
    service = _storage_service(
        SpoolStatus(used_bytes=1_000, free_bytes=100, hard_limited=True),
        sources_running=True,
        storage_gap_id="gap-stale",
    )

    await service._storage_loop()

    assert service._sources.start_calls == 0
    assert service._sources.stop_calls == 1
    assert service._gaps.closed == []
    assert service._storage_gap_id == "gap-stale"


@pytest.mark.asyncio
async def test_storage_recovery_readiness_timeout_remains_retryable() -> None:
    service = _storage_service(
        SpoolStatus(used_bytes=100, free_bytes=1_000, hard_limited=False),
        sources_running=False,
        storage_gap_id="gap-stale",
        ready_error=TimeoutError("sources did not become ready"),
    )

    await service._storage_loop()

    assert service._sources.start_calls == 1
    assert service._sources.ready_calls == 1
    assert service._sources.stop_calls == 1
    assert service._sources.running is False
    assert service._gaps.closed == []
    assert service._storage_gap_id == "gap-stale"


@pytest.mark.asyncio
async def test_storage_recovery_source_failure_remains_fatal() -> None:
    service = _storage_service(
        SpoolStatus(used_bytes=100, free_bytes=1_000, hard_limited=False),
        sources_running=False,
        storage_gap_id="gap-stale",
        ready_error=RuntimeError("source group failed"),
    )

    with pytest.raises(RuntimeError, match="source group failed"):
        await service._storage_loop()

    assert service._gaps.closed == []
    assert service._storage_gap_id == "gap-stale"


def _service(previous: object, decision: object | None) -> Any:
    service: Any = object.__new__(CollectorService)
    service._sources = OnlineSources()
    service._gaps = BoundaryGaps()
    service._universe_store = Universe(previous, decision)
    service._ingest = RecordingIngest()
    service._day_index = RecordingDayIndex()
    service._lease = RecordingLease()
    service._config = SimpleNamespace(day_seal_grace_seconds=0, queue_resume_ratio=0.5)
    service._queues = SimpleNamespace(utilization=0.0)
    service._stop = asyncio.Event()
    service._rebalance_requested = asyncio.Event()
    service._membership_requested = asyncio.Event()
    service._pending_membership = None
    service._writers = SimpleNamespace(metrics=SimpleNamespace(durable_through_ns=1))
    service._service_started_ns = 1
    service._control_events = []

    async def record_control(_stream_type: StreamType, _payload: bytes) -> None:
        service._control_events.append(_stream_type)

    service._emit_control_event = record_control
    return service


def _storage_service(
    status: SpoolStatus,
    *,
    sources_running: bool,
    storage_gap_id: str | None,
    ready_error: BaseException | None = None,
) -> Any:
    service: Any = object.__new__(CollectorService)
    service._stop = asyncio.Event()
    service._operation_lock = asyncio.Lock()
    service._sources = StorageSources(running=sources_running, ready_error=ready_error)
    service._spool = StorageSpool(status, service._stop)
    service._gaps = StorageGaps()
    service._storage_gap_id = storage_gap_id
    service._stale_gaps = ()
    service._universe_store = SimpleNamespace(active=SimpleNamespace(members=_members()))
    service._config = SimpleNamespace(storage_check_seconds=0)
    service._formal_start_path = SimpleNamespace(exists=lambda: True)

    async def no_op() -> None:
        return None

    service._close_stale_gaps = no_op
    service._seal_completed_days = no_op
    service._mark_formal_start = no_op
    return service


def _members() -> tuple[str, ...]:
    return tuple(f"S{index:03}USDT" for index in range(60))
