from __future__ import annotations

import asyncio
import gzip
from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import orjson
import pytest

from miry.collector.config import load_collector_config
from miry.collector.polling import RestPollers
from miry.collector.readiness import required_realtime_sources
from miry.collector.routes import ConnectionHandle, RouteRunner, _reconnect_delay
from miry.collector.scheduling import advance_fixed_deadline, staggered_offsets
from miry.collector.sharding import TrafficSharder
from miry.collector.sources import SourceManager, SourceUpdateError
from miry.collector.websocket import SourceIdentity, public_subscriptions
from miry.contracts.models import GapReason, RawEvent, StreamType

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class FakeRest:
    def __init__(self) -> None:
        self.calls: Counter[str] = Counter()

    async def fetch(
        self, path: str, *, params: dict[str, str | int] | None = None
    ) -> tuple[bytes, int, int, str]:
        assert path == "/fapi/v1/openInterest"
        assert params is not None
        symbol = str(params["symbol"])
        self.calls[symbol] += 1
        if symbol == "ETHUSDT" and self.calls[symbol] == 1:
            raise aiohttp.ClientConnectionError("temporary failure")
        return b'{"symbol":"BTCUSDT","openInterest":"1","time":1}', 1, 2, symbol


class FakeIngest:
    def __init__(self, stop: asyncio.Event) -> None:
        self.events: list[RawEvent] = []
        self._stop = stop

    async def put(self, event: RawEvent) -> None:
        self.events.append(event)
        if {item.exchange_symbol for item in self.events} == {"BTCUSDT", "ETHUSDT"}:
            self._stop.set()


class DailyKlineRest:
    def __init__(self) -> None:
        self.params: list[dict[str, str | int]] = []

    async def fetch(
        self, path: str, *, params: dict[str, str | int] | None = None
    ) -> tuple[bytes, int, int, str]:
        assert path == "/fapi/v1/klines"
        assert params is not None
        self.params.append(params)
        day_ms = 86_400_000
        rows = [
            [
                open_ms,
                "1",
                "1",
                "1",
                "1",
                "1",
                open_ms + day_ms - 1,
                "10000000",
                100000,
            ]
            for open_ms in range(int(params["startTime"]), int(params["endTime"]) + 1, day_ms)
        ][: int(params["limit"])]
        return orjson.dumps(rows), 1, 2, f"request-{len(self.params)}"

    async def fetch_background(
        self, path: str, *, params: dict[str, str | int] | None = None
    ) -> tuple[bytes, int, int, str]:
        return await self.fetch(path, params=params)


class RecordingIngest:
    def __init__(self) -> None:
        self.events: list[RawEvent] = []

    async def put(self, event: RawEvent) -> None:
        self.events.append(event)


class FakeQueues:
    async def wait_until_resumable(self) -> None:
        return


class FakeGaps:
    def __init__(self) -> None:
        self.opened: list[tuple[GapReason, tuple[str, ...], tuple[StreamType, ...]]] = []
        self.closed: list[tuple[str, GapReason, tuple[str, ...], tuple[StreamType, ...]]] = []

    async def open(
        self,
        reason: GapReason,
        *,
        exchange_symbols: tuple[str, ...],
        stream_types: tuple[StreamType, ...],
        affected_from_realtime_ns: int | None = None,
        detail: str,
    ) -> str:
        self.opened.append((reason, exchange_symbols, stream_types))
        return "gap-ethusdt"

    async def close(
        self,
        gap_id: str,
        reason: GapReason,
        *,
        exchange_symbols: tuple[str, ...],
        stream_types: tuple[StreamType, ...],
        detail: str,
    ) -> None:
        self.closed.append((gap_id, reason, exchange_symbols, stream_types))


class RouteGaps:
    def __init__(self) -> None:
        self.opened: list[tuple[GapReason, str | None]] = []
        self.closed: list[tuple[str, GapReason, str | None]] = []

    async def open(
        self,
        reason: GapReason,
        *,
        connection_id: str | None = None,
        exchange_symbols: tuple[str, ...],
        stream_types: tuple[StreamType, ...],
        affected_from_realtime_ns: int | None = None,
        detail: str,
    ) -> str:
        self.opened.append((reason, connection_id))
        return "gap-connection"

    async def close(
        self,
        gap_id: str,
        reason: GapReason,
        *,
        connection_id: str | None = None,
        exchange_symbols: tuple[str, ...],
        stream_types: tuple[StreamType, ...],
        detail: str,
    ) -> None:
        self.closed.append((gap_id, reason, connection_id))


class SequencedRouteGaps:
    def __init__(self) -> None:
        self.opened: list[tuple[str, GapReason, str | None]] = []
        self.closed: list[tuple[str, GapReason, str | None]] = []

    async def open(
        self,
        reason: GapReason,
        *,
        connection_id: str | None = None,
        exchange_symbols: tuple[str, ...],
        stream_types: tuple[StreamType, ...],
        affected_from_realtime_ns: int | None = None,
        detail: str,
    ) -> str:
        gap_id = f"gap-{len(self.opened) + 1}"
        self.opened.append((gap_id, reason, connection_id))
        return gap_id

    async def close(
        self,
        gap_id: str,
        reason: GapReason,
        *,
        connection_id: str | None = None,
        exchange_symbols: tuple[str, ...],
        stream_types: tuple[StreamType, ...],
        detail: str,
    ) -> None:
        self.closed.append((gap_id, reason, connection_id))


@pytest.mark.asyncio
async def test_open_interest_failure_is_tracked_per_symbol() -> None:
    stop = asyncio.Event()
    ingest = FakeIngest(stop)
    gaps = FakeGaps()
    ready: list[str] = []

    async def ignore_discovery(value: object) -> None:
        return None

    pollers = RestPollers(
        config=SimpleNamespace(
            open_interest_interval_seconds=0.01,
            open_interest_startup_spread_seconds=0.01,
        ),  # type: ignore[arg-type]
        instruments=("BTCUSDT", "ETHUSDT"),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=ingest,  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=gaps,  # type: ignore[arg-type]
        rest=FakeRest(),  # type: ignore[arg-type]
        stop=stop,
        on_ready=ready.append,
        on_discovery=ignore_discovery,
    )

    await asyncio.wait_for(pollers._open_interest_loop(), timeout=1)

    assert gaps.opened == [(GapReason.CONNECTION_LOST, ("ETHUSDT",), (StreamType.OPEN_INTEREST,))]
    assert gaps.closed == [
        (
            "gap-ethusdt",
            GapReason.CONNECTION_LOST,
            ("ETHUSDT",),
            (StreamType.OPEN_INTEREST,),
        )
    ]
    assert ready == ["open_interest"]


def test_fixed_rate_deadline_skips_missed_slots_without_drifting() -> None:
    assert advance_fixed_deadline(100.0, 30.0, 101.0) == 130.0
    assert advance_fixed_deadline(100.0, 30.0, 170.0) == 190.0


def test_realtime_readiness_does_not_wait_for_nightly_discovery() -> None:
    assert required_realtime_sources(4) == {
        "public-0",
        "public-1",
        "public-2",
        "public-3",
        "market-0",
        "open_interest",
        "clock",
    }


def test_startup_poll_offsets_fit_inside_bounded_window() -> None:
    offsets = staggered_offsets(60, 5)

    assert offsets[0] == 0
    assert offsets[-1] < 5
    assert offsets == tuple(sorted(offsets))


@pytest.mark.asyncio
async def test_daily_kline_evidence_appends_only_the_new_complete_day(
    tmp_path: Path,
) -> None:
    config_path = PROJECT_ROOT / "deploy/vultr/edge.yaml.example"
    config = load_collector_config(config_path).model_copy(update={"data_root": tmp_path})
    rest = DailyKlineRest()
    ingest = RecordingIngest()
    stop = asyncio.Event()

    async def ignore_discovery(value: object) -> None:
        return None

    pollers = RestPollers(
        config=config,
        instruments=("BTCUSDT",),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=ingest,  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=SimpleNamespace(),  # type: ignore[arg-type]
        rest=rest,  # type: ignore[arg-type]
        stop=stop,
        on_ready=lambda _: None,
        on_discovery=ignore_discovery,
    )
    exchange_info = orjson.dumps(
        {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "onboardDate": 1,
                }
            ]
        }
    )
    first = await pollers._fetch_daily_klines(
        exchange_info, datetime(2026, 8, 17, 23, 50, tzinfo=UTC)
    )
    cache = tmp_path / "control/universe/observations/2026-08-17/daily-klines.json.gz"
    cache.parent.mkdir(parents=True)
    cache.write_bytes(gzip.compress(first))

    second = await pollers._fetch_daily_klines(
        exchange_info, datetime(2026, 8, 18, 23, 50, tzinfo=UTC)
    )

    assert [value["limit"] for value in rest.params] == [35, 1]
    payload = orjson.loads(second)
    assert len(payload["symbols"]["BTCUSDT"]["payload"]) == 35
    assert [event.stream_type for event in ingest.events] == [
        StreamType.DAILY_KLINES,
        StreamType.DAILY_KLINES,
    ]


@pytest.mark.asyncio
async def test_live_update_only_changes_replaced_symbol_subscriptions() -> None:
    stop = asyncio.Event()
    initial = tuple(f"S{index:03}USDT" for index in range(60))
    proposed = tuple(sorted((*initial[:-1], "NEWUSDT")))
    runner = RouteRunner(
        name="public-0",
        url="wss://example.invalid/stream",
        subscriptions=public_subscriptions(initial, d0_enabled=False),
        instruments=initial,
        stream_types=(StreamType.BOOK_TICKER, StreamType.DEPTH),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=SimpleNamespace(),  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=stop,
        subscriptions_for=lambda values: public_subscriptions(values, d0_enabled=False),
    )

    updating = asyncio.create_task(runner.update_instruments(proposed))
    update = await asyncio.wait_for(runner._updates.get(), timeout=0.5)
    assert set(update.remove) == {
        "s059usdt@bookTicker",
        "s059usdt@depth@100ms",
    }
    assert set(update.add) == {
        "newusdt@bookTicker",
        "newusdt@depth@100ms",
    }
    assert update.snapshot_requests == (("NEWUSDT", StreamType.DEPTH_SNAPSHOT),)
    update.started.set_result(None)
    update.acknowledged.set_result(None)
    update.completion.set_result(None)
    await asyncio.sleep(0)
    assert not updating.done()
    runner._mark_event(StreamType.BOOK_TICKER, "NEWUSDT")
    runner._mark_event(StreamType.DEPTH, "NEWUSDT")
    await updating
    assert runner.instruments == proposed


@pytest.mark.asyncio
async def test_live_update_counts_events_received_while_snapshot_is_pending() -> None:
    runner = RouteRunner(
        name="public-0",
        url="wss://example.invalid/stream",
        subscriptions=public_subscriptions(("BTCUSDT",), d0_enabled=False),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.BOOK_TICKER, StreamType.DEPTH),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=SimpleNamespace(),  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=asyncio.Event(),
        subscriptions_for=lambda values: public_subscriptions(values, d0_enabled=False),
    )

    updating = asyncio.create_task(runner.update_instruments(("BTCUSDT", "ETHUSDT")))
    update = await asyncio.wait_for(runner._updates.get(), timeout=0.5)
    runner._mark_event(StreamType.BOOK_TICKER, "ETHUSDT")
    runner._mark_event(StreamType.DEPTH, "ETHUSDT")
    update.started.set_result(None)
    update.acknowledged.set_result(None)
    update.completion.set_result(None)

    await updating
    assert runner.instruments == ("BTCUSDT", "ETHUSDT")


@pytest.mark.asyncio
async def test_source_manager_moves_symbols_in_add_ready_remove_phases() -> None:
    expanded: set[str] = set()

    class PlannedSharder:
        def copy(self) -> PlannedSharder:
            return self

        def shards(self, _instruments: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
            return (("AUSDT", "CUSDT"), ("BUSDT", "DUSDT"))

    class RecordingRoute:
        def __init__(self, name: str, instruments: tuple[str, ...]) -> None:
            self.name = name
            self.instruments = instruments
            self.calls: list[tuple[str, ...]] = []

        async def update_instruments(self, instruments: tuple[str, ...]) -> None:
            self.calls.append(instruments)
            if len(self.calls) == 1:
                expanded.add(self.name)
            else:
                assert expanded == {"public-0", "public-1"}
            self.instruments = instruments

    class RecordingPollers:
        async def update_instruments(self, _instruments: tuple[str, ...]) -> None:
            return None

    manager = object.__new__(SourceManager)
    manager._task = asyncio.current_task()
    manager._pollers = RecordingPollers()
    manager._update_lock = asyncio.Lock()
    manager._public_sharder = PlannedSharder()
    manager._instruments = ("AUSDT", "BUSDT", "CUSDT", "DUSDT")
    public_0 = RecordingRoute("public-0", ("AUSDT", "BUSDT"))
    public_1 = RecordingRoute("public-1", ("CUSDT", "DUSDT"))
    manager._routes = {
        "public-0": public_0,
        "public-1": public_1,
        "market-0": RecordingRoute(
            "market-0", ("AUSDT", "BUSDT", "CUSDT", "DUSDT")
        ),
    }

    await manager.update_instruments(manager._instruments)

    assert public_0.calls == [
        ("AUSDT", "BUSDT", "CUSDT"),
        ("AUSDT", "CUSDT"),
    ]
    assert public_1.calls == [
        ("BUSDT", "CUSDT", "DUSDT"),
        ("BUSDT", "DUSDT"),
    ]


@pytest.mark.asyncio
async def test_source_manager_rebalances_once_with_complete_traffic_evidence() -> None:
    class CompleteTraffic:
        has_complete_evidence = True

        def effective_rates(self) -> dict[str, int]:
            return {"AUSDT": 100, "BUSDT": 90, "CUSDT": 10, "DUSDT": 5}

    class RecordingRoute:
        def __init__(self, instruments: tuple[str, ...]) -> None:
            self.instruments = instruments
            self.calls: list[tuple[str, ...]] = []

        async def update_instruments(self, instruments: tuple[str, ...]) -> None:
            self.calls.append(instruments)
            self.instruments = instruments

    manager = object.__new__(SourceManager)
    manager._task = asyncio.current_task()
    manager._stop = asyncio.Event()
    manager._update_lock = asyncio.Lock()
    manager._config = SimpleNamespace(public_connection_shards=2)
    manager._traffic = CompleteTraffic()
    manager._instruments = ("AUSDT", "BUSDT", "CUSDT", "DUSDT")
    manager._public_sharder = TrafficSharder(2, CompleteTraffic().effective_rates())
    manager._public_sharder._assignments = {  # type: ignore[attr-defined]
        "AUSDT": 0,
        "BUSDT": 0,
        "CUSDT": 1,
        "DUSDT": 1,
    }
    public_0 = RecordingRoute(("AUSDT", "BUSDT"))
    public_1 = RecordingRoute(("CUSDT", "DUSDT"))
    manager._routes = {"public-0": public_0, "public-1": public_1}

    assert await manager.rebalance_public_routes()
    assert public_0.calls == [
        ("AUSDT", "BUSDT", "CUSDT"),
        ("BUSDT", "CUSDT"),
    ]
    assert public_1.calls == [
        ("AUSDT", "CUSDT", "DUSDT"),
        ("AUSDT", "DUSDT"),
    ]

    assert not await manager.rebalance_public_routes()
    assert len(public_0.calls) == 2
    assert len(public_1.calls) == 2


@pytest.mark.asyncio
async def test_source_manager_rolls_back_expansion_before_any_trim_on_failure() -> None:
    class PlannedSharder:
        def copy(self) -> PlannedSharder:
            return self

        def shards(self, _instruments: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
            return (("AUSDT", "CUSDT"), ("BUSDT", "DUSDT"))

    class RecordingRoute:
        def __init__(self, instruments: tuple[str, ...], *, fail_first: bool = False) -> None:
            self.instruments = instruments
            self.fail_first = fail_first
            self.calls: list[tuple[str, ...]] = []

        async def update_instruments(self, instruments: tuple[str, ...]) -> None:
            self.calls.append(instruments)
            if self.fail_first and len(self.calls) == 1:
                raise OSError("subscription update failed")
            self.instruments = instruments

    class RecordingPollers:
        async def update_instruments(self, _instruments: tuple[str, ...]) -> None:
            return None

    manager = object.__new__(SourceManager)
    manager._task = asyncio.current_task()
    manager._pollers = RecordingPollers()
    manager._update_lock = asyncio.Lock()
    manager._public_sharder = PlannedSharder()
    manager._instruments = ("AUSDT", "BUSDT", "CUSDT", "DUSDT")
    public_0 = RecordingRoute(("AUSDT", "BUSDT"), fail_first=True)
    public_1 = RecordingRoute(("CUSDT", "DUSDT"))
    manager._routes = {
        "public-0": public_0,
        "public-1": public_1,
        "market-0": RecordingRoute(manager._instruments),
    }

    with pytest.raises(RuntimeError, match="before old subscriptions were removed"):
        await manager.update_instruments(manager._instruments)

    assert ("AUSDT", "CUSDT") not in public_0.calls
    assert ("BUSDT", "DUSDT") not in public_1.calls
    assert public_1.instruments == ("CUSDT", "DUSDT")


@pytest.mark.asyncio
async def test_source_manager_keeps_expanded_coverage_when_trim_fails() -> None:
    class CompleteTraffic:
        has_complete_evidence = True

        def effective_rates(self) -> dict[str, int]:
            return {"AUSDT": 100, "BUSDT": 90, "CUSDT": 10, "DUSDT": 5}

    class RecordingRoute:
        def __init__(self, instruments: tuple[str, ...], *, fail_trim: bool = False) -> None:
            self.instruments = instruments
            self.fail_trim = fail_trim
            self.calls: list[tuple[str, ...]] = []

        async def update_instruments(self, instruments: tuple[str, ...]) -> None:
            self.calls.append(instruments)
            if self.fail_trim and len(self.calls) == 2:
                raise TimeoutError("trim acknowledgement timed out")
            self.instruments = instruments

    manager = object.__new__(SourceManager)
    manager._task = asyncio.current_task()
    manager._stop = asyncio.Event()
    manager._update_lock = asyncio.Lock()
    manager._config = SimpleNamespace(public_connection_shards=2)
    manager._traffic = CompleteTraffic()
    manager._instruments = ("AUSDT", "BUSDT", "CUSDT", "DUSDT")
    old_sharder = TrafficSharder(2, CompleteTraffic().effective_rates())
    old_sharder._assignments = {  # type: ignore[attr-defined]
        "AUSDT": 0,
        "BUSDT": 0,
        "CUSDT": 1,
        "DUSDT": 1,
    }
    manager._public_sharder = old_sharder
    public_0 = RecordingRoute(("AUSDT", "BUSDT"), fail_trim=True)
    public_1 = RecordingRoute(("CUSDT", "DUSDT"))
    manager._routes = {"public-0": public_0, "public-1": public_1}

    with pytest.raises(SourceUpdateError, match="expanded coverage remains active"):
        await manager.rebalance_public_routes()

    assert set(public_0.instruments) | set(public_1.instruments) == set(manager._instruments)
    assert public_0.instruments == ("AUSDT", "BUSDT", "CUSDT")
    assert public_1.instruments == ("AUSDT", "DUSDT")
    assert manager._public_sharder is old_sharder

    public_0.fail_trim = False
    assert await manager.rebalance_public_routes()
    assert public_0.instruments == ("AUSDT", "BUSDT")
    assert public_1.instruments == ("CUSDT", "DUSDT")


@pytest.mark.asyncio
async def test_targeted_refresh_only_changes_stale_public_streams() -> None:
    runner = RouteRunner(
        name="public-0",
        url="wss://example.invalid/stream",
        subscriptions=(
            "btcusdt@bookTicker",
            "btcusdt@depth@100ms",
            "ethusdt@bookTicker",
            "ethusdt@depth@100ms",
        ),
        instruments=("BTCUSDT", "ETHUSDT"),
        stream_types=(StreamType.BOOK_TICKER, StreamType.DEPTH),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=SimpleNamespace(),  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=asyncio.Event(),
        subscriptions_for=lambda values: public_subscriptions(values, d0_enabled=False),
    )

    refreshing = asyncio.create_task(
        runner._refresh_keys(
            (
                (StreamType.DEPTH, "BTCUSDT"),
                (StreamType.BOOK_TICKER, "ETHUSDT"),
            )
        )
    )
    update = await asyncio.wait_for(runner._updates.get(), timeout=0.5)

    assert update.remove == ("btcusdt@depth@100ms", "ethusdt@bookTicker")
    assert update.add == update.remove
    assert update.snapshot_requests == (("BTCUSDT", StreamType.DEPTH_SNAPSHOT),)
    update.started.set_result(None)
    update.acknowledged.set_result(None)
    update.completion.set_result(None)
    await refreshing


@pytest.mark.asyncio
async def test_targeted_mark_price_refresh_does_not_touch_trade_streams() -> None:
    runner = RouteRunner(
        name="market-0",
        url="wss://example.invalid/stream",
        subscriptions=(
            "btcusdt@aggTrade",
            "btcusdt@markPrice@1s",
            "btcusdt@forceOrder",
            "!contractInfo",
        ),
        instruments=("BTCUSDT",),
        stream_types=(
            StreamType.AGG_TRADE,
            StreamType.MARK_PRICE,
            StreamType.FORCE_ORDER,
            StreamType.CONTRACT_INFO,
        ),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=SimpleNamespace(),  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=asyncio.Event(),
        subscriptions_for=lambda values: tuple(
            f"{symbol.lower()}@markPrice@1s" for symbol in values
        ),
    )

    refreshing = asyncio.create_task(
        runner._refresh_keys(((StreamType.MARK_PRICE, "BTCUSDT"),))
    )
    update = await asyncio.wait_for(runner._updates.get(), timeout=0.5)

    assert update.remove == ("btcusdt@markPrice@1s",)
    assert update.add == update.remove
    assert update.snapshot_requests == ()
    update.started.set_result(None)
    update.acknowledged.set_result(None)
    update.completion.set_result(None)
    await refreshing


@pytest.mark.asyncio
async def test_subscription_ack_deadline_is_separate_from_snapshot_completion() -> None:
    runner = RouteRunner(
        name="public-0",
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@depth@100ms",),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.DEPTH,),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=SimpleNamespace(),  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=asyncio.Event(),
        subscriptions_for=lambda values: tuple(
            f"{symbol.lower()}@depth@100ms" for symbol in values
        ),
        subscription_audit_timeout_seconds=0.01,
    )

    refreshing = asyncio.create_task(
        runner._refresh_keys(((StreamType.DEPTH, "BTCUSDT"),))
    )
    update = await asyncio.wait_for(runner._updates.get(), timeout=0.5)
    update.started.set_result(None)
    update.acknowledged.set_result(None)

    await asyncio.sleep(0.02)
    assert not refreshing.done(), "snapshot completion inherited the control ACK deadline"

    update.completion.set_result(None)
    await refreshing


@pytest.mark.asyncio
async def test_subscription_ack_deadline_starts_after_control_queue_wait() -> None:
    runner = RouteRunner(
        name="public-0",
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@depth@100ms",),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.DEPTH,),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=SimpleNamespace(),  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=asyncio.Event(),
        subscriptions_for=lambda values: tuple(
            f"{symbol.lower()}@depth@100ms" for symbol in values
        ),
        subscription_audit_timeout_seconds=0.01,
    )

    refreshing = asyncio.create_task(
        runner._refresh_keys(((StreamType.DEPTH, "BTCUSDT"),))
    )
    update = await asyncio.wait_for(runner._updates.get(), timeout=0.5)

    await asyncio.sleep(0.02)
    assert not refreshing.done(), "queue wait consumed the network ACK deadline"

    update.started.set_result(None)
    update.acknowledged.set_result(None)
    update.completion.set_result(None)
    await refreshing


@pytest.mark.asyncio
async def test_liveness_detects_depth_silence_while_book_ticker_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    stop = asyncio.Event()
    gaps = FakeGaps()
    monkeypatch.setattr("miry.collector.routes.time.monotonic", lambda: clock[0])
    runner = RouteRunner(
        name="public-0",
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@bookTicker", "btcusdt@depth@100ms"),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.BOOK_TICKER, StreamType.DEPTH),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=gaps,  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=stop,
        subscriptions_for=lambda values: public_subscriptions(values, d0_enabled=False),
        liveness_timeout_seconds=120,
    )

    waits = 0

    async def advance_clock(_seconds: float) -> None:
        nonlocal waits
        waits += 1
        if waits == 1:
            clock[0] = 121.0
            runner._mark_event(StreamType.BOOK_TICKER, "BTCUSDT")
        else:
            stop.set()

    async def refresh(keys: tuple[tuple[StreamType, str], ...]) -> None:
        assert keys == ((StreamType.DEPTH, "BTCUSDT"),)
        stop.set()

    monkeypatch.setattr(runner, "_wait_or_stop", advance_clock)
    monkeypatch.setattr(runner, "_refresh_keys", refresh)

    await asyncio.wait_for(runner.liveness_loop(), timeout=0.5)

    assert gaps.opened == [(GapReason.CONNECTION_LOST, ("BTCUSDT",), (StreamType.DEPTH,))]


@pytest.mark.asyncio
async def test_liveness_gap_stays_open_until_the_stream_proves_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    gaps = FakeGaps()
    runner = RouteRunner(
        name="market-0",
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@markPrice@1s",),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.MARK_PRICE,),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=gaps,  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=stop,
        subscriptions_for=lambda values: tuple(
            f"{symbol.lower()}@markPrice@1s" for symbol in values
        ),
        liveness_timeout_seconds=0.01,
        liveness_stream_types=(StreamType.MARK_PRICE,),
    )
    runner._connection_generation = 1
    runner._connection_ready.set()
    waits = 0

    async def recover_then_stop(_seconds: float) -> None:
        nonlocal waits
        waits += 1
        if waits == 1:
            runner._last_event[(StreamType.MARK_PRICE, "BTCUSDT")] = (
                runner._last_event[(StreamType.MARK_PRICE, "BTCUSDT")][0] - 1,
                1,
            )
        elif waits == 2:
            runner._mark_event(StreamType.MARK_PRICE, "BTCUSDT")
        else:
            stop.set()

    async def refresh_without_event(_keys: tuple[tuple[StreamType, str], ...]) -> None:
        return None

    async def no_fresh_event(*_args: object, **_kwargs: object) -> None:
        raise TimeoutError("stream remained silent")

    monkeypatch.setattr(runner, "_wait_or_stop", recover_then_stop)
    monkeypatch.setattr(runner, "_refresh_keys", refresh_without_event)
    monkeypatch.setattr(runner, "_wait_for_fresh_events", no_fresh_event)

    await asyncio.wait_for(runner.liveness_loop(), timeout=0.5)

    assert gaps.opened == [(GapReason.CONNECTION_LOST, ("BTCUSDT",), (StreamType.MARK_PRICE,))]
    assert gaps.closed == [
        (
            "gap-ethusdt",
            GapReason.CONNECTION_LOST,
            ("BTCUSDT",),
            (StreamType.MARK_PRICE,),
        )
    ]
    assert not runner._reconnect_requested.is_set()


@pytest.mark.asyncio
async def test_liveness_refresh_timeout_keeps_route_monitor_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    stop = asyncio.Event()
    gaps = FakeGaps()
    monkeypatch.setattr("miry.collector.routes.time.monotonic", lambda: clock[0])
    runner = RouteRunner(
        name="market-0",
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@markPrice@1s",),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.MARK_PRICE,),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=gaps,  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=stop,
        subscriptions_for=lambda values: tuple(
            f"{symbol.lower()}@markPrice@1s" for symbol in values
        ),
        liveness_timeout_seconds=5,
        liveness_stream_types=(StreamType.MARK_PRICE,),
    )
    runner._connection_generation = 1
    runner._connection_ready.set()

    waits = 0
    refreshes = 0

    async def advance_then_stop(_seconds: float) -> None:
        nonlocal waits
        waits += 1
        if waits == 1:
            clock[0] = 6.0
        elif waits == 3:
            stop.set()

    async def timeout_refresh(_keys: tuple[tuple[StreamType, str], ...]) -> None:
        nonlocal refreshes
        refreshes += 1
        raise TimeoutError("targeted refresh timed out")

    monkeypatch.setattr(runner, "_wait_or_stop", advance_then_stop)
    monkeypatch.setattr(runner, "_refresh_keys", timeout_refresh)

    await asyncio.wait_for(runner.liveness_loop(), timeout=0.5)

    assert waits == 3
    assert refreshes == 2
    assert gaps.opened == [
        (GapReason.CONNECTION_LOST, ("BTCUSDT",), (StreamType.MARK_PRICE,))
    ]
    assert gaps.closed == []
    assert runner._reconnect_requested.is_set()


@pytest.mark.asyncio
async def test_stale_refresh_failure_cannot_reconnect_a_new_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    stop = asyncio.Event()
    gaps = FakeGaps()
    monkeypatch.setattr("miry.collector.routes.time.monotonic", lambda: clock[0])
    runner = RouteRunner(
        name="market-0",
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@markPrice@1s",),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.MARK_PRICE,),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=gaps,  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=stop,
        subscriptions_for=lambda values: tuple(
            f"{symbol.lower()}@markPrice@1s" for symbol in values
        ),
        liveness_timeout_seconds=5,
        liveness_stream_types=(StreamType.MARK_PRICE,),
        refresh_failures_before_reconnect=2,
    )
    runner._connection_generation = 1
    runner._connection_ready.set()
    waits = 0

    async def advance_then_stop(_seconds: float) -> None:
        nonlocal waits
        waits += 1
        if waits == 1:
            clock[0] = 6.0
        else:
            stop.set()

    async def fail_after_replacement(
        _keys: tuple[tuple[StreamType, str], ...],
    ) -> None:
        runner._connection_generation += 1
        runner._connection_ready.clear()
        raise TimeoutError("old connection refresh expired")

    monkeypatch.setattr(runner, "_wait_or_stop", advance_then_stop)
    monkeypatch.setattr(runner, "_refresh_keys", fail_after_replacement)

    await asyncio.wait_for(runner.liveness_loop(), timeout=0.5)

    assert not runner._reconnect_requested.is_set()


def test_reconnect_backoff_is_exponential_and_capped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "miry.collector.routes.random.uniform",
        lambda _minimum, _maximum: 1.0,
    )

    assert [_reconnect_delay(failures) for failures in range(1, 8)] == [
        1.0,
        2.0,
        4.0,
        8.0,
        16.0,
        30.0,
        30.0,
    ]


@pytest.mark.asyncio
async def test_route_timeout_opens_gap_and_recovery_closes_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    gaps = RouteGaps()
    runner = RouteRunner(
        name="market-0",
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@aggTrade",),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.AGG_TRADE,),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=gaps,  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=stop,
    )
    starts = 0

    async def fail_silently() -> None:
        raise TimeoutError("no websocket message for 30s")

    async def wait_until_cancelled() -> None:
        await asyncio.Event().wait()

    async def start_connection(
        *,
        on_transport_ready: Callable[[SourceIdentity], Awaitable[None]] | None = None,
    ) -> ConnectionHandle:
        nonlocal starts
        starts += 1
        identity = SourceIdentity("tokyo01", "boot", f"segment-{starts}", f"connection-{starts}")
        if starts == 1:
            task = asyncio.create_task(fail_silently())
        else:
            task = asyncio.create_task(wait_until_cancelled())
            if on_transport_ready is not None:
                await on_transport_ready(identity)
            asyncio.get_running_loop().call_soon(stop.set)
        return ConnectionHandle(identity, asyncio.Event(), asyncio.Event(), task)

    async def skip_retry_delay(seconds: float) -> None:
        return None

    monkeypatch.setattr(runner, "_start_ready_connection", start_connection)
    monkeypatch.setattr(runner, "_wait_or_stop", skip_retry_delay)

    await asyncio.wait_for(runner.run(), timeout=0.5)

    assert gaps.opened == [(GapReason.CONNECTION_LOST, "connection-1")]
    assert gaps.closed == [("gap-connection", GapReason.CONNECTION_LOST, "connection-2")]


@pytest.mark.asyncio
async def test_targeted_recovery_failure_reconnects_only_the_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    gaps = RouteGaps()
    runner = RouteRunner(
        name="market-0",
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@markPrice@1s",),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.MARK_PRICE,),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=gaps,  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=stop,
    )
    starts = 0

    async def wait_for_stop() -> None:
        await stop.wait()

    async def wait_until_cancelled() -> None:
        await asyncio.Event().wait()

    async def start_connection(
        *,
        on_transport_ready: Callable[[SourceIdentity], Awaitable[None]] | None = None,
    ) -> ConnectionHandle:
        nonlocal starts
        starts += 1
        identity = SourceIdentity(
            "tokyo01", "boot", f"segment-{starts}", f"connection-{starts}"
        )
        if starts == 1:
            asyncio.get_running_loop().call_soon(runner._reconnect_requested.set)
            task = asyncio.create_task(wait_until_cancelled())
        else:
            assert on_transport_ready is not None
            await on_transport_ready(identity)
            asyncio.get_running_loop().call_soon(stop.set)
            task = asyncio.create_task(wait_for_stop())
        return ConnectionHandle(identity, asyncio.Event(), asyncio.Event(), task)

    async def skip_retry_delay(_seconds: float) -> None:
        return None

    monkeypatch.setattr(runner, "_start_ready_connection", start_connection)
    monkeypatch.setattr(runner, "_wait_or_stop", skip_retry_delay)

    await asyncio.wait_for(runner.run(), timeout=0.5)

    assert starts == 2
    assert gaps.opened == [(GapReason.CONNECTION_LOST, "connection-1")]
    assert gaps.closed == [("gap-connection", GapReason.CONNECTION_LOST, "connection-2")]


@pytest.mark.asyncio
async def test_route_reports_transport_recovery_before_snapshots_finish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    snapshot_release = asyncio.Event()
    transport_recovered = asyncio.Event()
    observed_keys: set[tuple[StreamType, str]] = set()

    class ControlledConnection:
        def __init__(self, **kwargs: object) -> None:
            self.ready = kwargs["ready"]
            self.transport_ready = kwargs["transport_ready"]
            self.stop = kwargs["stop"]
            observed_keys.update(kwargs["transport_ready_keys"])  # type: ignore[arg-type]

        async def run(self) -> None:
            self.transport_ready.set()  # type: ignore[union-attr]
            await snapshot_release.wait()
            self.ready.set()  # type: ignore[union-attr]
            await self.stop.wait()  # type: ignore[union-attr]

    monkeypatch.setattr(
        "miry.collector.routes.BinanceWebSocketConnection",
        ControlledConnection,
    )
    runner = RouteRunner(
        name="public-0",
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@bookTicker", "btcusdt@depth@100ms"),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.BOOK_TICKER, StreamType.DEPTH),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=SimpleNamespace(),  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=stop,
        liveness_timeout_seconds=30,
        liveness_stream_types=(StreamType.BOOK_TICKER, StreamType.DEPTH),
    )

    async def report_transport_recovery(identity: SourceIdentity) -> None:
        assert identity.connection_id.startswith("public-0-")
        transport_recovered.set()

    starting = asyncio.create_task(
        runner._start_ready_connection(on_transport_ready=report_transport_recovery)
    )
    handle: ConnectionHandle | None = None
    try:
        await asyncio.wait_for(transport_recovered.wait(), timeout=0.5)
        assert not starting.done()
        assert observed_keys == {
            (StreamType.BOOK_TICKER, "BTCUSDT"),
            (StreamType.DEPTH, "BTCUSDT"),
        }
        snapshot_release.set()
        handle = await asyncio.wait_for(starting, timeout=0.5)
    finally:
        stop.set()
        snapshot_release.set()
        if not starting.done():
            starting.cancel()
        await asyncio.gather(starting, return_exceptions=True)
    assert handle is not None
    handle.stop.set()
    await asyncio.wait_for(handle.task, timeout=0.5)


@pytest.mark.asyncio
async def test_snapshot_failure_after_transport_recovery_opens_a_new_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    gaps = SequencedRouteGaps()
    runner = RouteRunner(
        name="public-0",
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@bookTicker", "btcusdt@depth@100ms"),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.BOOK_TICKER, StreamType.DEPTH),
        collector_id="tokyo01",
        boot_id="boot",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=FakeQueues(),  # type: ignore[arg-type]
        gaps=gaps,  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=stop,
    )
    starts = 0

    async def fail_connection() -> None:
        raise OSError("abrupt close")

    async def wait_for_stop() -> None:
        await stop.wait()

    async def start_connection(
        *,
        on_transport_ready: Callable[[SourceIdentity], Awaitable[None]] | None = None,
    ) -> ConnectionHandle:
        nonlocal starts
        starts += 1
        identity = SourceIdentity("tokyo01", "boot", f"segment-{starts}", f"connection-{starts}")
        if starts == 1:
            return ConnectionHandle(
                identity,
                asyncio.Event(),
                asyncio.Event(),
                asyncio.create_task(fail_connection()),
            )
        assert on_transport_ready is not None
        await on_transport_ready(identity)
        if starts == 2:
            raise OSError("snapshot failed after transport recovery")
        asyncio.get_running_loop().call_soon(stop.set)
        return ConnectionHandle(
            identity,
            asyncio.Event(),
            asyncio.Event(),
            asyncio.create_task(wait_for_stop()),
        )

    async def skip_retry_delay(_seconds: float) -> None:
        return None

    monkeypatch.setattr(runner, "_start_ready_connection", start_connection)
    monkeypatch.setattr(runner, "_wait_or_stop", skip_retry_delay)

    await asyncio.wait_for(runner.run(), timeout=0.5)

    assert gaps.opened == [
        ("gap-1", GapReason.CONNECTION_LOST, "connection-1"),
        ("gap-2", GapReason.L2_REANCHOR, "connection-1"),
        ("gap-3", GapReason.CONNECTION_LOST, None),
    ]
    assert gaps.closed == [
        ("gap-1", GapReason.CONNECTION_LOST, "connection-2"),
        ("gap-3", GapReason.CONNECTION_LOST, "connection-3"),
        ("gap-2", GapReason.L2_REANCHOR, "connection-3"),
    ]
