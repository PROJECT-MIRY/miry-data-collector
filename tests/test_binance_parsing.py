from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace

import orjson
import pytest

from miry.collector.rest import BinanceRestClient
from miry.collector.websocket import (
    BinanceWebSocketConnection,
    SourceIdentity,
    decode_websocket,
)
from miry.collector.ws_control import SubscriptionAuditError, SubscriptionUpdate
from miry.contracts.models import RawEvent, StreamType
from miry.pipeline.parsing import logical_identity, parse_typed_row


@asynccontextmanager
async def fake_connection(websocket: object) -> AsyncIterator[object]:
    yield websocket


class StalledWebSocket:
    def __init__(self) -> None:
        self.subscription_id: int | None = None
        self.receive_calls = 0
        self.stalled = asyncio.Event()
        self.sent_methods: list[str] = []

    async def send(self, value: str) -> None:
        message = orjson.loads(value)
        self.sent_methods.append(str(message["method"]))
        self.subscription_id = int(message["id"])

    async def recv(self, *, decode: bool) -> bytes:
        assert decode is False
        self.receive_calls += 1
        if self.receive_calls == 1:
            return orjson.dumps({"result": None, "id": self.subscription_id})
        self.stalled.set()
        await asyncio.Future()
        raise AssertionError("unreachable")


class MissingSubscriptionWebSocket:
    def __init__(self) -> None:
        self.responses: asyncio.Queue[bytes] = asyncio.Queue()

    async def send(self, value: str) -> None:
        message = orjson.loads(value)
        request_id = int(message["id"])
        result: object = None if message["method"] == "SUBSCRIBE" else []
        await self.responses.put(orjson.dumps({"result": result, "id": request_id}))

    async def recv(self, *, decode: bool) -> bytes:
        assert decode is False
        return await self.responses.get()


class MissingAuditResponseWebSocket:
    def __init__(self) -> None:
        self.responses: asyncio.Queue[bytes] = asyncio.Queue()
        self.audit_requests = 0

    async def send(self, value: str) -> None:
        message = orjson.loads(value)
        if message["method"] == "SUBSCRIBE":
            await self.responses.put(orjson.dumps({"result": None, "id": message["id"]}))
        elif message["method"] == "LIST_SUBSCRIPTIONS":
            self.audit_requests += 1

    async def recv(self, *, decode: bool) -> bytes:
        assert decode is False
        try:
            return self.responses.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(0.001)
            return orjson.dumps(
                {
                    "stream": "btcusdt@aggTrade",
                    "data": {
                        "e": "aggTrade",
                        "E": 1,
                        "T": 1,
                        "s": "BTCUSDT",
                        "a": 1,
                        "p": "1",
                        "q": "1",
                        "f": 1,
                        "l": 1,
                        "m": False,
                    },
                }
            )


class RecoveringAuditWebSocket(MissingAuditResponseWebSocket):
    def __init__(self) -> None:
        super().__init__()
        self.recovered = asyncio.Event()

    async def send(self, value: str) -> None:
        message = orjson.loads(value)
        if message["method"] == "SUBSCRIBE":
            await self.responses.put(orjson.dumps({"result": None, "id": message["id"]}))
            return
        if message["method"] != "LIST_SUBSCRIPTIONS":
            return
        self.audit_requests += 1
        if self.audit_requests == 2:
            await self.responses.put(
                orjson.dumps({"result": ["btcusdt@aggTrade"], "id": message["id"]})
            )
            self.recovered.set()


class RecoveryWebSocket:
    def __init__(self) -> None:
        self.responses: asyncio.Queue[bytes] = asyncio.Queue()
        self.waiting_for_depth = asyncio.Event()
        self.release_depth = asyncio.Event()
        self.depth_sent = False

    async def send(self, value: str) -> None:
        message = orjson.loads(value)
        if message["method"] != "SUBSCRIBE":
            return
        await self.responses.put(orjson.dumps({"result": None, "id": message["id"]}))
        await self.responses.put(
            orjson.dumps(
                {
                    "stream": "btcusdt@bookTicker",
                    "data": {"e": "bookTicker", "s": "BTCUSDT"},
                }
            )
        )

    async def recv(self, *, decode: bool) -> bytes:
        assert decode is False
        if not self.responses.empty():
            return self.responses.get_nowait()
        if self.depth_sent:
            await asyncio.Future()
            raise AssertionError("unreachable")
        self.waiting_for_depth.set()
        await self.release_depth.wait()
        self.depth_sent = True
        return orjson.dumps(
            {
                "stream": "btcusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "s": "BTCUSDT",
                    "U": 2,
                    "u": 2,
                    "pu": 1,
                    "b": [],
                    "a": [],
                },
            }
        )


class BlockingSnapshotRest:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def fetch_snapshot(
        self, path: str, *, symbol: str
    ) -> tuple[bytes, int, int, str]:
        assert path == "/fapi/v1/depth"
        assert symbol == "BTCUSDT"
        self.started.set()
        await self.release.wait()
        return b'{"lastUpdateId":1,"bids":[],"asks":[]}', 1, 2, "snapshot"


class RecordingIngest:
    def __init__(self) -> None:
        self.events: list[RawEvent] = []

    async def put(self, event: RawEvent) -> None:
        self.events.append(event)


class BurstWebSocket:
    remote_address = ("127.0.0.1", 443)
    latency = 0.001

    def __init__(self, messages: int, stop: asyncio.Event) -> None:
        self._messages = messages
        self._stop = stop
        self._subscription_id: int | None = None
        self._received = 0
        self.payload = orjson.dumps(
            {
                "stream": "btcusdt@bookTicker",
                "data": {"e": "bookTicker", "s": "BTCUSDT"},
            }
        )

    async def send(self, value: str) -> None:
        message = orjson.loads(value)
        if message["method"] == "SUBSCRIBE":
            self._subscription_id = int(message["id"])

    async def recv(self, *, decode: bool) -> bytes:
        assert decode is False
        if self._received == 0:
            self._received += 1
            return orjson.dumps({"result": None, "id": self._subscription_id})
        self._received += 1
        if self._received == self._messages + 1:
            self._stop.set()
        return self.payload


class UpdatingWebSocket:
    remote_address = ("127.0.0.1", 443)
    latency = 0.001

    def __init__(self) -> None:
        self.responses: asyncio.Queue[bytes] = asyncio.Queue()
        self.sent_methods: list[str] = []

    async def send(self, value: str) -> None:
        message = orjson.loads(value)
        self.sent_methods.append(str(message["method"]))
        await self.responses.put(orjson.dumps({"result": None, "id": message["id"]}))

    async def recv(self, *, decode: bool) -> bytes:
        assert decode is False
        return await self.responses.get()


@pytest.mark.asyncio
async def test_snapshot_rate_slot_does_not_serialize_slow_http_requests() -> None:
    client = BinanceRestClient(
        "https://example.invalid",
        SimpleNamespace(),  # type: ignore[arg-type]
        snapshot_interval_seconds=0.01,
    )
    started = [asyncio.Event(), asyncio.Event()]
    release = asyncio.Event()
    calls = 0

    async def slow_fetch(
        path: str, *, params: dict[str, str | int] | None = None
    ) -> tuple[bytes, int, int, str]:
        nonlocal calls
        assert path == "/fapi/v1/depth"
        assert params is not None
        index = calls
        calls += 1
        started[index].set()
        await release.wait()
        return b"{}", 1, 2, str(index)

    client.fetch = slow_fetch  # type: ignore[method-assign]
    requests = [
        asyncio.create_task(client.fetch_snapshot("/fapi/v1/depth", symbol=symbol))
        for symbol in ("BTCUSDT", "ETHUSDT")
    ]
    try:
        await asyncio.wait_for(started[0].wait(), timeout=0.1)
        await asyncio.wait_for(started[1].wait(), timeout=0.1)
    finally:
        release.set()
        await asyncio.gather(*requests, return_exceptions=True)

    assert calls == 2


@pytest.mark.asyncio
async def test_snapshot_scheduler_bounds_slow_http_concurrency() -> None:
    client = BinanceRestClient(
        "https://example.invalid",
        SimpleNamespace(),  # type: ignore[arg-type]
        snapshot_interval_seconds=0.001,
        snapshot_max_concurrency=4,
    )
    four_started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def blocked_fetch(
        path: str, *, params: dict[str, str | int] | None = None
    ) -> tuple[bytes, int, int, str]:
        nonlocal calls
        calls += 1
        if calls == 4:
            four_started.set()
        await release.wait()
        return b"{}", 1, 2, path

    client.fetch = blocked_fetch  # type: ignore[method-assign]
    requests = [
        asyncio.create_task(client.fetch_snapshot("/fapi/v1/depth", symbol=str(index)))
        for index in range(6)
    ]
    try:
        await asyncio.wait_for(four_started.wait(), timeout=0.1)
        await asyncio.sleep(0.02)
        assert calls == 4
    finally:
        release.set()
        await asyncio.gather(*requests)

    assert calls == 6


@pytest.mark.asyncio
async def test_background_rest_request_waits_for_snapshot_recovery() -> None:
    client = BinanceRestClient(
        "https://example.invalid",
        SimpleNamespace(),  # type: ignore[arg-type]
        snapshot_interval_seconds=0.01,
    )
    snapshot_started = asyncio.Event()
    background_started = asyncio.Event()
    release = asyncio.Event()

    async def controlled_fetch(
        path: str, *, params: dict[str, str | int] | None = None
    ) -> tuple[bytes, int, int, str]:
        if path == "/fapi/v1/depth":
            snapshot_started.set()
            await release.wait()
        else:
            background_started.set()
        return b"{}", 1, 2, path

    client.fetch = controlled_fetch  # type: ignore[method-assign]
    snapshot = asyncio.create_task(client.fetch_snapshot("/fapi/v1/depth", symbol="BTCUSDT"))
    await asyncio.wait_for(snapshot_started.wait(), timeout=0.1)
    background = asyncio.create_task(client.fetch_background("/fapi/v1/exchangeInfo"))
    await asyncio.sleep(0.02)
    assert not background_started.is_set()

    release.set()
    await asyncio.gather(snapshot, background)
    assert background_started.is_set()


@pytest.mark.parametrize(
    ("stream", "payload", "expected_field", "expected_value"),
    [
        (
            StreamType.DEPTH,
            {
                "e": "depthUpdate",
                "E": 1,
                "T": 2,
                "s": "BTCUSDT",
                "U": 10,
                "u": 11,
                "pu": 9,
                "b": [["100", "1"]],
                "a": [["101", "2"]],
            },
            "final_update_id",
            11,
        ),
        (
            StreamType.DEPTH_SNAPSHOT,
            {"lastUpdateId": 11, "bids": [["100", "1"]], "asks": [["101", "2"]]},
            "last_update_id",
            11,
        ),
        (
            StreamType.BOOK_TICKER,
            {
                "e": "bookTicker",
                "E": 1,
                "T": 2,
                "s": "BTCUSDT",
                "u": 12,
                "b": "100",
                "B": "1",
                "a": "101",
                "A": "2",
            },
            "bid_price",
            "100",
        ),
        (
            StreamType.AGG_TRADE,
            {
                "e": "aggTrade",
                "E": 1,
                "T": 2,
                "s": "BTCUSDT",
                "a": 3,
                "p": "100",
                "q": "1",
                "f": 4,
                "l": 5,
                "m": True,
            },
            "aggregate_trade_id",
            3,
        ),
        (
            StreamType.TRADE,
            {
                "e": "trade",
                "E": 1,
                "T": 2,
                "s": "BTCUSDT",
                "t": 7,
                "p": "100",
                "q": "1",
                "m": False,
            },
            "trade_id",
            7,
        ),
        (
            StreamType.MARK_PRICE,
            {
                "e": "markPriceUpdate",
                "E": 1,
                "s": "BTCUSDT",
                "p": "100",
                "i": "99",
                "P": "0",
                "r": "-0.0001",
                "T": 8,
            },
            "funding_rate",
            "-0.0001",
        ),
        (
            StreamType.FORCE_ORDER,
            {
                "e": "forceOrder",
                "E": 1,
                "o": {
                    "s": "BTCUSDT",
                    "S": "SELL",
                    "o": "LIMIT",
                    "f": "IOC",
                    "q": "2",
                    "p": "100",
                    "ap": "99",
                    "X": "FILLED",
                    "l": "1",
                    "z": "2",
                    "T": 2,
                },
            },
            "side",
            "SELL",
        ),
        (
            StreamType.CONTRACT_INFO,
            {
                "e": "contractInfo",
                "E": 1,
                "s": "BTCUSDT",
                "ct": "PERPETUAL",
                "dt": 0,
                "ot": 1,
                "cs": "TRADING",
            },
            "contract_status",
            "TRADING",
        ),
        (
            StreamType.OPEN_INTEREST,
            {"symbol": "BTCUSDT", "openInterest": "123.45", "time": 2},
            "open_interest",
            "123.45",
        ),
        (
            StreamType.CLOCK_SAMPLE,
            {"serverTime": 1_786_320_000_000},
            "exchange_event_time_ms",
            1_786_320_000_000,
        ),
    ],
)
def test_formal_stream_fixtures(
    stream: StreamType,
    payload: dict[str, object],
    expected_field: str,
    expected_value: object,
) -> None:
    raw_row = {
        "stream_type": stream.value,
        "exchange_symbol": "BTCUSDT",
        "connection_id": "connection-1",
        "receive_seq": 1,
        "app_receive_realtime_ns": 10,
        "app_receive_monotonic_ns": 20,
        "payload_bytes": orjson.dumps(payload),
    }
    parsed = parse_typed_row(raw_row)
    assert parsed is not None
    assert parsed[expected_field] == expected_value


@pytest.mark.parametrize(
    ("stream", "payload"),
    [
        (
            StreamType.MARK_PRICE,
            {
                "e": "markPriceUpdate",
                "E": 1,
                "s": "BTCUSDT",
                "p": "100",
                "i": "99",
                "P": "0",
                "r": "-0.0001",
                "T": 8,
            },
        ),
        (
            StreamType.FORCE_ORDER,
            {
                "e": "forceOrder",
                "E": 1,
                "o": {
                    "s": "BTCUSDT",
                    "S": "SELL",
                    "o": "LIMIT",
                    "f": "IOC",
                    "q": "2",
                    "p": "100",
                    "ap": "99",
                    "X": "FILLED",
                    "l": "1",
                    "z": "2",
                    "T": 2,
                },
            },
        ),
        (
            StreamType.CONTRACT_INFO,
            {
                "e": "contractInfo",
                "E": 1,
                "s": "BTCUSDT",
                "ct": "PERPETUAL",
                "dt": 0,
                "ot": 1,
                "cs": "TRADING",
            },
        ),
    ],
)
def test_overlap_market_events_have_connection_independent_identity(
    stream: StreamType, payload: dict[str, object]
) -> None:
    rows = []
    for connection, sequence in (("old", 10), ("replacement", 1)):
        typed = parse_typed_row(
            {
                "stream_type": stream.value,
                "exchange_symbol": "BTCUSDT",
                "connection_id": connection,
                "receive_seq": sequence,
                "app_receive_realtime_ns": sequence,
                "app_receive_monotonic_ns": sequence,
                "payload_bytes": orjson.dumps(payload),
            }
        )
        assert typed is not None
        rows.append(typed)

    assert logical_identity(rows[0]) is not None
    assert logical_identity(rows[0]) == logical_identity(rows[1])


@pytest.mark.parametrize(
    ("stream", "payload"),
    [
        (StreamType.MARKET_TICKERS, [{"symbol": "BTCUSDT", "quoteVolume": "1"}]),
        (StreamType.EXCHANGE_INFO, {"symbols": [{"symbol": "BTCUSDT"}]}),
    ],
)
def test_market_wide_payload_is_valid_discovery_evidence(
    stream: StreamType, payload: object
) -> None:
    assert (
        parse_typed_row(
            {
                "stream_type": stream.value,
                "payload_bytes": orjson.dumps(payload),
            }
        )
        is None
    )


def test_edge_depth_decode_reuses_parsed_sequence_fields() -> None:
    decoded = decode_websocket(
        orjson.dumps(
            {
                "stream": "btcusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "s": "BTCUSDT",
                    "U": 10,
                    "u": 11,
                    "pu": 9,
                    "b": [],
                    "a": [],
                },
            }
        )
    )

    assert decoded.stream_type is StreamType.DEPTH
    assert decoded.symbol == "BTCUSDT"
    assert decoded.data is not None
    assert (decoded.data["pu"], decoded.data["u"]) == (9, 11)


def test_source_identity_assigns_sequence_without_async_scheduling() -> None:
    identity = SourceIdentity("tokyo01", "boot", "segment", "connection")

    first = identity.event(
        stream_type=StreamType.AGG_TRADE,
        exchange_symbol="BTCUSDT",
        payload=b"{}",
        realtime_ns=1,
        monotonic_ns=1,
    )
    second = identity.event(
        stream_type=StreamType.AGG_TRADE,
        exchange_symbol="BTCUSDT",
        payload=b"{}",
        realtime_ns=2,
        monotonic_ns=2,
    )

    assert (first.receive_seq, second.receive_seq) == (1, 2)


@pytest.mark.asyncio
async def test_burst_receive_uses_constant_background_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    websocket = BurstWebSocket(200, stop)
    ingest = RecordingIngest()
    monkeypatch.setattr(
        "miry.collector.websocket.connect",
        lambda *args, **kwargs: fake_connection(websocket),
    )
    real_create_task = asyncio.create_task
    tasks_created = 0

    def count_task(coro: object, *args: object, **kwargs: object) -> asyncio.Task[object]:
        nonlocal tasks_created
        tasks_created += 1
        return real_create_task(coro, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr("miry.collector.websocket.asyncio.create_task", count_task)

    async def ignore_depth_gap(*args: object) -> str:
        return "gap-depth"

    async def ignore_depth_reanchored(*args: object) -> None:
        return None

    connection = BinanceWebSocketConnection(
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@bookTicker",),
        identity=SourceIdentity("tokyo01", "boot", "segment", "connection"),
        ingest=ingest,  # type: ignore[arg-type]
        snapshot_requests=(),
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        ready=asyncio.Event(),
        stop=stop,
        receive_timeout_seconds=1,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        max_queue=4,
        max_message_bytes=2 * 1024**2,
        updates=asyncio.Queue(),
        on_depth_gap=ignore_depth_gap,
        on_depth_reanchored=ignore_depth_reanchored,
        subscription_audit_seconds=60,
    )

    await connection.run()

    assert len(ingest.events) == 201
    assert tasks_created <= 12


@pytest.mark.asyncio
async def test_subscription_update_acknowledges_without_stopping_receiver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    ready = asyncio.Event()
    websocket = UpdatingWebSocket()
    updates: asyncio.Queue[SubscriptionUpdate] = asyncio.Queue()
    monkeypatch.setattr(
        "miry.collector.websocket.connect",
        lambda *args, **kwargs: fake_connection(websocket),
    )

    async def ignore_depth_gap(*args: object) -> str:
        return "gap-depth"

    async def ignore_depth_reanchored(*args: object) -> None:
        return None

    connection = BinanceWebSocketConnection(
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@bookTicker",),
        identity=SourceIdentity("tokyo01", "boot", "segment", "connection"),
        ingest=RecordingIngest(),  # type: ignore[arg-type]
        snapshot_requests=(),
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        ready=ready,
        stop=stop,
        receive_timeout_seconds=1,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        max_queue=4,
        max_message_bytes=2 * 1024**2,
        updates=updates,
        on_depth_gap=ignore_depth_gap,
        on_depth_reanchored=ignore_depth_reanchored,
        subscription_audit_seconds=60,
    )
    task = asyncio.create_task(connection.run())
    try:
        await asyncio.wait_for(ready.wait(), timeout=0.2)
        loop = asyncio.get_running_loop()
        acknowledged = loop.create_future()
        completion = loop.create_future()
        updates.put_nowait(
            SubscriptionUpdate(
                add=("ethusdt@bookTicker",),
                remove=("btcusdt@bookTicker",),
                snapshot_requests=(),
                acknowledged=acknowledged,
                completion=completion,
            )
        )
        await asyncio.wait_for(completion, timeout=0.2)
        assert acknowledged.done()
        assert websocket.sent_methods == ["SUBSCRIBE", "UNSUBSCRIBE", "SUBSCRIBE"]
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=0.2)


@pytest.mark.asyncio
async def test_websocket_silence_fails_the_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    websocket = StalledWebSocket()
    ingest = RecordingIngest()

    monkeypatch.setattr(
        "miry.collector.websocket.connect",
        lambda *args, **kwargs: fake_connection(websocket),
    )

    async def open_depth_gap(*args: object) -> str:
        return "gap-depth"

    async def close_depth_gap(*args: object) -> None:
        return None

    connection = BinanceWebSocketConnection(
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@aggTrade",),
        identity=SourceIdentity("tokyo01", "boot", "segment", "connection"),
        ingest=ingest,  # type: ignore[arg-type]
        snapshot_requests=(),
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        ready=asyncio.Event(),
        stop=asyncio.Event(),
        receive_timeout_seconds=0.01,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        max_queue=4,
        max_message_bytes=2 * 1024**2,
        updates=asyncio.Queue(),
        on_depth_gap=open_depth_gap,
        on_depth_reanchored=close_depth_gap,
    )

    task = asyncio.create_task(connection.run())
    try:
        await asyncio.wait_for(websocket.stalled.wait(), timeout=0.5)
        await asyncio.sleep(0.05)
        assert task.done(), "a silent websocket remained stuck in recv()"
        with pytest.raises(TimeoutError, match="no websocket message"):
            await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_reconnected_websocket_ignores_expired_subscription_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = StalledWebSocket()
    monkeypatch.setattr(
        "miry.collector.websocket.connect",
        lambda *args, **kwargs: fake_connection(websocket),
    )
    loop = asyncio.get_running_loop()
    acknowledged = loop.create_future()
    completion = loop.create_future()
    acknowledged.cancel()
    completion.cancel()
    updates: asyncio.Queue[SubscriptionUpdate] = asyncio.Queue()
    updates.put_nowait(
        SubscriptionUpdate(
            add=("ethusdt@markPrice@1s",),
            remove=("btcusdt@markPrice@1s",),
            snapshot_requests=(),
            acknowledged=acknowledged,
            completion=completion,
        )
    )

    async def open_depth_gap(*args: object) -> str:
        return "gap-depth"

    async def close_depth_gap(*args: object) -> None:
        return None

    connection = BinanceWebSocketConnection(
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@markPrice@1s",),
        identity=SourceIdentity("tokyo01", "boot", "segment", "connection"),
        ingest=RecordingIngest(),  # type: ignore[arg-type]
        snapshot_requests=(),
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        ready=asyncio.Event(),
        stop=asyncio.Event(),
        receive_timeout_seconds=1,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        max_queue=4,
        max_message_bytes=2 * 1024**2,
        updates=updates,
        on_depth_gap=open_depth_gap,
        on_depth_reanchored=close_depth_gap,
    )

    task = asyncio.create_task(connection.run())
    try:
        await asyncio.wait_for(websocket.stalled.wait(), timeout=0.5)
        assert websocket.sent_methods == ["SUBSCRIBE"]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_transport_recovers_before_l2_snapshots_finish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = RecoveryWebSocket()
    rest = BlockingSnapshotRest()
    snapshot_ready = asyncio.Event()
    transport_ready = asyncio.Event()
    stop = asyncio.Event()
    monkeypatch.setattr(
        "miry.collector.websocket.connect",
        lambda *args, **kwargs: fake_connection(websocket),
    )

    async def open_depth_gap(*args: object) -> str:
        return "gap-depth"

    async def close_depth_gap(*args: object) -> None:
        return None

    connection = BinanceWebSocketConnection(
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@bookTicker", "btcusdt@depth@100ms"),
        identity=SourceIdentity("tokyo01", "boot", "segment", "connection"),
        ingest=RecordingIngest(),  # type: ignore[arg-type]
        snapshot_requests=(("BTCUSDT", StreamType.DEPTH_SNAPSHOT),),
        rest=rest,  # type: ignore[arg-type]
        ready=snapshot_ready,
        transport_ready=transport_ready,
        transport_ready_keys=(
            (StreamType.BOOK_TICKER, "BTCUSDT"),
            (StreamType.DEPTH, "BTCUSDT"),
        ),
        stop=stop,
        receive_timeout_seconds=1,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        max_queue=4,
        max_message_bytes=2 * 1024**2,
        updates=asyncio.Queue(),
        on_depth_gap=open_depth_gap,
        on_depth_reanchored=close_depth_gap,
    )

    task = asyncio.create_task(connection.run())
    try:
        await asyncio.wait_for(rest.started.wait(), timeout=0.5)
        await asyncio.wait_for(websocket.waiting_for_depth.wait(), timeout=0.5)
        assert not transport_ready.is_set()
        websocket.release_depth.set()
        await asyncio.wait_for(transport_ready.wait(), timeout=0.5)
        assert not snapshot_ready.is_set()
        rest.release.set()
        await asyncio.wait_for(snapshot_ready.wait(), timeout=0.5)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_subscription_audit_fails_when_one_stream_disappears(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = MissingSubscriptionWebSocket()
    monkeypatch.setattr(
        "miry.collector.websocket.connect",
        lambda *args, **kwargs: fake_connection(websocket),
    )

    async def open_depth_gap(*args: object) -> str:
        return "gap-depth"

    async def close_depth_gap(*args: object) -> None:
        return None

    connection = BinanceWebSocketConnection(
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@aggTrade", "btcusdt@markPrice@1s"),
        identity=SourceIdentity("tokyo01", "boot", "segment", "connection"),
        ingest=RecordingIngest(),  # type: ignore[arg-type]
        snapshot_requests=(),
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        ready=asyncio.Event(),
        stop=asyncio.Event(),
        receive_timeout_seconds=1,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        max_queue=4,
        max_message_bytes=2 * 1024**2,
        updates=asyncio.Queue(),
        on_depth_gap=open_depth_gap,
        on_depth_reanchored=close_depth_gap,
        subscription_audit_seconds=0.01,
        subscription_audit_timeout_seconds=0.01,
    )

    with pytest.raises(SubscriptionAuditError, match="subscription audit mismatch") as captured:
        await asyncio.wait_for(connection.run(), timeout=0.5)
    assert captured.value.affected_from_realtime_ns > 0


@pytest.mark.asyncio
async def test_subscription_audit_response_cannot_silently_disappear(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = MissingAuditResponseWebSocket()
    monkeypatch.setattr(
        "miry.collector.websocket.connect",
        lambda *args, **kwargs: fake_connection(websocket),
    )

    async def open_depth_gap(*args: object) -> str:
        return "gap-depth"

    async def close_depth_gap(*args: object) -> None:
        return None

    connection = BinanceWebSocketConnection(
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@aggTrade",),
        identity=SourceIdentity("tokyo01", "boot", "segment", "connection"),
        ingest=RecordingIngest(),  # type: ignore[arg-type]
        snapshot_requests=(),
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        ready=asyncio.Event(),
        stop=asyncio.Event(),
        receive_timeout_seconds=1,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        max_queue=4,
        max_message_bytes=2 * 1024**2,
        updates=asyncio.Queue(),
        on_depth_gap=open_depth_gap,
        on_depth_reanchored=close_depth_gap,
        subscription_audit_seconds=0.01,
        subscription_audit_timeout_seconds=0.01,
        subscription_audit_failures_before_reconnect=3,
    )

    with pytest.raises(SubscriptionAuditError, match="audit response was not received") as captured:
        await asyncio.wait_for(connection.run(), timeout=0.5)
    assert captured.value.affected_from_realtime_ns > 0
    assert websocket.audit_requests == 3


@pytest.mark.asyncio
async def test_subscription_audit_recovers_after_one_missing_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = RecoveringAuditWebSocket()
    monkeypatch.setattr(
        "miry.collector.websocket.connect",
        lambda *args, **kwargs: fake_connection(websocket),
    )
    stop = asyncio.Event()

    async def ignore_depth_gap(*args: object) -> str:
        return "gap-depth"

    async def ignore_depth_reanchored(*args: object) -> None:
        return None

    connection = BinanceWebSocketConnection(
        url="wss://example.invalid/stream",
        subscriptions=("btcusdt@aggTrade",),
        identity=SourceIdentity("tokyo01", "boot", "segment", "connection"),
        ingest=RecordingIngest(),  # type: ignore[arg-type]
        snapshot_requests=(),
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        ready=asyncio.Event(),
        stop=stop,
        receive_timeout_seconds=1,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        max_queue=4,
        max_message_bytes=2 * 1024**2,
        updates=asyncio.Queue(),
        on_depth_gap=ignore_depth_gap,
        on_depth_reanchored=ignore_depth_reanchored,
        subscription_audit_seconds=0.01,
        subscription_audit_timeout_seconds=0.01,
        subscription_audit_failures_before_reconnect=3,
    )

    task = asyncio.create_task(connection.run())
    try:
        await asyncio.wait_for(websocket.recovered.wait(), timeout=0.2)
        await asyncio.sleep(0.01)
        assert not task.done()
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=0.2)
