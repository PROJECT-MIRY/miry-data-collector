from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import aiohttp
import msgspec
import orjson
from websockets.asyncio.client import ClientConnection, connect

from miry.collector.ingest import IngestCoordinator
from miry.collector.rest import BinanceRestClient
from miry.collector.ws_control import (
    ControlRequest,
    SubscriptionController,
    SubscriptionUpdate,
)
from miry.contracts.models import RawEvent, StreamType
from miry.orderbook.bridge import BridgeStatus, SnapshotBridgeTracker, UpdateSpan

logger = logging.getLogger(__name__)

RECEIVE_FAIRNESS_BATCH = 64
SNAPSHOT_BRIDGE_ATTEMPTS = 5


class SnapshotBridgeError(OSError):
    pass


class _LiquidationOrder(msgspec.Struct, frozen=True, gc=False):
    symbol: str | None = msgspec.field(default=None, name="s")


class _WebSocketData(msgspec.Struct, frozen=True, gc=False):
    event_type: str = msgspec.field(name="e")
    symbol: str | None = msgspec.field(default=None, name="s")
    first_update_id: int | None = msgspec.field(default=None, name="U")
    final_update_id: int | None = msgspec.field(default=None, name="u")
    previous_final_update_id: int | None = msgspec.field(default=None, name="pu")
    order: _LiquidationOrder | None = msgspec.field(default=None, name="o")


class _CombinedWebSocket(msgspec.Struct, frozen=True, gc=False):
    stream: str
    data: _WebSocketData


COMBINED_DECODER = msgspec.json.Decoder(_CombinedWebSocket)
EVENT_STREAM_TYPES = {
    "bookTicker": StreamType.BOOK_TICKER,
    "aggTrade": StreamType.AGG_TRADE,
    "trade": StreamType.TRADE,
    "markPriceUpdate": StreamType.MARK_PRICE,
    "forceOrder": StreamType.FORCE_ORDER,
    "contractInfo": StreamType.CONTRACT_INFO,
}


@dataclass(frozen=True, slots=True)
class DepthUpdateIds:
    first: int
    final: int
    previous: int


@dataclass(frozen=True, slots=True)
class DecodedWebSocket:
    stream_type: StreamType
    symbol: str | None
    message: dict[str, Any] | None
    depth_update: DepthUpdateIds | dict[str, Any] | None


def decode_websocket(raw: bytes) -> DecodedWebSocket:
    try:
        combined = COMBINED_DECODER.decode(raw)
    except msgspec.DecodeError:
        return _decode_websocket_fallback(raw)
    data = combined.data
    symbol_value = (
        data.order.symbol
        if data.event_type == "forceOrder" and data.order
        else data.symbol
    )
    symbol = symbol_value.upper() if symbol_value else None
    if data.event_type == "depthUpdate":
        if (
            data.first_update_id is None
            or data.final_update_id is None
            or data.previous_final_update_id is None
        ):
            return _decode_websocket_fallback(raw)
        stream_type = (
            StreamType.RPI_DEPTH if "@rpidepth@" in combined.stream.lower() else StreamType.DEPTH
        )
        return DecodedWebSocket(
            stream_type,
            symbol,
            None,
            DepthUpdateIds(
                data.first_update_id,
                data.final_update_id,
                data.previous_final_update_id,
            ),
        )
    return DecodedWebSocket(
        EVENT_STREAM_TYPES.get(data.event_type, StreamType.UNKNOWN), symbol, None, None
    )


def _decode_websocket_fallback(raw: bytes) -> DecodedWebSocket:
    try:
        message = orjson.loads(raw)
    except orjson.JSONDecodeError:
        return DecodedWebSocket(StreamType.UNKNOWN, None, None, None)
    if not isinstance(message, dict):
        return DecodedWebSocket(StreamType.UNKNOWN, None, None, None)
    if "result" in message or ("id" in message and "data" not in message):
        return DecodedWebSocket(StreamType.WS_CONTROL, None, message, None)
    stream = str(message.get("stream", "")).lower()
    data = message.get("data", message)
    if not isinstance(data, dict):
        return DecodedWebSocket(StreamType.UNKNOWN, None, message, None)
    event_type = str(data.get("e", ""))
    symbol_value = data.get("s")
    if event_type == "forceOrder" and isinstance(data.get("o"), dict):
        symbol_value = data["o"].get("s")
    symbol = str(symbol_value).upper() if symbol_value else None

    if event_type == "depthUpdate":
        stream_type = StreamType.RPI_DEPTH if "@rpidepth@" in stream else StreamType.DEPTH
        return DecodedWebSocket(stream_type, symbol, None, data)
    return DecodedWebSocket(
        EVENT_STREAM_TYPES.get(event_type, StreamType.UNKNOWN), symbol, None, None
    )


@dataclass(slots=True)
class SourceIdentity:
    collector_id: str
    boot_id: str
    segment_id: str
    connection_id: str
    _sequence: int = 0

    def event(
        self,
        *,
        stream_type: StreamType,
        exchange_symbol: str | None,
        payload: bytes,
        realtime_ns: int,
        monotonic_ns: int,
        request_id: str | None = None,
        request_realtime_ns: int | None = None,
    ) -> RawEvent:
        self._sequence += 1
        return RawEvent(
            schema_version=1,
            exchange_symbol=exchange_symbol,
            stream_type=stream_type,
            collector_id=self.collector_id,
            boot_id=self.boot_id,
            segment_id=self.segment_id,
            connection_id=self.connection_id,
            receive_seq=self._sequence,
            app_receive_realtime_ns=realtime_ns,
            app_receive_monotonic_ns=monotonic_ns,
            payload_bytes=payload,
            request_id=request_id,
            request_realtime_ns=request_realtime_ns,
        )


class BinanceWebSocketConnection:
    def __init__(
        self,
        *,
        url: str,
        subscriptions: tuple[str, ...],
        identity: SourceIdentity,
        ingest: IngestCoordinator,
        snapshot_requests: tuple[tuple[str, StreamType], ...],
        rest: BinanceRestClient,
        ready: asyncio.Event,
        stop: asyncio.Event,
        receive_timeout_seconds: float,
        ping_interval_seconds: float,
        ping_timeout_seconds: float,
        max_queue: int,
        max_message_bytes: int,
        updates: asyncio.Queue[SubscriptionUpdate],
        on_depth_gap: Callable[[str, str, StreamType, int, int, int], Awaitable[str]],
        on_depth_reanchored: Callable[[str, str, StreamType], Awaitable[None]],
        on_event: Callable[[StreamType, str | None, int, int], None] | None = None,
        subscription_audit_seconds: float = 60.0,
        subscription_audit_timeout_seconds: float = 10.0,
        subscription_audit_failures_before_reconnect: int = 1,
        transport_ready: asyncio.Event | None = None,
        transport_ready_keys: tuple[tuple[StreamType, str], ...] = (),
    ) -> None:
        self._url = url
        self._subscriptions = subscriptions
        self._identity = identity
        self._ingest = ingest
        self._snapshot_requests = snapshot_requests
        self._rest = rest
        self._ready = ready
        self._stop = stop
        self._receive_timeout_seconds = receive_timeout_seconds
        self._ping_interval_seconds = ping_interval_seconds
        self._ping_timeout_seconds = ping_timeout_seconds
        self._max_queue = max_queue
        self._max_message_bytes = max_message_bytes
        self._updates = updates
        self._on_depth_gap = on_depth_gap
        self._on_depth_reanchored = on_depth_reanchored
        self._on_event = on_event or (
            lambda _stream, _symbol, _realtime_ns, _monotonic_ns: None
        )
        self._subscription_audit_seconds = subscription_audit_seconds
        self._subscription_audit_timeout_seconds = subscription_audit_timeout_seconds
        self._subscription_audit_failures_before_reconnect = (
            subscription_audit_failures_before_reconnect
        )
        self._transport_ready = (
            transport_ready if transport_ready is not None else asyncio.Event()
        )
        self._transport_pending = set(transport_ready_keys)
        self._initial_subscription_acknowledged = False
        self._bridges: dict[tuple[StreamType, str], SnapshotBridgeTracker] = {}
        self._bridge_changed: dict[tuple[StreamType, str], asyncio.Event] = {}
        self._resync_tasks: dict[tuple[StreamType, str], asyncio.Task[None]] = {}
        self._snapshot_pending = set(snapshot_requests)
        self._last_receive_monotonic = time.monotonic()
        self._background_failure: asyncio.Future[None] | None = None

    async def run(self) -> None:
        tasks: list[asyncio.Task[Any]] = []
        control: SubscriptionController | None = None
        self._background_failure = asyncio.get_running_loop().create_future()
        try:
            async with connect(
                self._url,
                ping_interval=self._ping_interval_seconds,
                ping_timeout=self._ping_timeout_seconds,
                max_queue=self._max_queue,
                max_size=self._max_message_bytes,
                close_timeout=10,
            ) as websocket:
                peer = _peer_text(getattr(websocket, "remote_address", None))
                logger.info(
                    "websocket connected connection_id=%s peer=%s subscriptions=%d",
                    self._identity.connection_id,
                    peer,
                    len(self._subscriptions),
                )
                control = SubscriptionController(
                    websocket=websocket,
                    subscriptions=self._subscriptions,
                    updates=self._updates,
                    connection_id=self._identity.connection_id,
                    peer=peer,
                    audit_seconds=self._subscription_audit_seconds,
                    audit_timeout_seconds=self._subscription_audit_timeout_seconds,
                    audit_failures_before_reconnect=(
                        self._subscription_audit_failures_before_reconnect
                    ),
                    recover_snapshots=self._fetch_requested_snapshots,
                )
                self._last_receive_monotonic = time.monotonic()
                initial_subscription_started = time.monotonic()
                initial_request = await control.send_initial()
                receiver = asyncio.create_task(
                    self._receive_loop(websocket, control),
                    name=f"receiver-{self._identity.connection_id}",
                )
                watchdog = asyncio.create_task(
                    self._receive_watchdog(),
                    name=f"receive-watchdog-{self._identity.connection_id}",
                )
                stop_task = asyncio.create_task(
                    self._stop.wait(), name=f"connection-stop-{self._identity.connection_id}"
                )
                bootstrap = asyncio.create_task(
                    self._bootstrap(control, initial_request, initial_subscription_started),
                    name=f"bootstrap-{self._identity.connection_id}",
                )
                tasks.extend((receiver, watchdog, stop_task, bootstrap))
                if not await _wait_for_phase(
                    success=bootstrap,
                    stop_task=stop_task,
                    failures=(receiver, watchdog, self._background_failure),
                ):
                    return
                tasks.remove(bootstrap)
                updates = asyncio.create_task(
                    control.run_updates(),
                    name=f"subscription-updates-{self._identity.connection_id}",
                )
                audit = asyncio.create_task(
                    control.run_audits(),
                    name=f"subscription-audit-{self._identity.connection_id}",
                )
                tasks.extend((updates, audit))
                await _wait_for_phase(
                    success=None,
                    stop_task=stop_task,
                    failures=(
                        receiver,
                        watchdog,
                        updates,
                        audit,
                        self._background_failure,
                    ),
                )
        finally:
            if control is not None:
                control.close()
            for task in tasks:
                task.cancel()
            resync_tasks = list(self._resync_tasks.values())
            for task in resync_tasks:
                task.cancel()
            await asyncio.gather(*tasks, *resync_tasks, return_exceptions=True)
            if self._background_failure is not None:
                if not self._background_failure.done():
                    self._background_failure.cancel()
                await asyncio.gather(self._background_failure, return_exceptions=True)
            self._resync_tasks.clear()
            self._background_failure = None

    async def _receive_loop(
        self, websocket: ClientConnection, control: SubscriptionController
    ) -> None:
        received_since_yield = 0
        while not self._stop.is_set():
            raw = await websocket.recv(decode=False)
            realtime_ns = time.time_ns()
            monotonic_ns = time.monotonic_ns()
            self._last_receive_monotonic = monotonic_ns / 1_000_000_000
            if not isinstance(raw, bytes):
                raw = raw.encode()
            decoded = decode_websocket(raw)
            event = self._identity.event(
                stream_type=decoded.stream_type,
                exchange_symbol=decoded.symbol,
                payload=raw,
                realtime_ns=realtime_ns,
                monotonic_ns=monotonic_ns,
            )
            control_error: Exception | None = None
            if decoded.stream_type is StreamType.WS_CONTROL and decoded.message:
                try:
                    control.deliver(decoded.message, realtime_ns)
                except Exception as exc:
                    control_error = exc
            await self._ingest.put(event)
            if control_error is not None:
                raise control_error
            self._on_event(
                decoded.stream_type,
                decoded.symbol,
                realtime_ns,
                monotonic_ns,
            )
            self._transport_pending.discard((decoded.stream_type, decoded.symbol or ""))
            self._maybe_mark_transport_ready()
            if (
                decoded.stream_type in {StreamType.DEPTH, StreamType.RPI_DEPTH}
                and decoded.symbol
                and decoded.depth_update is not None
            ):
                await self._observe_depth_update(
                    decoded.stream_type,
                    decoded.symbol,
                    decoded.depth_update,
                    realtime_ns,
                    event.receive_seq,
                )
            received_since_yield += 1
            if received_since_yield >= RECEIVE_FAIRNESS_BATCH:
                received_since_yield = 0
                await asyncio.sleep(0)

    async def _bootstrap(
        self,
        control: SubscriptionController,
        initial_request: ControlRequest,
        started: float,
    ) -> None:
        await control.complete_initial(initial_request, started=started)
        self._initial_subscription_acknowledged = True
        self._maybe_mark_transport_ready()
        if self._snapshot_requests:
            await self._fetch_snapshots()
        self._ready.set()

    async def _receive_watchdog(self) -> None:
        interval = max(0.001, min(1.0, self._receive_timeout_seconds / 4))
        while True:
            await asyncio.sleep(interval)
            if time.monotonic() - self._last_receive_monotonic >= self._receive_timeout_seconds:
                raise TimeoutError(
                    "no websocket message for "
                    f"{self._receive_timeout_seconds:g}s "
                    f"connection_id={self._identity.connection_id}"
                )

    def _maybe_mark_transport_ready(self) -> None:
        if self._initial_subscription_acknowledged and not self._transport_pending:
            self._transport_ready.set()

    async def _fetch_requested_snapshots(
        self,
        requests: tuple[tuple[str, StreamType], ...],
    ) -> None:
        self._snapshot_pending.update(requests)
        await asyncio.gather(
            *(
                self._recover_snapshot_bridge(symbol, stream_type)
                for symbol, stream_type in requests
            )
        )

    async def _fetch_snapshots(self) -> None:
        await asyncio.gather(
            *(
                self._recover_snapshot_bridge(symbol, stream_type)
                for symbol, stream_type in self._snapshot_requests
            )
        )

    async def _fetch_snapshot(
        self, symbol: str, stream_type: StreamType, *, bridge_attempt: int
    ) -> int:
        delay = 1.0
        for attempt in range(5):
            try:
                is_rpi = stream_type is StreamType.RPI_DEPTH_SNAPSHOT
                path = "/fapi/v1/rpiDepth" if is_rpi else "/fapi/v1/depth"
                payload, requested_at, observed_at, request_id = await self._rest.fetch_snapshot(
                    path, symbol=symbol
                )
                event = self._identity.event(
                    stream_type=stream_type,
                    exchange_symbol=symbol,
                    payload=payload,
                    realtime_ns=observed_at,
                    monotonic_ns=time.monotonic_ns(),
                    request_id=request_id,
                    request_realtime_ns=requested_at,
                )
                await self._ingest.put(event)
                logger.info(
                    "snapshot fetched connection_id=%s symbol=%s stream=%s latency_ms=%.3f "
                    "http_attempt=%d bridge_attempt=%d",
                    self._identity.connection_id,
                    symbol,
                    stream_type.value,
                    (observed_at - requested_at) / 1_000_000,
                    attempt + 1,
                    bridge_attempt,
                )
                decoded = orjson.loads(payload)
                if not isinstance(decoded, dict):
                    raise ValueError("depth snapshot is not an object")
                return int(decoded["lastUpdateId"])
            except (aiohttp.ClientError, TimeoutError):
                if attempt == 4:
                    raise
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10)
        raise AssertionError("snapshot HTTP retry loop did not return or raise")

    async def _recover_snapshot_bridge(
        self, symbol: str, snapshot_type: StreamType
    ) -> None:
        stream_type = _stream_type_for_snapshot(snapshot_type)
        key = (stream_type, symbol)
        tracker = self._bridges.setdefault(key, SnapshotBridgeTracker())
        changed = self._bridge_changed.setdefault(key, asyncio.Event())
        self._snapshot_pending.add((symbol, snapshot_type))
        for bridge_attempt in range(1, SNAPSHOT_BRIDGE_ATTEMPTS + 1):
            await self._wait_for_depth_buffer(tracker, changed, symbol, stream_type)
            last_update_id = await self._fetch_snapshot(
                symbol, snapshot_type, bridge_attempt=bridge_attempt
            )
            result = tracker.on_snapshot(last_update_id)
            if result.status is BridgeStatus.BRIDGED:
                self._snapshot_pending.discard((symbol, snapshot_type))
                logger.info(
                    "snapshot bridged connection_id=%s symbol=%s stream=%s "
                    "last_update_id=%d bridge_attempt=%d",
                    self._identity.connection_id,
                    symbol,
                    snapshot_type.value,
                    last_update_id,
                    bridge_attempt,
                )
                return
            if result.status is BridgeStatus.STALE_SNAPSHOT:
                logger.warning(
                    "snapshot rejected stale connection_id=%s symbol=%s stream=%s "
                    "last_update_id=%d bridge_attempt=%d",
                    self._identity.connection_id,
                    symbol,
                    snapshot_type.value,
                    last_update_id,
                    bridge_attempt,
                )
                continue
            try:
                await self._wait_for_snapshot_decision(tracker, changed)
            except TimeoutError:
                logger.warning(
                    "snapshot bridge wait timed out connection_id=%s symbol=%s stream=%s "
                    "last_update_id=%d bridge_attempt=%d",
                    self._identity.connection_id,
                    symbol,
                    snapshot_type.value,
                    last_update_id,
                    bridge_attempt,
                )
                tracker.invalidate()
                continue
            if tracker.is_bridged:
                self._snapshot_pending.discard((symbol, snapshot_type))
                logger.info(
                    "snapshot bridged connection_id=%s symbol=%s stream=%s "
                    "last_update_id=%d bridge_attempt=%d",
                    self._identity.connection_id,
                    symbol,
                    snapshot_type.value,
                    last_update_id,
                    bridge_attempt,
                )
                return
        raise SnapshotBridgeError(
            f"snapshot did not bridge after {SNAPSHOT_BRIDGE_ATTEMPTS} attempts "
            f"connection_id={self._identity.connection_id} symbol={symbol}"
        )

    async def _wait_for_depth_buffer(
        self,
        tracker: SnapshotBridgeTracker,
        changed: asyncio.Event,
        symbol: str,
        stream_type: StreamType,
    ) -> None:
        while not tracker.has_events:
            changed.clear()
            if tracker.has_events:
                return
            try:
                await asyncio.wait_for(
                    changed.wait(), timeout=self._receive_timeout_seconds
                )
            except TimeoutError as exc:
                raise SnapshotBridgeError(
                    "no depth event available before snapshot "
                    f"connection_id={self._identity.connection_id} "
                    f"symbol={symbol} stream={stream_type.value}"
                ) from exc

    async def _wait_for_snapshot_decision(
        self, tracker: SnapshotBridgeTracker, changed: asyncio.Event
    ) -> None:
        async with asyncio.timeout(self._receive_timeout_seconds):
            while tracker.snapshot_last_update_id is not None and not tracker.is_bridged:
                changed.clear()
                if tracker.snapshot_last_update_id is None or tracker.is_bridged:
                    return
                await changed.wait()

    async def _observe_depth_update(
        self,
        stream_type: StreamType,
        symbol: str,
        update: DepthUpdateIds | dict[str, Any],
        received_realtime_ns: int,
        receive_seq: int,
    ) -> None:
        key = (stream_type, symbol)
        tracker = self._bridges.get(key)
        if tracker is None:
            tracker = SnapshotBridgeTracker()
            self._bridges[key] = tracker
        ids = (
            update
            if isinstance(update, DepthUpdateIds)
            else DepthUpdateIds(int(update["U"]), int(update["u"]), int(update["pu"]))
        )
        result = tracker.on_diff(
            UpdateSpan(
                receive_seq=receive_seq,
                first_update_id=ids.first,
                final_update_id=ids.final,
                previous_final_update_id=ids.previous,
            )
        )
        changed = self._bridge_changed.get(key)
        if changed is None:
            changed = asyncio.Event()
            self._bridge_changed[key] = changed
        changed.set()
        snapshot_type = _snapshot_type_for_stream(stream_type)
        if (
            result.status is not BridgeStatus.SEQUENCE_GAP
            or key in self._resync_tasks
            or (symbol, snapshot_type) in self._snapshot_pending
        ):
            return
        expected = result.expected_previous_update_id
        previous = result.received_previous_update_id
        if expected is None or previous is None:
            raise AssertionError("sequence gap result omitted update IDs")
        gap_id = await self._on_depth_gap(
            self._identity.connection_id,
            symbol,
            stream_type,
            expected,
            previous,
            received_realtime_ns,
        )
        async def reanchor() -> None:
            await self._recover_snapshot_bridge(symbol, snapshot_type)
            await self._on_depth_reanchored(gap_id, symbol, stream_type)

        task = asyncio.create_task(
            reanchor(), name=f"depth-reanchor-{self._identity.connection_id}-{symbol}"
        )
        self._resync_tasks[key] = task
        task.add_done_callback(lambda completed: self._resync_done(key, completed))

    def _resync_done(self, key: tuple[StreamType, str], task: asyncio.Task[None]) -> None:
        if self._resync_tasks.get(key) is task:
            del self._resync_tasks[key]
        if task.cancelled():
            return
        error = task.exception()
        if (
            error is not None
            and self._background_failure is not None
            and not self._background_failure.done()
        ):
            self._background_failure.set_exception(error)


async def _wait_for_phase(
    *,
    success: asyncio.Task[None] | None,
    stop_task: asyncio.Task[bool],
    failures: tuple[asyncio.Future[Any], ...],
) -> bool:
    waiters = (*failures, stop_task) if success is None else (*failures, stop_task, success)
    done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
    if stop_task in done:
        return False
    if success is not None and success in done:
        await success
        return True
    for completed in done:
        if completed.cancelled():
            await completed
        error = completed.exception()
        if error is not None:
            raise error
    raise ConnectionError("websocket connection task stopped unexpectedly")


def _peer_text(value: object) -> str:
    if isinstance(value, tuple) and len(value) >= 2:
        return f"{value[0]}:{value[1]}"
    return str(value) if value is not None else "unknown"


def _snapshot_type_for_stream(stream_type: StreamType) -> StreamType:
    if stream_type is StreamType.DEPTH:
        return StreamType.DEPTH_SNAPSHOT
    if stream_type is StreamType.RPI_DEPTH:
        return StreamType.RPI_DEPTH_SNAPSHOT
    raise ValueError(f"stream has no snapshot type: {stream_type.value}")


def _stream_type_for_snapshot(snapshot_type: StreamType) -> StreamType:
    if snapshot_type is StreamType.DEPTH_SNAPSHOT:
        return StreamType.DEPTH
    if snapshot_type is StreamType.RPI_DEPTH_SNAPSHOT:
        return StreamType.RPI_DEPTH
    raise ValueError(f"unsupported snapshot type: {snapshot_type.value}")


def public_subscriptions(instruments: tuple[str, ...], *, d0_enabled: bool) -> tuple[str, ...]:
    streams = [
        stream
        for symbol in instruments
        for stream in (f"{symbol.lower()}@bookTicker", f"{symbol.lower()}@depth@100ms")
    ]
    if d0_enabled:
        streams.extend(f"{symbol.lower()}@trade" for symbol in instruments)
        streams.extend(f"{symbol.lower()}@rpiDepth@500ms" for symbol in instruments)
    return tuple(streams)


def market_subscriptions(instruments: tuple[str, ...]) -> tuple[str, ...]:
    streams = [
        stream
        for symbol in instruments
        for stream in (
            f"{symbol.lower()}@aggTrade",
            f"{symbol.lower()}@markPrice@1s",
            f"{symbol.lower()}@forceOrder",
        )
    ]
    streams.append("!contractInfo")
    return tuple(streams)
