from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import aiohttp
import orjson
from websockets.asyncio.client import connect

from miry.collector.ingest import IngestCoordinator
from miry.collector.rest import BinanceRestClient
from miry.contracts.models import RawEvent, StreamType

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DecodedWebSocket:
    stream_type: StreamType
    symbol: str | None
    message: dict[str, Any] | None
    data: dict[str, Any] | None


@dataclass(slots=True)
class SubscriptionUpdate:
    add: tuple[str, ...]
    remove: tuple[str, ...]
    snapshot_requests: tuple[tuple[str, StreamType], ...]
    acknowledged: asyncio.Future[None]
    completion: asyncio.Future[None]


class SubscriptionAuditError(OSError):
    def __init__(self, message: str, *, affected_from_realtime_ns: int) -> None:
        super().__init__(message)
        self.affected_from_realtime_ns = affected_from_realtime_ns


@dataclass(frozen=True, slots=True)
class _ControlResponse:
    request_id: int
    message: dict[str, Any]
    observed_at_realtime_ns: int


@dataclass(frozen=True, slots=True)
class _PendingControlRequest:
    request_id: int
    future: asyncio.Future[_ControlResponse]


class _ControlRequests:
    def __init__(self, initial_id: int) -> None:
        self._next_id = initial_id - 1
        self._pending: dict[int, asyncio.Future[_ControlResponse]] = {}

    async def request(
        self,
        websocket: Any,
        method: str,
        *,
        params: tuple[str, ...] | None = None,
        timeout_seconds: float | None = None,
    ) -> _ControlResponse:
        pending = await self.send(websocket, method, params=params)
        return await self.wait(pending, timeout_seconds=timeout_seconds)

    async def send(
        self,
        websocket: Any,
        method: str,
        *,
        params: tuple[str, ...] | None = None,
    ) -> _PendingControlRequest:
        self._next_id += 1
        request_id = self._next_id
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        payload: dict[str, object] = {"method": method, "id": request_id}
        if params is not None:
            payload["params"] = list(params)
        try:
            await websocket.send(orjson.dumps(payload).decode())
        except BaseException:
            self._pending.pop(request_id, None)
            future.cancel()
            raise
        return _PendingControlRequest(request_id, future)

    async def wait(
        self,
        pending: _PendingControlRequest,
        *,
        timeout_seconds: float | None = None,
    ) -> _ControlResponse:
        try:
            if timeout_seconds is None:
                return await pending.future
            async with asyncio.timeout(timeout_seconds):
                return await pending.future
        finally:
            self._pending.pop(pending.request_id, None)

    def deliver(self, message: dict[str, Any], observed_at_realtime_ns: int) -> None:
        request_id = message.get("id")
        if not isinstance(request_id, int):
            return
        future = self._pending.get(request_id)
        if future is None or future.done():
            return
        future.set_result(_ControlResponse(request_id, message, observed_at_realtime_ns))

    def cancel(self) -> None:
        for future in self._pending.values():
            if not future.done():
                future.cancel()
        self._pending.clear()


def decode_websocket(raw: bytes) -> DecodedWebSocket:
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
        return DecodedWebSocket(stream_type, symbol, message, data)
    mapping = {
        "bookTicker": StreamType.BOOK_TICKER,
        "aggTrade": StreamType.AGG_TRADE,
        "trade": StreamType.TRADE,
        "markPriceUpdate": StreamType.MARK_PRICE,
        "forceOrder": StreamType.FORCE_ORDER,
        "contractInfo": StreamType.CONTRACT_INFO,
    }
    return DecodedWebSocket(mapping.get(event_type, StreamType.UNKNOWN), symbol, message, data)


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
        on_event: Callable[[StreamType, str | None], None] | None = None,
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
        self._on_event = on_event or (lambda _stream, _symbol: None)
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
        self._previous_u: dict[tuple[StreamType, str], int] = {}
        self._resync_tasks: dict[tuple[StreamType, str], asyncio.Task[None]] = {}
        self._snapshot_pending = set(snapshot_requests)
        self._last_receive_monotonic = time.monotonic()
        self._subscription_proven_realtime_ns: int | None = None
        self._background_failure: asyncio.Future[None] | None = None

    async def run(self) -> None:
        initial_subscription_id = int.from_bytes(uuid4().bytes[:4], "big")
        active_subscriptions = set(self._subscriptions)
        control_lock = asyncio.Lock()
        control = _ControlRequests(initial_subscription_id)
        tasks: list[asyncio.Task[Any]] = []
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
                self._last_receive_monotonic = time.monotonic()
                initial_subscription_started = time.monotonic()
                initial_request = await control.send(
                    websocket, "SUBSCRIBE", params=self._subscriptions
                )
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
                    self._bootstrap(
                        control, initial_request, peer, initial_subscription_started
                    ),
                    name=f"bootstrap-{self._identity.connection_id}",
                )
                tasks.extend((receiver, watchdog, stop_task, bootstrap))
                if not await self._wait_for_bootstrap(
                    bootstrap, receiver, watchdog, stop_task, self._background_failure
                ):
                    return
                tasks.remove(bootstrap)
                updates = asyncio.create_task(
                    self._update_loop(websocket, control, control_lock, active_subscriptions),
                    name=f"subscription-updates-{self._identity.connection_id}",
                )
                audit = asyncio.create_task(
                    self._audit_loop(
                        websocket, control, control_lock, active_subscriptions, peer
                    ),
                    name=f"subscription-audit-{self._identity.connection_id}",
                )
                tasks.extend((updates, audit))
                await self._wait_until_stopped(
                    receiver,
                    watchdog,
                    updates,
                    audit,
                    stop_task,
                    self._background_failure,
                )
        finally:
            control.cancel()
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

    async def _receive_loop(self, websocket: Any, control: _ControlRequests) -> None:
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
            await self._ingest.put(event)
            self._on_event(decoded.stream_type, decoded.symbol)
            self._transport_pending.discard((decoded.stream_type, decoded.symbol or ""))
            self._maybe_mark_transport_ready()
            if decoded.stream_type is StreamType.WS_CONTROL and decoded.message:
                if decoded.message.get("code") is not None:
                    raise OSError(f"Binance subscription rejected: {decoded.message}")
                control.deliver(decoded.message, realtime_ns)
            if (
                decoded.stream_type in {StreamType.DEPTH, StreamType.RPI_DEPTH}
                and decoded.symbol
                and decoded.data is not None
            ):
                await self._check_depth_sequence(
                    decoded.stream_type, decoded.symbol, decoded.data, realtime_ns
                )

    async def _bootstrap(
        self,
        control: _ControlRequests,
        initial_request: _PendingControlRequest,
        peer: str,
        started: float,
    ) -> None:
        response = await control.wait(initial_request)
        if not _is_subscription_ack(response.message, response.request_id):
            raise OSError(f"Binance subscription rejected: {response.message}")
        self._initial_subscription_acknowledged = True
        self._subscription_proven_realtime_ns = response.observed_at_realtime_ns
        self._maybe_mark_transport_ready()
        logger.info(
            "subscription ready connection_id=%s peer=%s rtt_ms=%.3f",
            self._identity.connection_id,
            peer,
            (time.monotonic() - started) * 1_000,
        )
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

    async def _update_loop(
        self,
        websocket: Any,
        control: _ControlRequests,
        control_lock: asyncio.Lock,
        active_subscriptions: set[str],
    ) -> None:
        while True:
            update = await self._updates.get()
            if update.acknowledged.cancelled() or update.completion.cancelled():
                continue
            self._snapshot_pending.update(update.snapshot_requests)
            try:
                requests = []
                async with control_lock:
                    for method, streams in (
                        ("UNSUBSCRIBE", update.remove),
                        ("SUBSCRIBE", update.add),
                    ):
                        if streams:
                            requests.append(control.request(websocket, method, params=streams))
                    if requests:
                        await asyncio.gather(*requests)
                    active_subscriptions.difference_update(update.remove)
                    active_subscriptions.update(update.add)
                if not update.acknowledged.done():
                    update.acknowledged.set_result(None)
                if update.snapshot_requests:
                    await self._fetch_requested_snapshots(
                        update.snapshot_requests, update.completion
                    )
                elif not update.completion.done():
                    update.completion.set_result(None)
            except BaseException as exc:
                _fail_subscription_update(update, exc)
                raise

    async def _audit_loop(
        self,
        websocket: Any,
        control: _ControlRequests,
        control_lock: asyncio.Lock,
        active_subscriptions: set[str],
        peer: str,
    ) -> None:
        failures = 0
        await asyncio.sleep(self._subscription_audit_seconds)
        while True:
            started = time.monotonic()
            try:
                async with control_lock:
                    response = await control.request(
                        websocket,
                        "LIST_SUBSCRIPTIONS",
                        timeout_seconds=self._subscription_audit_timeout_seconds,
                    )
            except TimeoutError as exc:
                failures += 1
                if failures >= self._subscription_audit_failures_before_reconnect:
                    raise SubscriptionAuditError(
                        "subscription audit response was not received within "
                        f"{self._subscription_audit_timeout_seconds:g}s "
                        f"for {failures} consecutive attempts",
                        affected_from_realtime_ns=(
                            self._subscription_proven_realtime_ns or time.time_ns()
                        ),
                    ) from exc
                logger.warning(
                    "subscription audit response missed connection_id=%s "
                    "failures=%d threshold=%d; retrying",
                    self._identity.connection_id,
                    failures,
                    self._subscription_audit_failures_before_reconnect,
                )
                continue
            result = response.message.get("result")
            actual = (
                set(result)
                if isinstance(result, list) and all(isinstance(value, str) for value in result)
                else set()
            )
            if actual != active_subscriptions:
                missing = sorted(active_subscriptions - actual)
                unexpected = sorted(actual - active_subscriptions)
                raise SubscriptionAuditError(
                    "subscription audit mismatch "
                    f"missing={missing} unexpected={unexpected}",
                    affected_from_realtime_ns=(
                        self._subscription_proven_realtime_ns
                        or response.observed_at_realtime_ns
                    ),
                )
            failures = 0
            self._subscription_proven_realtime_ns = response.observed_at_realtime_ns
            logger.info(
                "subscription audit connection_id=%s peer=%s rtt_ms=%.3f "
                "ping_rtt_ms=%s subscriptions=%d",
                self._identity.connection_id,
                peer,
                (time.monotonic() - started) * 1_000,
                _latency_ms(getattr(websocket, "latency", None)),
                len(actual),
            )
            await asyncio.sleep(self._subscription_audit_seconds)

    async def _wait_for_bootstrap(
        self,
        bootstrap: asyncio.Task[None],
        receiver: asyncio.Task[None],
        watchdog: asyncio.Task[None],
        stop_task: asyncio.Task[bool],
        background_failure: asyncio.Future[None],
    ) -> bool:
        done, _ = await asyncio.wait(
            (bootstrap, receiver, watchdog, stop_task, background_failure),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done:
            return False
        if bootstrap in done:
            await bootstrap
            return True
        await _raise_connection_completion(done)
        raise AssertionError("unreachable")

    async def _wait_until_stopped(
        self,
        receiver: asyncio.Task[None],
        watchdog: asyncio.Task[None],
        updates: asyncio.Task[None],
        audit: asyncio.Task[None],
        stop_task: asyncio.Task[bool],
        background_failure: asyncio.Future[None],
    ) -> None:
        done, _ = await asyncio.wait(
            (receiver, watchdog, updates, audit, stop_task, background_failure),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done:
            return
        await _raise_connection_completion(done)

    def _maybe_mark_transport_ready(self) -> None:
        if self._initial_subscription_acknowledged and not self._transport_pending:
            self._transport_ready.set()

    async def _fetch_requested_snapshots(
        self,
        requests: tuple[tuple[str, StreamType], ...],
        completion: asyncio.Future[None],
    ) -> None:
        try:
            for symbol, stream_type in requests:
                await self._fetch_snapshot(symbol, stream_type)
            if not completion.done():
                completion.set_result(None)
        except BaseException as exc:
            if not completion.done():
                completion.set_exception(exc)
            raise

    async def _fetch_snapshots(self) -> None:
        for symbol, stream_type in self._snapshot_requests:
            await self._fetch_snapshot(symbol, stream_type)

    async def _fetch_snapshot(self, symbol: str, stream_type: StreamType) -> None:
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
                    "attempt=%d",
                    self._identity.connection_id,
                    symbol,
                    stream_type.value,
                    (observed_at - requested_at) / 1_000_000,
                    attempt + 1,
                )
                self._snapshot_pending.discard((symbol, stream_type))
                return
            except (aiohttp.ClientError, TimeoutError):
                if attempt == 4:
                    raise
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10)

    async def _check_depth_sequence(
        self,
        stream_type: StreamType,
        symbol: str,
        data: dict[str, Any],
        received_realtime_ns: int,
    ) -> None:
        previous = int(data["pu"])
        final = int(data["u"])
        key = (stream_type, symbol)
        expected = self._previous_u.get(key)
        self._previous_u[key] = final
        snapshot_type = (
            StreamType.RPI_DEPTH_SNAPSHOT
            if stream_type is StreamType.RPI_DEPTH
            else StreamType.DEPTH_SNAPSHOT
        )
        if (
            expected is None
            or previous == expected
            or key in self._resync_tasks
            or (symbol, snapshot_type) in self._snapshot_pending
        ):
            return
        gap_id = await self._on_depth_gap(
            self._identity.connection_id,
            symbol,
            stream_type,
            expected,
            previous,
            received_realtime_ns,
        )

        async def reanchor() -> None:
            await self._fetch_snapshot(symbol, snapshot_type)
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


def _fail_subscription_update(update: SubscriptionUpdate, error: BaseException) -> None:
    if update.acknowledged.cancelled():
        if not update.completion.done():
            update.completion.cancel()
        return
    if not update.acknowledged.done():
        update.acknowledged.set_exception(error)
        if not update.completion.done():
            update.completion.cancel()
        return
    if not update.completion.done():
        update.completion.set_exception(error)


async def _raise_connection_completion(done: set[asyncio.Future[Any]]) -> None:
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


def _latency_ms(value: object) -> str:
    if isinstance(value, int | float):
        return f"{value * 1_000:.3f}"
    return "unknown"


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


def _is_subscription_ack(value: dict[str, Any] | None, expected_id: int) -> bool:
    return (
        isinstance(value, dict) and value.get("id") == expected_id and value.get("result") is None
    )
