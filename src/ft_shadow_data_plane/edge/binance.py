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

from ft_shadow_data_plane.contracts.models import RawEventV1, StreamType
from ft_shadow_data_plane.edge.ingest import IngestCoordinator
from ft_shadow_data_plane.edge.rest import BinanceRestClient

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


@dataclass(slots=True)
class _PendingSubscriptionUpdate:
    remaining_ids: set[int]
    add: tuple[str, ...]
    remove: tuple[str, ...]
    snapshot_requests: tuple[tuple[str, StreamType], ...]
    acknowledged: asyncio.Future[None]
    completion: asyncio.Future[None]


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
    ) -> RawEventV1:
        self._sequence += 1
        return RawEventV1(
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

    async def run(self) -> None:
        initial_subscription_id = int.from_bytes(uuid4().bytes[:4], "big")
        subscription_id = initial_subscription_id
        snapshot_task: asyncio.Task[None] | None = None
        update_task: asyncio.Task[SubscriptionUpdate] | None = None
        receive_task: asyncio.Task[bytes | str] | None = None
        audit_task: asyncio.Task[None] | None = None
        pending_updates: dict[int, _PendingSubscriptionUpdate] = {}
        pending_audits: set[int] = set()
        active_subscriptions = set(self._subscriptions)
        subscription_proven_realtime_ns: int | None = None
        pending_audit_started: float | None = None
        consecutive_audit_failures = 0
        try:
            async with connect(
                self._url,
                ping_interval=self._ping_interval_seconds,
                ping_timeout=self._ping_timeout_seconds,
                max_queue=self._max_queue,
                max_size=self._max_message_bytes,
                close_timeout=10,
            ) as websocket:
                await websocket.send(
                    orjson.dumps(
                        {
                            "method": "SUBSCRIBE",
                            "params": list(self._subscriptions),
                            "id": subscription_id,
                        }
                    ).decode()
                )
                update_task = asyncio.create_task(self._updates.get())
                audit_task = asyncio.create_task(asyncio.sleep(self._subscription_audit_seconds))
                while not self._stop.is_set():
                    if receive_task is None:
                        receive_task = asyncio.create_task(websocket.recv(decode=False))
                    wait_timeout = self._receive_timeout_seconds
                    if pending_audit_started is not None:
                        wait_timeout = min(
                            wait_timeout,
                            max(
                                0,
                                self._subscription_audit_timeout_seconds
                                - (time.monotonic() - pending_audit_started),
                            ),
                        )
                    try:
                        async with asyncio.timeout(wait_timeout):
                            waiters: set[asyncio.Task[Any]] = {
                                receive_task,
                                update_task,
                                audit_task,
                                *self._resync_tasks.values(),
                            }
                            if snapshot_task is not None:
                                waiters.add(snapshot_task)
                            done, _ = await asyncio.wait(
                                waiters,
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                    except TimeoutError as exc:
                        if pending_audit_started is not None:
                            consecutive_audit_failures += 1
                            pending_audits.clear()
                            pending_audit_started = None
                            if consecutive_audit_failures >= (
                                self._subscription_audit_failures_before_reconnect
                            ):
                                raise SubscriptionAuditError(
                                    "subscription audit response was not received within "
                                    f"{self._subscription_audit_timeout_seconds:g}s "
                                    f"for {consecutive_audit_failures} consecutive attempts",
                                    affected_from_realtime_ns=(
                                        subscription_proven_realtime_ns or time.time_ns()
                                    ),
                                ) from exc
                            logger.warning(
                                "subscription audit response missed connection_id=%s "
                                "failures=%d threshold=%d; retrying",
                                self._identity.connection_id,
                                consecutive_audit_failures,
                                self._subscription_audit_failures_before_reconnect,
                            )
                            if audit_task is not None:
                                audit_task.cancel()
                            audit_task = asyncio.create_task(asyncio.sleep(0))
                            continue
                        raise TimeoutError(
                            "no websocket message for "
                            f"{self._receive_timeout_seconds:g}s "
                            f"connection_id={self._identity.connection_id}"
                        ) from exc
                    if update_task in done:
                        update = update_task.result()
                        update_task = asyncio.create_task(self._updates.get())
                        if update.acknowledged.cancelled() or update.completion.cancelled():
                            continue
                        self._snapshot_pending.update(update.snapshot_requests)
                        ids: set[int] = set()
                        for method, streams in (
                            ("UNSUBSCRIBE", update.remove),
                            ("SUBSCRIBE", update.add),
                        ):
                            if not streams:
                                continue
                            subscription_id += 1
                            ids.add(subscription_id)
                            await websocket.send(
                                orjson.dumps(
                                    {
                                        "method": method,
                                        "params": list(streams),
                                        "id": subscription_id,
                                    }
                                ).decode()
                            )
                        pending = _PendingSubscriptionUpdate(
                            ids,
                            update.add,
                            update.remove,
                            update.snapshot_requests,
                            update.acknowledged,
                            update.completion,
                        )
                        for update_id in ids:
                            pending_updates[update_id] = pending
                        if not ids:
                            if not update.acknowledged.done():
                                update.acknowledged.set_result(None)
                            if not update.completion.done():
                                update.completion.set_result(None)
                    if audit_task in done:
                        if pending_audits:
                            consecutive_audit_failures += 1
                            pending_audits.clear()
                            pending_audit_started = None
                            if consecutive_audit_failures >= (
                                self._subscription_audit_failures_before_reconnect
                            ):
                                raise SubscriptionAuditError(
                                    "subscription audit response was not received within "
                                    f"{self._subscription_audit_timeout_seconds:g}s "
                                    f"for {consecutive_audit_failures} consecutive attempts",
                                    affected_from_realtime_ns=(
                                        subscription_proven_realtime_ns or time.time_ns()
                                    ),
                                )
                            logger.warning(
                                "subscription audit response missed connection_id=%s "
                                "failures=%d threshold=%d; retrying",
                                self._identity.connection_id,
                                consecutive_audit_failures,
                                self._subscription_audit_failures_before_reconnect,
                            )
                        if not pending_updates:
                            subscription_id += 1
                            pending_audits.add(subscription_id)
                            pending_audit_started = time.monotonic()
                            await websocket.send(
                                orjson.dumps(
                                    {"method": "LIST_SUBSCRIPTIONS", "id": subscription_id}
                                ).decode()
                            )
                        audit_task = asyncio.create_task(
                            asyncio.sleep(self._subscription_audit_seconds)
                        )
                    if snapshot_task is not None and snapshot_task in done:
                        await snapshot_task
                        snapshot_task = None
                        self._ready.set()
                    for key, task in tuple(self._resync_tasks.items()):
                        if task in done:
                            await task
                            del self._resync_tasks[key]
                    if receive_task not in done:
                        continue
                    raw = receive_task.result()
                    receive_task = None
                    realtime_ns = time.time_ns()
                    monotonic_ns = time.monotonic_ns()
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
                    if decoded.stream_type is StreamType.WS_CONTROL and _is_subscription_ack(
                        decoded.message, initial_subscription_id
                    ):
                        self._initial_subscription_acknowledged = True
                        self._maybe_mark_transport_ready()
                        subscription_proven_realtime_ns = realtime_ns
                        if self._snapshot_requests:
                            snapshot_task = asyncio.create_task(
                                self._fetch_snapshots(),
                                name=f"snapshots-{self._identity.connection_id}",
                            )
                        else:
                            self._ready.set()
                    if decoded.stream_type is StreamType.WS_CONTROL and decoded.message:
                        if decoded.message.get("code") is not None:
                            raise OSError(f"Binance subscription rejected: {decoded.message}")
                        response_id = decoded.message.get("id")
                        if isinstance(response_id, int) and response_id in pending_audits:
                            pending_audits.remove(response_id)
                            pending_audit_started = None
                            result = decoded.message.get("result")
                            actual = (
                                set(result)
                                if isinstance(result, list)
                                and all(isinstance(value, str) for value in result)
                                else set()
                            )
                            if actual != active_subscriptions:
                                missing = sorted(active_subscriptions - actual)
                                unexpected = sorted(actual - active_subscriptions)
                                raise SubscriptionAuditError(
                                    "subscription audit mismatch "
                                    f"missing={missing} unexpected={unexpected}",
                                    affected_from_realtime_ns=(
                                        subscription_proven_realtime_ns or realtime_ns
                                    ),
                                )
                            consecutive_audit_failures = 0
                            subscription_proven_realtime_ns = realtime_ns
                        if isinstance(response_id, int) and response_id in pending_updates:
                            pending = pending_updates.pop(response_id)
                            pending.remaining_ids.discard(response_id)
                            if not pending.remaining_ids:
                                active_subscriptions.difference_update(pending.remove)
                                active_subscriptions.update(pending.add)
                                if not pending.acknowledged.done():
                                    pending.acknowledged.set_result(None)
                                if pending.snapshot_requests:
                                    task = asyncio.create_task(
                                        self._fetch_requested_snapshots(
                                            pending.snapshot_requests, pending.completion
                                        )
                                    )
                                    self._resync_tasks[
                                        (StreamType.WS_CONTROL, str(response_id))
                                    ] = task
                                elif not pending.completion.done():
                                    pending.completion.set_result(None)
                    if (
                        decoded.stream_type in {StreamType.DEPTH, StreamType.RPI_DEPTH}
                        and decoded.symbol
                        and decoded.data is not None
                    ):
                        await self._check_depth_sequence(
                            decoded.stream_type, decoded.symbol, decoded.data, realtime_ns
                        )
        finally:
            if update_task is not None:
                update_task.cancel()
            if receive_task is not None:
                receive_task.cancel()
            if audit_task is not None:
                audit_task.cancel()
            for pending in pending_updates.values():
                error = ConnectionError("connection closed during update")
                if pending.acknowledged.cancelled():
                    if not pending.completion.done():
                        pending.completion.cancel()
                elif not pending.acknowledged.done():
                    pending.acknowledged.set_exception(error)
                    if not pending.completion.done():
                        pending.completion.cancel()
                elif not pending.completion.done():
                    pending.completion.set_exception(error)
            tasks = list(self._resync_tasks.values())
            if snapshot_task is not None:
                tasks.append(snapshot_task)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

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

        self._resync_tasks[key] = asyncio.create_task(
            reanchor(), name=f"depth-reanchor-{self._identity.connection_id}-{symbol}"
        )


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
