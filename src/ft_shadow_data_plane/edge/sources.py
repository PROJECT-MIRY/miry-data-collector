from __future__ import annotations

import asyncio
import gzip
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import aiohttp
import orjson
from websockets.exceptions import ConnectionClosed

from ft_shadow_data_plane.central.selector import (
    DiscoverySnapshot,
    liquidity_validation_symbols,
)
from ft_shadow_data_plane.contracts.models import SYMBOL_PATTERN, GapReason, StreamType
from ft_shadow_data_plane.contracts.serde import canonical_json_bytes, sha256_bytes
from ft_shadow_data_plane.edge.binance import (
    BinanceRestClient,
    BinanceWebSocketConnection,
    SourceIdentity,
    SubscriptionAuditError,
    SubscriptionUpdate,
    market_subscriptions,
    public_subscriptions,
)
from ft_shadow_data_plane.edge.config import EdgeConfig
from ft_shadow_data_plane.edge.gaps import GapJournal
from ft_shadow_data_plane.edge.ingest import IngestCoordinator
from ft_shadow_data_plane.edge.queue import ByteBoundedQueues, QueueOverloaded

logger = logging.getLogger(__name__)


class StableWeightedSharder:
    def __init__(self, count: int, weights: dict[str, int]) -> None:
        if count < 1:
            raise ValueError("shard count must be positive")
        self._count = count
        self._weights = weights
        self._assignments: dict[str, int] = {}

    def shards(self, instruments: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
        shard_count = min(self._count, len(instruments))
        if shard_count == 0:
            return ()
        initial_assignment = not self._assignments
        active = set(instruments)
        self._assignments = {
            symbol: shard
            for symbol, shard in self._assignments.items()
            if symbol in active and shard < shard_count
        }
        shards: list[list[str]] = [[] for _ in range(shard_count)]
        loads = [0] * shard_count
        for symbol, shard in sorted(self._assignments.items()):
            shards[shard].append(symbol)
            loads[shard] += self._weight(symbol)
        unassigned = sorted(
            active - self._assignments.keys(),
            key=lambda symbol: (-self._weight(symbol), symbol),
        )
        for symbol in unassigned:
            shard = min(range(shard_count), key=lambda index: (loads[index], index))
            self._assignments[symbol] = shard
            shards[shard].append(symbol)
            loads[shard] += self._weight(symbol)
        if initial_assignment:
            self._improve_initial_balance(shards, loads)
            self._assignments = {
                symbol: shard for shard, symbols in enumerate(shards) for symbol in symbols
            }
        return tuple(tuple(sorted(shard)) for shard in shards)

    def _weight(self, symbol: str) -> int:
        if symbol in self._weights:
            return self._weights[symbol]
        if self._weights:
            ordered = sorted(self._weights.values())
            return ordered[len(ordered) // 2]
        return 1

    def _improve_initial_balance(self, shards: list[list[str]], loads: list[int]) -> None:
        while True:
            current_spread = max(loads) - min(loads)
            best: tuple[int, int, str, str, int, int] | None = None
            best_spread = current_spread
            for left in range(len(shards)):
                for right in range(left + 1, len(shards)):
                    for left_symbol in shards[left]:
                        for right_symbol in shards[right]:
                            next_left = (
                                loads[left]
                                - self._weight(left_symbol)
                                + self._weight(right_symbol)
                            )
                            next_right = (
                                loads[right]
                                - self._weight(right_symbol)
                                + self._weight(left_symbol)
                            )
                            next_loads = [*loads]
                            next_loads[left] = next_left
                            next_loads[right] = next_right
                            spread = max(next_loads) - min(next_loads)
                            if spread < best_spread:
                                best_spread = spread
                                best = (
                                    left,
                                    right,
                                    left_symbol,
                                    right_symbol,
                                    next_left,
                                    next_right,
                                )
            if best is None:
                return
            left, right, left_symbol, right_symbol, next_left, next_right = best
            shards[left].remove(left_symbol)
            shards[left].append(right_symbol)
            shards[right].remove(right_symbol)
            shards[right].append(left_symbol)
            loads[left] = next_left
            loads[right] = next_right


@dataclass(slots=True)
class ConnectionHandle:
    identity: SourceIdentity
    ready: asyncio.Event
    stop: asyncio.Event
    task: asyncio.Task[None]
    transport_ready: asyncio.Event = field(default_factory=asyncio.Event)


class RouteRunner:
    def __init__(
        self,
        *,
        name: str,
        url: str,
        subscriptions: tuple[str, ...],
        instruments: tuple[str, ...],
        stream_types: tuple[StreamType, ...],
        collector_id: str,
        boot_id: str,
        ingest: IngestCoordinator,
        queues: ByteBoundedQueues,
        gaps: GapJournal,
        rest: BinanceRestClient,
        rotation_seconds: int,
        overlap_seconds: int,
        receive_timeout_seconds: float,
        ping_interval_seconds: float,
        ping_timeout_seconds: float,
        service_stop: asyncio.Event,
        rotation_offset_seconds: float = 0,
        d0_enabled: bool = False,
        on_ready: Callable[[str], None] | None = None,
        subscriptions_for: Callable[[tuple[str, ...]], tuple[str, ...]] | None = None,
        liveness_timeout_seconds: float | None = None,
        liveness_stream_types: tuple[StreamType, ...] | None = None,
        subscription_audit_seconds: float = 60.0,
        subscription_audit_timeout_seconds: float = 10.0,
        subscription_audit_failures_before_reconnect: int = 1,
        refresh_failures_before_reconnect: int = 2,
        websocket_max_queue: int = 4,
        websocket_max_message_bytes: int = 2 * 1024**2,
    ) -> None:
        self._name = name
        self._url = url
        self._subscriptions = subscriptions
        self._instruments = instruments
        self._stream_types = stream_types
        self._collector_id = collector_id
        self._boot_id = boot_id
        self._ingest = ingest
        self._queues = queues
        self._gaps = gaps
        self._rest = rest
        self._rotation_seconds = rotation_seconds
        self._next_rotation_seconds = rotation_seconds + rotation_offset_seconds
        self._overlap_seconds = overlap_seconds
        self._receive_timeout_seconds = receive_timeout_seconds
        self._ping_interval_seconds = ping_interval_seconds
        self._ping_timeout_seconds = ping_timeout_seconds
        self._service_stop = service_stop
        self._d0_enabled = d0_enabled
        self._on_ready = on_ready or (lambda _: None)
        self._subscriptions_for = subscriptions_for
        self._updates: asyncio.Queue[SubscriptionUpdate] = asyncio.Queue(maxsize=1)
        self._update_lock = asyncio.Lock()
        self._reconnect_requested = asyncio.Event()
        self._liveness_timeout_seconds = liveness_timeout_seconds
        self._liveness_stream_types = (
            tuple(dict.fromkeys(liveness_stream_types or stream_types))
            if liveness_timeout_seconds is not None
            else ()
        )
        self._subscription_audit_seconds = subscription_audit_seconds
        self._subscription_audit_timeout_seconds = subscription_audit_timeout_seconds
        self._subscription_audit_failures_before_reconnect = (
            subscription_audit_failures_before_reconnect
        )
        self._refresh_failures_before_reconnect = refresh_failures_before_reconnect
        self._websocket_max_queue = websocket_max_queue
        self._websocket_max_message_bytes = websocket_max_message_bytes
        now = time.monotonic()
        realtime_ns = time.time_ns()
        self._last_event = {
            (stream_type, symbol): (now, realtime_ns)
            for stream_type in self._liveness_stream_types
            for symbol in instruments
        }
        self._liveness_changed = asyncio.Event()
        self._connection_generation = 0
        self._connection_ready = asyncio.Event()

    @property
    def instruments(self) -> tuple[str, ...]:
        return self._instruments

    async def update_instruments(self, instruments: tuple[str, ...]) -> None:
        if self._subscriptions_for is None:
            raise RuntimeError(f"route {self._name} does not support live updates")
        async with self._update_lock:
            previous = set(self._instruments)
            proposed = set(instruments)
            added = tuple(sorted(proposed - previous))
            old_subscriptions = set(self._subscriptions)
            new_subscriptions = self._subscriptions_for(instruments)
            await self._submit_update(
                add=tuple(sorted(set(new_subscriptions) - old_subscriptions)),
                remove=tuple(sorted(old_subscriptions - set(new_subscriptions))),
                snapshot_requests=self._snapshot_requests_for(added),
            )
            self._subscriptions = new_subscriptions
            self._instruments = instruments
            now = time.monotonic()
            realtime_ns = time.time_ns()
            self._last_event = {
                (stream_type, symbol): self._last_event.get(
                    (stream_type, symbol), (now, realtime_ns)
                )
                for stream_type in self._liveness_stream_types
                for symbol in instruments
            }

    async def liveness_loop(self) -> None:
        timeout = self._liveness_timeout_seconds
        if timeout is None:
            await self._service_stop.wait()
            return
        active_gaps: dict[tuple[StreamType, str], tuple[str, float]] = {}
        failure_generation = -1
        consecutive_refresh_failures = 0
        while not self._service_stop.is_set():
            await self._wait_or_stop(max(1.0, timeout / 2))
            if self._service_stop.is_set():
                return
            now = time.monotonic()
            for key, (gap_id, opened_after) in tuple(active_gaps.items()):
                observed = self._last_event.get(key)
                if observed is not None and observed[0] <= opened_after:
                    continue
                stream_type, symbol = key
                await self._gaps.close(
                    gap_id,
                    GapReason.CONNECTION_LOST,
                    exchange_symbols=(symbol,),
                    stream_types=(stream_type,),
                    detail=(
                        "stream activity resumed after targeted recovery"
                        if observed is not None
                        else "stream removed from the active route"
                    ),
                )
                del active_gaps[key]
            stale = tuple(
                (stream_type, symbol, observed_realtime_ns)
                for (stream_type, symbol), (
                    observed_monotonic,
                    observed_realtime_ns,
                ) in self._last_event.items()
                if now - observed_monotonic >= timeout
            )
            if not stale:
                continue
            stale_keys = tuple((stream_type, symbol) for stream_type, symbol, _ in stale)
            logger.warning(
                "targeted subscription refresh route=%s stale=%s",
                self._name,
                [(stream.value, symbol) for stream, symbol, _realtime_ns in stale],
            )
            for stream_type, symbol, affected_from_ns in stale:
                key = (stream_type, symbol)
                if key in active_gaps:
                    continue
                gap_id = await self._gaps.open(
                    GapReason.CONNECTION_LOST,
                    exchange_symbols=(symbol,),
                    stream_types=(stream_type,),
                    affected_from_realtime_ns=affected_from_ns,
                    detail=(f"{self._name}: no {stream_type.value} event for {timeout:g}s"),
                )
                active_gaps[key] = (gap_id, self._last_event[key][0])
            try:
                refresh_generation = self._connection_generation
                await self._refresh_keys(stale_keys)
                if self._service_stop.is_set():
                    return
                refreshed_after = time.monotonic()
                await self._wait_for_fresh_events(
                    tuple((stream_type, symbol) for stream_type, symbol, _ in stale),
                    after=refreshed_after,
                    timeout_seconds=timeout,
                )
                for stream_type, symbol, _affected_from_ns in stale:
                    key = (stream_type, symbol)
                    active = active_gaps.get(key)
                    if active is None:
                        continue
                    gap_id, _opened_after = active
                    await self._gaps.close(
                        gap_id,
                        GapReason.CONNECTION_LOST,
                        exchange_symbols=(symbol,),
                        stream_types=(stream_type,),
                        detail="targeted subscriptions and L2 snapshots refreshed",
                    )
                    del active_gaps[key]
                failure_generation = -1
                consecutive_refresh_failures = 0
            except (
                QueueOverloaded,
                aiohttp.ClientError,
                OSError,
                TimeoutError,
            ) as exc:
                logger.warning(
                    "targeted subscription recovery incomplete route=%s "
                    "open_gaps=%d error=%s; will retry",
                    self._name,
                    len(active_gaps),
                    exc,
                    exc_info=True,
                )
                if isinstance(exc, QueueOverloaded):
                    await self._queues.wait_until_resumable()
                elif (
                    refresh_generation == self._connection_generation
                    and self._connection_ready.is_set()
                ):
                    if failure_generation != refresh_generation:
                        failure_generation = refresh_generation
                        consecutive_refresh_failures = 0
                    consecutive_refresh_failures += 1
                    if consecutive_refresh_failures >= self._refresh_failures_before_reconnect:
                        self._reconnect_requested.set()
                else:
                    failure_generation = -1
                    consecutive_refresh_failures = 0

    async def _refresh_keys(self, keys: tuple[tuple[StreamType, str], ...]) -> None:
        if self._subscriptions_for is None:
            return
        async with self._update_lock:
            streams = tuple(sorted(_liveness_subscription(key) for key in keys))
            snapshots = tuple(
                (symbol, _snapshot_type(stream_type))
                for stream_type, symbol in keys
                if stream_type in {StreamType.DEPTH, StreamType.RPI_DEPTH}
            )
            await self._submit_update(
                add=streams,
                remove=streams,
                snapshot_requests=snapshots,
            )

    async def _submit_update(
        self,
        *,
        add: tuple[str, ...],
        remove: tuple[str, ...],
        snapshot_requests: tuple[tuple[str, StreamType], ...],
    ) -> None:
        loop = asyncio.get_running_loop()
        acknowledged = loop.create_future()
        completion = loop.create_future()
        try:
            async with asyncio.timeout(self._subscription_audit_timeout_seconds):
                await self._updates.put(
                    SubscriptionUpdate(
                        add=add,
                        remove=remove,
                        snapshot_requests=snapshot_requests,
                        acknowledged=acknowledged,
                        completion=completion,
                    )
                )
                await acknowledged
            async with asyncio.timeout(180):
                await completion
        finally:
            for future in (acknowledged, completion):
                if not future.done():
                    future.cancel()

    def _mark_event(self, stream_type: StreamType, symbol: str | None) -> None:
        key = (stream_type, symbol)
        if key in self._last_event:
            self._last_event[key] = (time.monotonic(), time.time_ns())
            self._liveness_changed.set()

    async def _wait_for_fresh_events(
        self,
        keys: tuple[tuple[StreamType, str], ...],
        *,
        after: float,
        timeout_seconds: float,
    ) -> None:
        async with asyncio.timeout(timeout_seconds):
            while True:
                self._liveness_changed.clear()
                pending = [key for key in keys if self._last_event[key][0] <= after]
                if not pending:
                    return
                await self._liveness_changed.wait()

    async def run(self) -> None:
        current: ConnectionHandle | None = None
        gap_id: str | None = None
        gap_reason = GapReason.CONNECTION_LOST
        recovery_started_at: float | None = None
        transport_recovered_at: float | None = None
        reconnect_failures = 0
        active_since: float | None = None
        while not self._service_stop.is_set():
            if current is None:
                try:
                    on_transport_ready = None
                    if gap_id is not None:

                        async def report_transport_ready(
                            identity: SourceIdentity,
                            recovery_reason: GapReason = gap_reason,
                            started_at: float | None = recovery_started_at,
                        ) -> None:
                            nonlocal gap_id, transport_recovered_at
                            if gap_id is None:
                                return
                            transport_recovered_at = await self._close_transport_gap(
                                gap_id=gap_id,
                                reason=recovery_reason,
                                identity=identity,
                                started_at=started_at,
                            )
                            gap_id = None

                        on_transport_ready = report_transport_ready
                    current = await self._start_ready_connection(
                        on_transport_ready=on_transport_ready
                    )
                    self._activate_connection()
                    active_since = time.monotonic()
                    self._on_ready(self._name)
                    if transport_recovered_at is not None:
                        now = time.monotonic()
                        logger.info(
                            "connection snapshot ready route=%s connection_id=%s "
                            "reanchor_s=%.3f total_recovery_s=%.3f",
                            self._name,
                            current.identity.connection_id,
                            now - transport_recovered_at,
                            (
                                now - recovery_started_at
                                if recovery_started_at is not None
                                else 0.0
                            ),
                        )
                    recovery_started_at = None
                    transport_recovered_at = None
                except QueueOverloaded as exc:
                    if gap_id is None:
                        gap_reason = GapReason.INGEST_OVERLOAD
                        recovery_started_at = time.monotonic()
                        transport_recovered_at = None
                        gap_id = await self._open_gap(gap_reason, str(exc))
                    await self._queues.wait_until_resumable()
                    continue
                except asyncio.CancelledError:
                    return
                except (aiohttp.ClientError, ConnectionClosed, OSError, TimeoutError) as exc:
                    if gap_id is None:
                        gap_reason = GapReason.CONNECTION_LOST
                        recovery_started_at = time.monotonic()
                        transport_recovered_at = None
                        gap_id = await self._open_gap(
                            gap_reason,
                            str(exc),
                            affected_from_realtime_ns=_error_affected_from(exc),
                        )
                    reconnect_failures += 1
                    await self._wait_or_stop(_reconnect_delay(reconnect_failures))
                    continue

            outcome = await self._wait_current(current)
            if outcome == "stop":
                await _stop_handle(current)
                return
            if outcome in {"failed", "reconnect"}:
                self._connection_ready.clear()
                error = (
                    _task_error(current.task)
                    if outcome == "failed"
                    else OSError("targeted subscription recovery requested route reconnect")
                )
                gap_reason = (
                    GapReason.INGEST_OVERLOAD
                    if isinstance(error, QueueOverloaded)
                    else GapReason.CONNECTION_LOST
                )
                recovery_started_at = time.monotonic()
                transport_recovered_at = None
                gap_id = await self._open_gap(
                    gap_reason,
                    str(error or "connection closed"),
                    connection_id=current.identity.connection_id,
                    affected_from_realtime_ns=_error_affected_from(error),
                )
                logger.warning(
                    "connection %s route=%s connection_id=%s gap_id=%s error=%s",
                    "failed" if outcome == "failed" else "reconnect requested",
                    self._name,
                    current.identity.connection_id,
                    gap_id,
                    error or "connection closed",
                )
                if outcome == "reconnect":
                    await _stop_handle(current)
                current = None
                if gap_reason is GapReason.INGEST_OVERLOAD:
                    await self._queues.wait_until_resumable()
                else:
                    if active_since is not None and time.monotonic() - active_since >= 60:
                        reconnect_failures = 0
                    reconnect_failures += 1
                    await self._wait_or_stop(_reconnect_delay(reconnect_failures))
                continue

            try:
                async with self._update_lock:
                    replacement = await self._start_ready_connection()
                    await self._wait_or_stop(self._overlap_seconds)
                    if self._service_stop.is_set():
                        await _stop_handle(replacement)
                        await _stop_handle(current)
                        return
                    await _stop_handle(current)
                    current = replacement
                    self._activate_connection()
                    active_since = time.monotonic()
            except asyncio.CancelledError:
                await _stop_handle(current)
                return
            except (
                QueueOverloaded,
                aiohttp.ClientError,
                ConnectionClosed,
                OSError,
                TimeoutError,
            ) as exc:
                logger.warning("replacement connection failed route=%s error=%s", self._name, exc)
                await self._wait_or_stop(30)
                continue

    async def _start_ready_connection(
        self,
        *,
        on_transport_ready: Callable[[SourceIdentity], Awaitable[None]] | None = None,
    ) -> ConnectionHandle:
        connection_id = f"{self._name}-{uuid4().hex}"
        identity = SourceIdentity(
            collector_id=self._collector_id,
            boot_id=self._boot_id,
            segment_id=uuid4().hex,
            connection_id=connection_id,
        )
        ready = asyncio.Event()
        transport_ready = asyncio.Event()
        stop = asyncio.Event()
        connection = BinanceWebSocketConnection(
            url=self._url,
            subscriptions=self._subscriptions,
            identity=identity,
            ingest=self._ingest,
            snapshot_requests=self._snapshot_requests(),
            rest=self._rest,
            ready=ready,
            stop=stop,
            receive_timeout_seconds=self._receive_timeout_seconds,
            ping_interval_seconds=self._ping_interval_seconds,
            ping_timeout_seconds=self._ping_timeout_seconds,
            max_queue=self._websocket_max_queue,
            max_message_bytes=self._websocket_max_message_bytes,
            updates=self._updates,
            on_depth_gap=self._open_depth_gap,
            on_depth_reanchored=self._close_depth_gap,
            on_event=self._mark_event,
            subscription_audit_seconds=self._subscription_audit_seconds,
            subscription_audit_timeout_seconds=self._subscription_audit_timeout_seconds,
            subscription_audit_failures_before_reconnect=(
                self._subscription_audit_failures_before_reconnect
            ),
            transport_ready=transport_ready,
            transport_ready_keys=tuple(
                (stream_type, symbol)
                for stream_type in self._liveness_stream_types
                for symbol in self._instruments
            ),
        )
        task = asyncio.create_task(connection.run(), name=connection_id)
        handle = ConnectionHandle(identity, ready, stop, task, transport_ready)
        ready_task = asyncio.create_task(ready.wait())
        transport_ready_task = asyncio.create_task(transport_ready.wait())
        service_stop_task = asyncio.create_task(self._service_stop.wait())
        transport_reported = False
        started_at = time.monotonic()
        try:
            while not (ready_task.done() and transport_reported):
                remaining = 600 - (time.monotonic() - started_at)
                if remaining <= 0:
                    raise TimeoutError("websocket did not become fully ready within 600 seconds")
                waiters: set[asyncio.Task[object]] = {task, service_stop_task}
                if not ready_task.done():
                    waiters.add(ready_task)
                if not transport_reported:
                    waiters.add(transport_ready_task)
                done, _ = await asyncio.wait(
                    waiters,
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    raise TimeoutError("websocket did not become fully ready within 600 seconds")
                if service_stop_task in done and self._service_stop.is_set():
                    raise asyncio.CancelledError
                if task in done:
                    error = _task_error(task)
                    if error is not None:
                        raise error
                    raise OSError("websocket closed before becoming ready")
                if transport_ready_task in done and not transport_reported:
                    if on_transport_ready is not None:
                        await on_transport_ready(identity)
                    transport_reported = True
            return handle
        except BaseException:
            await _stop_handle(handle)
            raise
        finally:
            auxiliary_waiters = (ready_task, transport_ready_task, service_stop_task)
            for waiter in auxiliary_waiters:
                waiter.cancel()
            await asyncio.gather(*auxiliary_waiters, return_exceptions=True)

    def _snapshot_requests(self) -> tuple[tuple[str, StreamType], ...]:
        return self._snapshot_requests_for(self._instruments)

    def _snapshot_requests_for(
        self, instruments: tuple[str, ...]
    ) -> tuple[tuple[str, StreamType], ...]:
        if StreamType.DEPTH not in self._stream_types:
            return ()
        requests = [(symbol, StreamType.DEPTH_SNAPSHOT) for symbol in instruments]
        if self._d0_enabled:
            requests.extend((symbol, StreamType.RPI_DEPTH_SNAPSHOT) for symbol in instruments)
        return tuple(requests)

    async def _open_depth_gap(
        self,
        connection_id: str,
        symbol: str,
        stream_type: StreamType,
        expected: int,
        received: int,
        affected_from_realtime_ns: int,
    ) -> str:
        return await self._gaps.open(
            GapReason.L2_SEQUENCE,
            connection_id=connection_id,
            exchange_symbols=(symbol,),
            stream_types=(stream_type,),
            affected_from_realtime_ns=affected_from_realtime_ns,
            detail=f"expected_pu={expected} received_pu={received}",
        )

    async def _close_depth_gap(self, gap_id: str, symbol: str, stream_type: StreamType) -> None:
        await self._gaps.close(
            gap_id,
            GapReason.L2_SEQUENCE,
            exchange_symbols=(symbol,),
            stream_types=(stream_type,),
            detail="fresh snapshot received; central must still validate the bridge",
        )

    async def _wait_current(self, handle: ConnectionHandle) -> str:
        rotation = asyncio.create_task(asyncio.sleep(self._next_rotation_seconds))
        stopping = asyncio.create_task(self._service_stop.wait())
        reconnecting = asyncio.create_task(self._reconnect_requested.wait())
        done, pending = await asyncio.wait(
            (handle.task, rotation, stopping, reconnecting),
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            if task is not handle.task:
                task.cancel()
        if stopping in done and self._service_stop.is_set():
            return "stop"
        if handle.task in done:
            self._reconnect_requested.clear()
            return "failed"
        if reconnecting in done and self._reconnect_requested.is_set():
            self._reconnect_requested.clear()
            return "reconnect"
        self._next_rotation_seconds = self._rotation_seconds
        return "rotate"

    async def _open_gap(
        self,
        reason: GapReason,
        detail: str,
        *,
        connection_id: str | None = None,
        affected_from_realtime_ns: int | None = None,
    ) -> str:
        affected_from_ns = affected_from_realtime_ns
        if affected_from_ns is None:
            affected_from_ns = min(
                (observed[1] for observed in self._last_event.values()),
                default=time.time_ns(),
            )
        return await self._gaps.open(
            reason,
            connection_id=connection_id,
            exchange_symbols=self._instruments,
            stream_types=self._stream_types,
            affected_from_realtime_ns=affected_from_ns,
            detail=f"{self._name}: {detail}"[:500],
        )

    async def _close_transport_gap(
        self,
        *,
        gap_id: str,
        reason: GapReason,
        identity: SourceIdentity,
        started_at: float | None,
    ) -> float:
        await self._gaps.close(
            gap_id,
            reason,
            connection_id=identity.connection_id,
            exchange_symbols=self._instruments,
            stream_types=self._stream_types,
            detail=(
                f"{self._name} transport recovered; L2 remains invalid until snapshot bridge"
            ),
        )
        recovered_at = time.monotonic()
        elapsed = recovered_at - started_at if started_at is not None else 0.0
        logger.info(
            "connection transport recovered route=%s connection_id=%s gap_id=%s recovery_s=%.3f",
            self._name,
            identity.connection_id,
            gap_id,
            elapsed,
        )
        return recovered_at

    async def _wait_or_stop(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._service_stop.wait(), timeout=seconds)
        except TimeoutError:
            pass

    def _activate_connection(self) -> None:
        self._connection_generation += 1
        self._reconnect_requested.clear()
        self._connection_ready.set()


class RestPollers:
    def __init__(
        self,
        *,
        config: EdgeConfig,
        instruments: tuple[str, ...],
        collector_id: str,
        boot_id: str,
        ingest: IngestCoordinator,
        queues: ByteBoundedQueues,
        gaps: GapJournal,
        rest: BinanceRestClient,
        stop: asyncio.Event,
        on_ready: Callable[[str], None],
        on_discovery: Callable[[DiscoverySnapshot], Awaitable[None]],
    ) -> None:
        self._config = config
        self._instruments = set(instruments)
        self._ingest = ingest
        self._queues = queues
        self._gaps = gaps
        self._rest = rest
        self._stop = stop
        self._on_ready = on_ready
        self._on_discovery = on_discovery
        self._oi_tasks: dict[str, asyncio.Task[None]] = {}
        self._oi_first_pass: dict[str, asyncio.Future[None]] = {}
        self._oi_lock = asyncio.Lock()
        self._oi_ready_reported = False
        self._oi_identity = SourceIdentity(
            collector_id, boot_id, uuid4().hex, f"rest-open-interest-{uuid4().hex}"
        )
        self._discovery_identity = SourceIdentity(
            collector_id, boot_id, uuid4().hex, f"rest-discovery-{uuid4().hex}"
        )

    async def run(self) -> None:
        await asyncio.gather(self._open_interest_loop(), self._discovery_loop(), self._clock_loop())

    async def _open_interest_loop(self) -> None:
        await self._replace_open_interest_tasks(tuple(sorted(self._instruments)))
        try:
            while not self._stop.is_set():
                for symbol, task in tuple(self._oi_tasks.items()):
                    if task.done() and not task.cancelled():
                        error = task.exception()
                        if error is not None:
                            raise RuntimeError(f"open-interest task failed for {symbol}") from error
                await _wait_event(self._stop, 1)
        finally:
            tasks = list(self._oi_tasks.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._oi_tasks.clear()

    async def update_instruments(self, instruments: tuple[str, ...]) -> None:
        await self._replace_open_interest_tasks(instruments)

    async def _replace_open_interest_tasks(self, instruments: tuple[str, ...]) -> None:
        async with self._oi_lock:
            proposed = set(instruments)
            removed = set(self._oi_tasks) - proposed
            removed_tasks = [self._oi_tasks.pop(symbol) for symbol in removed]
            for symbol in removed:
                self._oi_first_pass.pop(symbol, None)
            for task in removed_tasks:
                task.cancel()
            if removed_tasks:
                await asyncio.gather(*removed_tasks, return_exceptions=True)

            added = tuple(sorted(proposed - set(self._oi_tasks)))
            interval = self._config.open_interest_interval_seconds
            count = max(1, len(added))
            for index, symbol in enumerate(added):
                first_pass = asyncio.get_running_loop().create_future()
                self._oi_first_pass[symbol] = first_pass
                self._oi_tasks[symbol] = asyncio.create_task(
                    self._open_interest_symbol_loop(
                        symbol,
                        first_pass,
                        initial_delay=index * interval / count,
                    ),
                    name=f"open-interest-{symbol}",
                )
            self._instruments = proposed
        if added:
            await asyncio.wait_for(
                asyncio.gather(*(self._oi_first_pass[symbol] for symbol in added)),
                timeout=max(120, self._config.open_interest_interval_seconds * 2),
            )
        if not self._oi_ready_reported and set(self._oi_first_pass) == proposed:
            first_passes = self._oi_first_pass.values()
            if all(future.done() and future.exception() is None for future in first_passes):
                self._oi_ready_reported = True
                self._on_ready("open_interest")

    async def _open_interest_symbol_loop(
        self, symbol: str, first_pass: asyncio.Future[None], *, initial_delay: float
    ) -> None:
        gap: tuple[str, GapReason] | None = None
        streams = (StreamType.OPEN_INTEREST,)
        symbols = (symbol,)
        interval = self._config.open_interest_interval_seconds
        next_poll = time.monotonic() + initial_delay
        while not self._stop.is_set():
            await _wait_event(self._stop, max(0.0, next_poll - time.monotonic()))
            if self._stop.is_set():
                return
            try:
                await self._fetch_oi(symbol)
                gap = await self._close_poll_gap(gap, symbols, streams)
                if not first_pass.done():
                    first_pass.set_result(None)
            except QueueOverloaded as exc:
                gap = await self._open_poll_gap(
                    gap,
                    GapReason.INGEST_OVERLOAD,
                    symbols,
                    streams,
                    str(exc),
                )
                await self._queues.wait_until_resumable()
            except (aiohttp.ClientError, TimeoutError) as exc:
                gap = await self._open_poll_gap(
                    gap,
                    GapReason.CONNECTION_LOST,
                    symbols,
                    streams,
                    str(exc),
                )
                logger.warning("open-interest poll failed symbol=%s error=%s", symbol, exc)
            finally:
                next_poll = _advance_deadline(next_poll, interval, time.monotonic())

    async def _fetch_oi(self, symbol: str) -> None:
        payload, requested_at, observed_at, request_id = await self._rest.fetch(
            "/fapi/v1/openInterest", params={"symbol": symbol}
        )
        event = self._oi_identity.event(
            stream_type=StreamType.OPEN_INTEREST,
            exchange_symbol=symbol,
            payload=payload,
            realtime_ns=observed_at,
            monotonic_ns=time.monotonic_ns(),
            request_id=request_id,
            request_realtime_ns=requested_at,
        )
        await self._ingest.put(event)

    async def _discovery_loop(self) -> None:
        gap: tuple[str, GapReason] | None = None
        streams = (
            StreamType.EXCHANGE_INFO,
            StreamType.MARKET_TICKERS,
            StreamType.DAILY_KLINES,
            StreamType.LIQUIDITY_DEPTH,
        )
        while not self._stop.is_set():
            try:
                exchange_info, first_observed_at = await self._fetch_discovery(
                    "/fapi/v1/exchangeInfo", StreamType.EXCHANGE_INFO
                )
                market_tickers, _ = await self._fetch_discovery(
                    "/fapi/v1/ticker/24hr", StreamType.MARKET_TICKERS
                )
                observed = datetime.fromtimestamp(first_observed_at / 1_000_000_000, UTC)
                daily_klines = await self._fetch_daily_klines(exchange_info, observed)
                validation_symbols = liquidity_validation_symbols(
                    exchange_info,
                    daily_klines,
                    policy=self._config.universe.rolling_policy(),
                )
                liquidity_depth = await self._fetch_liquidity_depth(validation_symbols)
                confirmation, observed_at = await self._fetch_discovery(
                    "/fapi/v1/exchangeInfo", StreamType.EXCHANGE_INFO
                )
                snapshot = DiscoverySnapshot(
                    observed_at=datetime.fromtimestamp(observed_at / 1_000_000_000, UTC),
                    exchange_info=exchange_info,
                    exchange_info_confirmation=confirmation,
                    market_tickers=market_tickers,
                    daily_klines=daily_klines,
                    liquidity_depth=liquidity_depth,
                )
                await self._on_discovery(snapshot)
                gap = await self._close_poll_gap(gap, (), streams)
                self._on_ready("discovery")
                await _wait_event(self._stop, self._seconds_until_discovery())
            except (aiohttp.ClientError, QueueOverloaded, TimeoutError) as exc:
                reason = (
                    GapReason.INGEST_OVERLOAD
                    if isinstance(exc, QueueOverloaded)
                    else GapReason.CONNECTION_LOST
                )
                gap = await self._open_poll_gap(gap, reason, (), streams, str(exc))
                if isinstance(exc, QueueOverloaded):
                    await self._queues.wait_until_resumable()
                logger.warning("discovery poll failed error=%s", exc)
                await _wait_event(self._stop, 30)

    async def _fetch_discovery(self, path: str, stream_type: StreamType) -> tuple[bytes, int]:
        payload, requested_at, observed_at, request_id = await self._rest.fetch_background(path)
        event = self._discovery_identity.event(
            stream_type=stream_type,
            exchange_symbol=None,
            payload=payload,
            realtime_ns=observed_at,
            monotonic_ns=time.monotonic_ns(),
            request_id=request_id,
            request_realtime_ns=requested_at,
        )
        await self._ingest.put(event)
        return payload, observed_at

    async def _fetch_daily_klines(self, exchange_info: bytes, observed_at: datetime) -> bytes:
        cutoff = datetime.combine(observed_at.date(), datetime.min.time(), UTC)
        cutoff_ms = int(cutoff.timestamp() * 1000)
        start_ms = cutoff_ms - self._config.universe.liquidity_window_days * 86_400_000
        cached = self._cached_daily_klines()
        responses: dict[str, dict[str, object]] = {}
        for symbol, onboard_ms in _eligible_instruments(exchange_info).items():
            first_full_day = ((onboard_ms + 86_400_000 - 1) // 86_400_000) * 86_400_000
            expected = tuple(range(max(start_ms, first_full_day), cutoff_ms, 86_400_000))
            cached_value = cached.get(symbol, {})
            cached_payload = cached_value.get("payload", [])
            if not isinstance(cached_payload, list):
                cached_payload = []
            by_open = {
                int(row[0]): row
                for row in cached_payload
                if isinstance(row, list) and len(row) >= 9 and int(row[0]) in expected
            }
            cached_hashes = cached_value.get("source_response_sha256s", [])
            source_hashes = (
                [str(value) for value in cached_hashes] if isinstance(cached_hashes, list) else []
            )
            if not source_hashes and isinstance(cached_value.get("response_sha256"), str):
                source_hashes.append(str(cached_value["response_sha256"]))
            missing = [open_ms for open_ms in expected if open_ms not in by_open]
            if missing:
                payload, _, _, _ = await self._fetch_with_retry(
                    "/fapi/v1/klines",
                    params={
                        "symbol": symbol,
                        "interval": "1d",
                        "startTime": min(missing),
                        "endTime": cutoff_ms - 1,
                        "limit": len(missing),
                    },
                )
                parsed = orjson.loads(payload)
                if not isinstance(parsed, list):
                    raise ValueError(f"daily kline response is not an array for {symbol}")
                for row in parsed:
                    if isinstance(row, list) and len(row) >= 9 and int(row[0]) in expected:
                        by_open[int(row[0])] = row
                source_hashes.append(sha256_bytes(payload))
            responses[symbol] = {
                "payload": [by_open[open_ms] for open_ms in sorted(by_open)],
                "source_response_sha256s": list(dict.fromkeys(source_hashes))[
                    -(self._config.universe.liquidity_window_days + 1) :
                ],
            }
        evidence = canonical_json_bytes(
            {
                "endpoint": "/fapi/v1/klines",
                "interval": "1d",
                "schema_version": 1,
                "window_end_exclusive_ms": cutoff_ms,
                "window_start_ms": start_ms,
                "symbols": responses,
            }
        )
        await self._emit_discovery_evidence(StreamType.DAILY_KLINES, evidence)
        return evidence

    def _cached_daily_klines(self) -> dict[str, dict[str, object]]:
        root = self._config.data_root / "control" / "universe" / "observations"
        paths = sorted(root.glob("*/daily-klines.json.gz"), reverse=True)
        if not paths:
            return {}
        try:
            payload = orjson.loads(gzip.decompress(paths[0].read_bytes()))
        except (OSError, orjson.JSONDecodeError):
            logger.warning("ignoring unreadable daily kline cache path=%s", paths[0])
            return {}
        symbols = payload.get("symbols") if isinstance(payload, dict) else None
        if not isinstance(symbols, dict):
            logger.warning("ignoring invalid daily kline cache path=%s", paths[0])
            return {}
        return {
            str(symbol): value
            for symbol, value in symbols.items()
            if isinstance(symbol, str) and isinstance(value, dict)
        }

    async def _fetch_liquidity_depth(self, symbols: tuple[str, ...]) -> bytes:
        samples: dict[str, list[dict[str, object]]] = {symbol: [] for symbol in symbols}
        book_tickers: list[dict[str, object]] = []
        for sample_number in range(1, self._config.universe.liquidity_book_ticker_samples + 1):
            payload, _, _, _ = await self._fetch_with_retry("/fapi/v1/ticker/bookTicker", params={})
            parsed = orjson.loads(payload)
            if not isinstance(parsed, list):
                raise ValueError("book ticker response is not an array")
            book_tickers.append(
                {
                    "payload": parsed,
                    "response_sha256": sha256_bytes(payload),
                    "sample": sample_number,
                }
            )
            if sample_number < self._config.universe.liquidity_book_ticker_samples:
                await _wait_event(self._stop, 5)
        rounds = self._config.universe.liquidity_depth_samples
        for round_number in range(1, rounds + 1):
            for symbol in symbols:
                payload, _, _, _ = await self._fetch_with_retry(
                    "/fapi/v1/depth", params={"symbol": symbol, "limit": 100}
                )
                parsed = orjson.loads(payload)
                if not isinstance(parsed, dict):
                    raise ValueError(f"depth response is not an object for {symbol}")
                samples[symbol].append(
                    {
                        "payload": parsed,
                        "response_sha256": sha256_bytes(payload),
                        "round": round_number,
                    }
                )
                await _wait_event(
                    self._stop,
                    self._config.universe.liquidity_request_interval_seconds,
                )
            if round_number < rounds:
                await _wait_event(self._stop, 5)
        evidence = canonical_json_bytes(
            {
                "depth_limit": 100,
                "endpoint": "/fapi/v1/depth",
                "book_ticker_endpoint": "/fapi/v1/ticker/bookTicker",
                "book_tickers": book_tickers,
                "rounds": rounds,
                "schema_version": 1,
                "symbols": samples,
            }
        )
        await self._emit_discovery_evidence(StreamType.LIQUIDITY_DEPTH, evidence)
        return evidence

    async def _fetch_with_retry(
        self, path: str, *, params: dict[str, str | int]
    ) -> tuple[bytes, int, int, str]:
        delay = 1.0
        for attempt in range(4):
            try:
                return await self._rest.fetch_background(path, params=params)
            except (aiohttp.ClientError, TimeoutError):
                if attempt == 3:
                    raise
                await _wait_event(self._stop, delay)
                delay = min(delay * 2, 8)
        raise AssertionError("unreachable")

    async def _emit_discovery_evidence(self, stream_type: StreamType, payload: bytes) -> None:
        event = self._discovery_identity.event(
            stream_type=stream_type,
            exchange_symbol=None,
            payload=payload,
            realtime_ns=time.time_ns(),
            monotonic_ns=time.monotonic_ns(),
        )
        await self._ingest.put(event)

    def _seconds_until_discovery(self) -> float:
        now = datetime.now(UTC)
        scheduled = datetime.combine(
            now.date(),
            datetime.min.time().replace(
                hour=self._config.universe.discovery_hour_utc,
                minute=self._config.universe.discovery_minute_utc,
            ),
            UTC,
        )
        if scheduled <= now:
            scheduled += timedelta(days=1)
        return (scheduled - now).total_seconds()

    async def _clock_loop(self) -> None:
        gap: tuple[str, GapReason] | None = None
        streams = (StreamType.CLOCK_SAMPLE,)
        while not self._stop.is_set():
            try:
                payload, requested_at, observed_at, request_id = await self._rest.fetch(
                    "/fapi/v1/time"
                )
                event = self._discovery_identity.event(
                    stream_type=StreamType.CLOCK_SAMPLE,
                    exchange_symbol=None,
                    payload=payload,
                    realtime_ns=observed_at,
                    monotonic_ns=time.monotonic_ns(),
                    request_id=request_id,
                    request_realtime_ns=requested_at,
                )
                await self._ingest.put(event)
                gap = await self._close_poll_gap(gap, (), streams)
                self._on_ready("clock")
                await _wait_event(self._stop, self._config.clock_sample_interval_seconds)
            except (aiohttp.ClientError, QueueOverloaded, TimeoutError) as exc:
                reason = (
                    GapReason.INGEST_OVERLOAD
                    if isinstance(exc, QueueOverloaded)
                    else GapReason.CONNECTION_LOST
                )
                gap = await self._open_poll_gap(gap, reason, (), streams, str(exc))
                if isinstance(exc, QueueOverloaded):
                    await self._queues.wait_until_resumable()
                logger.warning("clock sample failed error=%s", exc)
                await _wait_event(self._stop, 10)

    async def _open_poll_gap(
        self,
        current: tuple[str, GapReason] | None,
        reason: GapReason,
        symbols: tuple[str, ...],
        streams: tuple[StreamType, ...],
        detail: str,
    ) -> tuple[str, GapReason]:
        if current is not None:
            return current
        gap_id = await self._gaps.open(
            reason,
            exchange_symbols=symbols,
            stream_types=streams,
            detail=detail[:500],
        )
        return gap_id, reason

    async def _close_poll_gap(
        self,
        current: tuple[str, GapReason] | None,
        symbols: tuple[str, ...],
        streams: tuple[StreamType, ...],
    ) -> tuple[str, GapReason] | None:
        if current is None:
            return None
        gap_id, reason = current
        await self._gaps.close(
            gap_id,
            reason,
            exchange_symbols=symbols,
            stream_types=streams,
            detail="REST polling recovered",
        )
        return None


class SourceManager:
    def __init__(
        self,
        config: EdgeConfig,
        *,
        collector_id: str,
        boot_id: str,
        ingest: IngestCoordinator,
        queues: ByteBoundedQueues,
        gaps: GapJournal,
        on_discovery: Callable[[DiscoverySnapshot], Awaitable[None]],
    ) -> None:
        self._config = config
        self._collector_id = collector_id
        self._boot_id = boot_id
        self._ingest = ingest
        self._queues = queues
        self._gaps = gaps
        self._on_discovery = on_discovery
        self._stop: asyncio.Event | None = None
        self._task: asyncio.Task[None] | None = None
        self._ready: asyncio.Event | None = None
        self._routes: dict[str, RouteRunner] = {}
        self._pollers: RestPollers | None = None
        self._instruments: tuple[str, ...] = ()
        self._update_lock = asyncio.Lock()
        self._public_sharder = StableWeightedSharder(
            config.public_connection_shards,
            config.public_symbol_load_weights,
        )

    @property
    def running(self) -> bool:
        return self._task is not None

    def raise_if_failed(self) -> None:
        if self._task is None or not self._task.done():
            return
        error = _task_error(self._task)
        if error is not None:
            raise RuntimeError("Binance source group failed") from error
        raise RuntimeError("Binance source group stopped unexpectedly")

    async def start(self, instruments: tuple[str, ...]) -> None:
        if self._task is not None:
            raise RuntimeError("sources already running")
        self._stop = asyncio.Event()
        self._ready = asyncio.Event()
        self._instruments = instruments
        self._task = asyncio.create_task(
            self._run(instruments, self._stop, self._ready), name="binance-sources"
        )

    async def wait_ready(self) -> None:
        if self._task is None or self._ready is None:
            raise RuntimeError("sources are not running")
        ready_task = asyncio.create_task(self._ready.wait())
        done, pending = await asyncio.wait(
            (ready_task, self._task), timeout=600, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            if task is not self._task:
                task.cancel()
        if self._task in done:
            error = _task_error(self._task)
            if error is not None:
                raise RuntimeError("Binance sources failed before readiness") from error
            raise RuntimeError("Binance sources stopped before readiness")
        if ready_task not in done:
            raise TimeoutError("Binance sources did not become ready within 600 seconds")

    async def stop(self) -> None:
        if self._task is None or self._stop is None:
            return
        self._stop.set()
        await asyncio.gather(self._task, return_exceptions=True)
        self._task = None
        self._stop = None
        self._ready = None
        self._routes = {}
        self._pollers = None

    async def update_instruments(self, instruments: tuple[str, ...]) -> None:
        if self._task is None or self._pollers is None:
            raise RuntimeError("sources are not running")
        async with self._update_lock:
            shards = self._public_sharder.shards(instruments)
            updates = [
                self._routes[f"public-{index}"].update_instruments(shard)
                for index, shard in enumerate(shards)
            ]
            updates.append(self._routes["market-0"].update_instruments(instruments))
            updates.append(self._pollers.update_instruments(instruments))
            await asyncio.gather(*updates)
            self._instruments = instruments

    async def _run(
        self, instruments: tuple[str, ...], stop: asyncio.Event, ready: asyncio.Event
    ) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            rest = BinanceRestClient(
                self._config.rest_url,
                session,
                snapshot_interval_seconds=self._config.snapshot_request_interval_seconds,
                snapshot_max_concurrency=self._config.snapshot_request_concurrency,
            )
            routes: list[Awaitable[None]] = []
            shards = self._public_sharder.shards(instruments)
            expected = {
                *(f"public-{index}" for index in range(len(shards))),
                "market-0",
                "open_interest",
                "discovery",
                "clock",
            }
            ready_names: set[str] = set()

            def mark_ready(name: str) -> None:
                ready_names.add(name)
                if expected <= ready_names:
                    ready.set()

            for index, shard in enumerate(shards):
                public_stream_types = [StreamType.BOOK_TICKER, StreamType.DEPTH]
                if self._config.d0_enabled:
                    public_stream_types.extend((StreamType.TRADE, StreamType.RPI_DEPTH))
                snapshot_count = len(shard) * (2 if self._config.d0_enabled else 1)
                rotation_offset = index * (
                    snapshot_count * self._config.snapshot_request_interval_seconds
                    + self._config.connection_overlap_seconds
                )
                runner = RouteRunner(
                    name=f"public-{index}",
                    url=self._config.public_ws_url,
                    subscriptions=public_subscriptions(shard, d0_enabled=self._config.d0_enabled),
                    instruments=shard,
                    stream_types=tuple(public_stream_types),
                    collector_id=self._collector_id,
                    boot_id=self._boot_id,
                    ingest=self._ingest,
                    queues=self._queues,
                    gaps=self._gaps,
                    rest=rest,
                    rotation_seconds=self._config.connection_rotation_seconds,
                    rotation_offset_seconds=rotation_offset,
                    overlap_seconds=self._config.connection_overlap_seconds,
                    receive_timeout_seconds=self._config.websocket_receive_timeout_seconds,
                    ping_interval_seconds=self._config.websocket_ping_interval_seconds,
                    ping_timeout_seconds=self._config.websocket_ping_timeout_seconds,
                    service_stop=stop,
                    d0_enabled=self._config.d0_enabled,
                    on_ready=mark_ready,
                    subscriptions_for=lambda values: public_subscriptions(
                        values, d0_enabled=self._config.d0_enabled
                    ),
                    liveness_timeout_seconds=self._config.public_stream_liveness_seconds,
                    liveness_stream_types=(StreamType.BOOK_TICKER, StreamType.DEPTH),
                    subscription_audit_seconds=self._config.subscription_audit_seconds,
                    subscription_audit_timeout_seconds=(
                        self._config.subscription_audit_timeout_seconds
                    ),
                    subscription_audit_failures_before_reconnect=(
                        self._config.subscription_audit_failures_before_reconnect
                    ),
                    refresh_failures_before_reconnect=(
                        self._config.refresh_failures_before_reconnect
                    ),
                    websocket_max_queue=self._config.websocket_max_queue,
                    websocket_max_message_bytes=self._config.websocket_max_message_bytes,
                )
                self._routes[f"public-{index}"] = runner
                routes.extend((runner.run(), runner.liveness_loop()))
            market_runner = RouteRunner(
                name="market-0",
                url=self._config.market_ws_url,
                subscriptions=market_subscriptions(instruments),
                instruments=instruments,
                stream_types=(
                    StreamType.AGG_TRADE,
                    StreamType.MARK_PRICE,
                    StreamType.FORCE_ORDER,
                    StreamType.CONTRACT_INFO,
                ),
                collector_id=self._collector_id,
                boot_id=self._boot_id,
                ingest=self._ingest,
                queues=self._queues,
                gaps=self._gaps,
                rest=rest,
                rotation_seconds=self._config.connection_rotation_seconds,
                overlap_seconds=self._config.connection_overlap_seconds,
                receive_timeout_seconds=self._config.websocket_receive_timeout_seconds,
                ping_interval_seconds=self._config.websocket_ping_interval_seconds,
                ping_timeout_seconds=self._config.websocket_ping_timeout_seconds,
                service_stop=stop,
                on_ready=mark_ready,
                subscriptions_for=market_subscriptions,
                liveness_timeout_seconds=self._config.mark_price_liveness_seconds,
                liveness_stream_types=(StreamType.MARK_PRICE,),
                subscription_audit_seconds=self._config.subscription_audit_seconds,
                subscription_audit_timeout_seconds=(
                    self._config.subscription_audit_timeout_seconds
                ),
                subscription_audit_failures_before_reconnect=(
                    self._config.subscription_audit_failures_before_reconnect
                ),
                refresh_failures_before_reconnect=(
                    self._config.refresh_failures_before_reconnect
                ),
                websocket_max_queue=self._config.websocket_max_queue,
                websocket_max_message_bytes=self._config.websocket_max_message_bytes,
            )
            self._routes["market-0"] = market_runner
            routes.extend((market_runner.run(), market_runner.liveness_loop()))
            pollers = RestPollers(
                config=self._config,
                instruments=instruments,
                collector_id=self._collector_id,
                boot_id=self._boot_id,
                ingest=self._ingest,
                queues=self._queues,
                gaps=self._gaps,
                rest=rest,
                stop=stop,
                on_ready=mark_ready,
                on_discovery=self._on_discovery,
            )
            self._pollers = pollers
            routes.append(pollers.run())
            try:
                await asyncio.gather(*routes)
            finally:
                self._pollers = None


def _liveness_subscription(key: tuple[StreamType, str]) -> str:
    stream_type, symbol = key
    suffixes = {
        StreamType.DEPTH: "depth@100ms",
        StreamType.RPI_DEPTH: "rpiDepth@500ms",
        StreamType.BOOK_TICKER: "bookTicker",
        StreamType.MARK_PRICE: "markPrice@1s",
    }
    try:
        suffix = suffixes[stream_type]
    except KeyError as exc:
        raise ValueError(f"unsupported liveness subscription: {stream_type.value}") from exc
    return f"{symbol.lower()}@{suffix}"


def _snapshot_type(stream_type: StreamType) -> StreamType:
    if stream_type is StreamType.DEPTH:
        return StreamType.DEPTH_SNAPSHOT
    if stream_type is StreamType.RPI_DEPTH:
        return StreamType.RPI_DEPTH_SNAPSHOT
    raise ValueError(f"stream has no snapshot type: {stream_type.value}")


def _eligible_instruments(exchange_info: bytes) -> dict[str, int]:
    payload = orjson.loads(exchange_info)
    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), list):
        raise ValueError("exchangeInfo must contain a symbols array")
    symbols: dict[str, int] = {}
    for item in payload["symbols"]:
        if not isinstance(item, dict) or not isinstance(item.get("symbol"), str):
            raise ValueError("exchangeInfo contains an invalid symbol row")
        symbol = str(item["symbol"])
        if (
            item.get("status") == "TRADING"
            and item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") == "USDT"
            and item.get("marginAsset") == "USDT"
            and SYMBOL_PATTERN.fullmatch(symbol)
        ):
            try:
                onboard_ms = int(str(item["onboardDate"]))
            except (KeyError, ValueError) as exc:
                raise ValueError(f"invalid onboardDate for {symbol}") from exc
            if onboard_ms <= 0:
                raise ValueError(f"invalid onboardDate for {symbol}")
            symbols[symbol] = onboard_ms
    return dict(sorted(symbols.items()))


async def _stop_handle(handle: ConnectionHandle) -> None:
    handle.stop.set()
    handle.task.cancel()
    await asyncio.gather(handle.task, return_exceptions=True)


def _task_error(task: asyncio.Task[None]) -> BaseException | None:
    if task.cancelled():
        return asyncio.CancelledError()
    return task.exception()


def _error_affected_from(error: BaseException | None) -> int | None:
    if isinstance(error, SubscriptionAuditError):
        return error.affected_from_realtime_ns
    return None


def _reconnect_delay(failures: int) -> float:
    exponential = min(30.0, float(2 ** max(0, failures - 1)))
    return min(30.0, exponential * random.uniform(0.8, 1.2))


async def _wait_event(event: asyncio.Event, delay_seconds: float) -> None:
    try:
        await asyncio.wait_for(event.wait(), timeout=delay_seconds)
    except TimeoutError:
        pass


def _advance_deadline(previous: float, interval: float, now: float) -> float:
    deadline = previous + interval
    if deadline <= now:
        missed = int((now - deadline) // interval) + 1
        deadline += missed * interval
    return deadline
