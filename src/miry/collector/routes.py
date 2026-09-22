from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from uuid import uuid4

import aiohttp
from websockets.exceptions import ConnectionClosed

from miry.collector.gaps import GapJournal
from miry.collector.ingest import IngestCoordinator
from miry.collector.queue import ByteBoundedQueues, QueueOverloaded
from miry.collector.rest import BinanceRestClient
from miry.collector.websocket import (
    BinanceWebSocketConnection,
    SourceIdentity,
)
from miry.collector.ws_control import SubscriptionAuditError, SubscriptionUpdate
from miry.contracts.models import GapReason, StreamType

logger = logging.getLogger(__name__)


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
        on_message: Callable[[str, str, int], None] | None = None,
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
        self._on_message = on_message
        self._subscriptions_for = subscriptions_for
        self._updates: asyncio.Queue[SubscriptionUpdate] = asyncio.Queue(maxsize=1)
        self._update_lock = asyncio.Lock()
        self._reconnect_requested = asyncio.Event()
        self._liveness_timeout_seconds = liveness_timeout_seconds
        self._liveness_stream_types = tuple(
            dict.fromkeys(liveness_stream_types or stream_types)
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

    @property
    def ready(self) -> bool:
        return self._connection_ready.is_set() and not self._reconnect_requested.is_set()

    async def update_instruments(self, instruments: tuple[str, ...]) -> None:
        if self._subscriptions_for is None:
            raise RuntimeError(f"route {self._name} does not support live updates")
        async with self._update_lock:
            if instruments == self._instruments:
                return
            previous = set(self._instruments)
            proposed = set(instruments)
            added = tuple(sorted(proposed - previous))
            old_subscriptions = set(self._subscriptions)
            new_subscriptions = self._subscriptions_for(instruments)
            readiness_started = time.monotonic()
            realtime_ns = time.time_ns()
            readiness_keys = tuple(
                (stream_type, symbol)
                for stream_type in self._liveness_stream_types
                for symbol in added
            )
            for key in readiness_keys:
                self._last_event[key] = (readiness_started, realtime_ns)
            try:
                await self._submit_update(
                    add=tuple(sorted(set(new_subscriptions) - old_subscriptions)),
                    remove=tuple(sorted(old_subscriptions - set(new_subscriptions))),
                    snapshot_requests=self._snapshot_requests_for(added),
                )
                if readiness_keys:
                    await self._wait_for_fresh_events(
                        readiness_keys,
                        after=readiness_started,
                        timeout_seconds=max(30.0, self._liveness_timeout_seconds or 0.0),
                    )
            except BaseException:
                self._last_event = {
                    key: value
                    for key, value in self._last_event.items()
                    if key[1] in previous
                }
                self._reconnect_requested.set()
                raise
            self._subscriptions = new_subscriptions
            self._instruments = instruments
            now = time.monotonic()
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
            await self._close_recovered_liveness_gaps(active_gaps)
            now = time.monotonic()
            stale = tuple(
                (stream_type, symbol, observed_realtime_ns)
                for (stream_type, symbol), (
                    observed_monotonic,
                    observed_realtime_ns,
                ) in self._last_event.items()
                if symbol in self._instruments
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
                    stale_keys,
                    after=refreshed_after,
                    timeout_seconds=timeout,
                )
                await self._close_recovered_liveness_gaps(active_gaps)
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

    async def _close_recovered_liveness_gaps(
        self, active_gaps: dict[tuple[StreamType, str], tuple[str, float]]
    ) -> None:
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
        started = loop.create_future()
        acknowledged = loop.create_future()
        completion = loop.create_future()
        try:
            async with asyncio.timeout(180):
                await self._updates.put(
                    SubscriptionUpdate(
                        add=add,
                        remove=remove,
                        snapshot_requests=snapshot_requests,
                        started=started,
                        acknowledged=acknowledged,
                        completion=completion,
                    )
                )
                await started
            async with asyncio.timeout(self._subscription_audit_timeout_seconds):
                await acknowledged
            async with asyncio.timeout(180):
                await completion
        except asyncio.CancelledError as exc:
            # A connection may cancel a shared future without cancelling this
            # caller. Treat that as a failed update so liveness can reconcile
            # its open gaps after reconnect; preserve actual caller shutdown.
            caller = asyncio.current_task()
            if caller is not None and caller.cancelling():
                raise
            raise ConnectionError(
                f"{self._name}: subscription update interrupted by connection shutdown"
            ) from exc
        finally:
            for future in (started, acknowledged, completion):
                if not future.done():
                    future.cancel()

    def _mark_event(
        self,
        stream_type: StreamType,
        symbol: str | None,
        realtime_ns: int | None = None,
        monotonic_ns: int | None = None,
    ) -> None:
        observed_realtime_ns = realtime_ns
        if symbol is not None and self._on_message is not None:
            if observed_realtime_ns is None:
                observed_realtime_ns = time.time_ns()
            self._on_message(self._name, symbol, observed_realtime_ns)
        key = (stream_type, symbol)
        if key in self._last_event:
            if observed_realtime_ns is None:
                observed_realtime_ns = time.time_ns()
            observed_monotonic = (
                monotonic_ns / 1_000_000_000
                if monotonic_ns is not None
                else time.monotonic()
            )
            self._last_event[key] = (observed_monotonic, observed_realtime_ns)
            if not self._liveness_changed.is_set():
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
        l2_gap_id: str | None = None
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
                    if l2_gap_id is not None:
                        await self._close_l2_reanchor_gap(
                            l2_gap_id, current.identity.connection_id
                        )
                        l2_gap_id = None
                    self._activate_connection()
                    active_since = time.monotonic()
                    self._on_ready(self._name)
                    if transport_recovered_at is not None:
                        now = time.monotonic()
                        logger.info(
                            "connection L2 bridged route=%s connection_id=%s "
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
                        if l2_gap_id is None:
                            l2_gap_id = await self._open_l2_reanchor_gap(str(exc))
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
                        if l2_gap_id is None:
                            l2_gap_id = await self._open_l2_reanchor_gap(
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
                    task_error(current.task)
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
                if l2_gap_id is None:
                    l2_gap_id = await self._open_l2_reanchor_gap(
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
                    error = task_error(task)
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
            detail="snapshot bridge verified for the active depth sequence",
        )

    async def _open_l2_reanchor_gap(
        self,
        detail: str,
        *,
        connection_id: str | None = None,
        affected_from_realtime_ns: int | None = None,
    ) -> str | None:
        stream_types = tuple(
            stream_type
            for stream_type in self._stream_types
            if stream_type in {StreamType.DEPTH, StreamType.RPI_DEPTH}
        )
        if not stream_types:
            return None
        affected_from_ns = affected_from_realtime_ns
        if affected_from_ns is None:
            affected_from_ns = min(
                (
                    observed[1]
                    for key, observed in self._last_event.items()
                    if key[0] in stream_types
                ),
                default=time.time_ns(),
            )
        return await self._gaps.open(
            GapReason.L2_REANCHOR,
            connection_id=connection_id,
            exchange_symbols=self._instruments,
            stream_types=stream_types,
            affected_from_realtime_ns=affected_from_ns,
            detail=f"{self._name}: {detail}"[:500],
        )

    async def _close_l2_reanchor_gap(self, gap_id: str, connection_id: str) -> None:
        stream_types = tuple(
            stream_type
            for stream_type in self._stream_types
            if stream_type in {StreamType.DEPTH, StreamType.RPI_DEPTH}
        )
        await self._gaps.close(
            gap_id,
            GapReason.L2_REANCHOR,
            connection_id=connection_id,
            exchange_symbols=self._instruments,
            stream_types=stream_types,
            detail=f"{self._name} snapshot bridges verified",
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


async def _stop_handle(handle: ConnectionHandle) -> None:
    handle.stop.set()
    handle.task.cancel()
    await asyncio.gather(handle.task, return_exceptions=True)


def task_error(task: asyncio.Task[None]) -> BaseException | None:
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
