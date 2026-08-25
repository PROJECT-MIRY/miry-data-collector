from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

import aiohttp

from miry.collector.config import CollectorConfig
from miry.collector.gaps import GapJournal
from miry.collector.ingest import IngestCoordinator
from miry.collector.polling import RestPollers
from miry.collector.queue import ByteBoundedQueues
from miry.collector.readiness import SourceReadiness
from miry.collector.rest import BinanceRestClient
from miry.collector.routes import RouteRunner, task_error
from miry.collector.sharding import TrafficSharder
from miry.collector.traffic import PublicTrafficRecorder
from miry.collector.websocket import (
    market_subscriptions,
    public_subscriptions,
)
from miry.contracts.models import StreamType
from miry.universe.models import DiscoverySnapshot

logger = logging.getLogger(__name__)


class SourceUpdateError(RuntimeError):
    pass


class SourceManager:
    def __init__(
        self,
        config: CollectorConfig,
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
        self._readiness: SourceReadiness | None = None
        self._routes: dict[str, RouteRunner] = {}
        self._pollers: RestPollers | None = None
        self._instruments: tuple[str, ...] = ()
        self._update_lock = asyncio.Lock()
        self._traffic = PublicTrafficRecorder(
            config.data_root / "control/public-message-rates.json",
            config.message_rates,
        )
        self._public_sharder = TrafficSharder(
            config.public_connection_shards,
            self._traffic.effective_rates(),
        )

    @property
    def running(self) -> bool:
        return self._task is not None

    @property
    def ready_for_rebalance(self) -> bool:
        return (
            self._task is not None
            and self._stop is not None
            and not self._stop.is_set()
            and all(
                self._routes[f"public-{index}"].ready
                for index in range(self._config.public_connection_shards)
            )
        )

    def raise_if_failed(self) -> None:
        if self._task is None or not self._task.done():
            return
        error = task_error(self._task)
        if error is not None:
            raise RuntimeError("Binance source group failed") from error
        raise RuntimeError("Binance source group stopped unexpectedly")

    async def start(self, instruments: tuple[str, ...]) -> None:
        if self._task is not None:
            raise RuntimeError("sources already running")
        self._stop = asyncio.Event()
        self._readiness = SourceReadiness(
            min(self._config.public_connection_shards, len(instruments))
        )
        self._instruments = instruments
        self._task = asyncio.create_task(
            self._run(instruments, self._stop, self._readiness), name="binance-sources"
        )

    async def wait_ready(self) -> None:
        if self._readiness is None:
            raise RuntimeError("sources are not running")
        await self._wait_for_readiness(self._readiness.realtime, "realtime")

    async def wait_discovery_ready(self) -> None:
        if self._readiness is None:
            raise RuntimeError("sources are not running")
        await self._wait_for_readiness(self._readiness.discovery, "discovery")

    async def _wait_for_readiness(self, event: asyncio.Event, phase: str) -> None:
        if self._task is None:
            raise RuntimeError("sources are not running")
        ready_task = asyncio.create_task(event.wait())
        done, pending = await asyncio.wait(
            (ready_task, self._task), timeout=600, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            if task is not self._task:
                task.cancel()
        if self._task in done:
            error = task_error(self._task)
            if error is not None:
                raise RuntimeError(f"Binance sources failed before {phase} readiness") from error
            raise RuntimeError(f"Binance sources stopped before {phase} readiness")
        if ready_task not in done:
            raise TimeoutError(f"Binance {phase} sources did not become ready within 600 seconds")

    async def stop(self) -> None:
        if self._task is None or self._stop is None:
            return
        self._stop.set()
        await asyncio.gather(self._task, return_exceptions=True)
        self._task = None
        self._stop = None
        self._readiness = None
        self._routes = {}
        self._pollers = None

    async def update_instruments(self, instruments: tuple[str, ...]) -> None:
        if self._task is None or self._pollers is None:
            raise RuntimeError("sources are not running")
        async with self._update_lock:
            sharder = self._public_sharder.copy()
            shards = sharder.shards(instruments)
            plans = self._public_route_plans(shards)
            plans.extend(
                (
                    (
                        self._instruments,
                        instruments,
                        self._routes["market-0"].update_instruments,
                    ),
                    (self._instruments, instruments, self._pollers.update_instruments),
                )
            )
            await _add_ready_remove(plans)
            self._public_sharder = sharder
            self._instruments = instruments

    async def rebalance_public_routes(self) -> bool:
        if self._task is None or self._stop is None or self._stop.is_set():
            raise SourceUpdateError("sources are not available for route rebalance")
        async with self._update_lock:
            if self._task is None or self._stop is None or self._stop.is_set():
                raise SourceUpdateError("sources stopped before route rebalance")
            if not self._traffic.has_complete_evidence:
                logger.info(
                    "public route rebalance skipped: 24 complete traffic blocks unavailable"
                )
                return False
            committed_shards = self._public_sharder.shards(self._instruments)
            current_shards = self._current_public_shards()
            if current_shards != committed_shards:
                await _add_ready_remove(self._public_route_plans(committed_shards))
                logger.info("public routes reconciled to committed assignment")
                return True
            sharder = TrafficSharder(
                self._config.public_connection_shards,
                self._traffic.effective_rates(),
            )
            shards = sharder.rebalance(self._instruments, current_shards)
            changed = await _add_ready_remove(self._public_route_plans(shards))
            self._public_sharder = sharder
            if changed:
                logger.info("public routes rebalanced shards=%s", shards)
            else:
                logger.info("public route rebalance skipped: assignment unchanged")
            return changed

    def _current_public_shards(self) -> tuple[tuple[str, ...], ...]:
        return tuple(
            self._routes[f"public-{index}"].instruments
            for index in range(self._config.public_connection_shards)
        )

    def _public_route_plans(
        self, shards: tuple[tuple[str, ...], ...]
    ) -> list[
        tuple[
            tuple[str, ...],
            tuple[str, ...],
            Callable[[tuple[str, ...]], Awaitable[None]],
        ]
    ]:
        return [
            (
                self._routes[f"public-{index}"].instruments,
                shard,
                self._routes[f"public-{index}"].update_instruments,
            )
            for index, shard in enumerate(shards)
        ]

    async def _run(
        self, instruments: tuple[str, ...], stop: asyncio.Event, readiness: SourceReadiness
    ) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            rest = BinanceRestClient(
                self._config.rest_url,
                session,
                snapshot_interval_seconds=self._config.snapshot_request_interval_seconds,
                snapshot_max_concurrency=self._config.snapshot_request_concurrency,
            )
            routes: list[Coroutine[Any, Any, None]] = []
            shards = self._public_sharder.shards(instruments)

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
                    on_ready=readiness.mark,
                    on_message=self._traffic.record,
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
                on_ready=readiness.mark,
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
                on_ready=readiness.mark,
                on_discovery=self._on_discovery,
            )
            self._pollers = pollers
            routes.extend((pollers.run(), self._traffic.run(stop)))
            try:
                async with asyncio.TaskGroup() as group:
                    for index, route in enumerate(routes):
                        group.create_task(route, name=f"source-{index}")
            finally:
                self._pollers = None


async def _add_ready_remove(
    plans: list[
        tuple[
            tuple[str, ...],
            tuple[str, ...],
            Callable[[tuple[str, ...]], Awaitable[None]],
        ]
    ],
) -> bool:
    changed = [(current, target, update) for current, target, update in plans if current != target]
    if not changed:
        return False
    expanded = [
        (current, tuple(sorted(set(current) | set(target))), target, update)
        for current, target, update in changed
    ]
    additions = [
        update(values) for current, values, _target, update in expanded if values != current
    ]
    if additions:
        results = await asyncio.gather(*additions, return_exceptions=True)
        failures = [result for result in results if isinstance(result, BaseException)]
        if failures:
            rollbacks = [
                update(current)
                for current, values, _target, update in expanded
                if values != current
            ]
            rollback_results = await asyncio.gather(*rollbacks, return_exceptions=True)
            rollback_failures = sum(
                isinstance(result, BaseException) for result in rollback_results
            )
            if rollback_failures:
                logger.error(
                    "source expansion rollback incomplete failures=%d",
                    rollback_failures,
                )
            cancellation = next(
                (result for result in failures if isinstance(result, asyncio.CancelledError)),
                None,
            )
            if cancellation is not None:
                raise cancellation
            message = "source expansion failed before old subscriptions were removed"
            raise SourceUpdateError(message) from failures[0]
    removals = [
        update(target) for _current, values, target, update in expanded if target != values
    ]
    if removals:
        results = await asyncio.gather(*removals, return_exceptions=True)
        cancellation = next(
            (result for result in results if isinstance(result, asyncio.CancelledError)),
            None,
        )
        if cancellation is not None:
            raise cancellation
        errors = [result for result in results if isinstance(result, Exception)]
        if errors:
            raise SourceUpdateError(
                "source removal incomplete; expanded coverage remains active"
            ) from errors[0]
    return True
