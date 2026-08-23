from __future__ import annotations

import asyncio
import gzip
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import aiohttp
import orjson

from miry.collector.config import CollectorConfig
from miry.collector.gaps import GapJournal
from miry.collector.ingest import IngestCoordinator
from miry.collector.queue import ByteBoundedQueues, QueueOverloaded
from miry.collector.rest import BinanceRestClient
from miry.collector.scheduling import advance_fixed_deadline, staggered_offsets
from miry.collector.websocket import (
    SourceIdentity,
)
from miry.contracts.models import GapReason, StreamType
from miry.contracts.serde import canonical_json_bytes, sha256_bytes
from miry.contracts.symbols import is_exchange_symbol
from miry.universe.models import DiscoverySnapshot
from miry.universe.selection import liquidity_validation_symbols

logger = logging.getLogger(__name__)


class RestPollers:
    def __init__(
        self,
        *,
        config: CollectorConfig,
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
            startup_window = min(
                float(interval), self._config.open_interest_startup_spread_seconds
            )
            startup_offsets = staggered_offsets(len(added), startup_window)
            for index, symbol in enumerate(added):
                first_pass = asyncio.get_running_loop().create_future()
                self._oi_first_pass[symbol] = first_pass
                self._oi_tasks[symbol] = asyncio.create_task(
                    self._open_interest_symbol_loop(
                        symbol,
                        first_pass,
                        initial_delay=startup_offsets[index],
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
                next_poll = advance_fixed_deadline(next_poll, interval, time.monotonic())

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
                    tracked=tuple(sorted(self._instruments)),
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
        evidence_days = max(
            self._config.universe.liquidity_window_days,
            self._config.universe.market_context_baseline_days + 7,
        )
        start_ms = cutoff_ms - evidence_days * 86_400_000
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
                    -(evidence_days + 1) :
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
                await _wait_event(self._stop, 1)
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
            and is_exchange_symbol(symbol)
        ):
            try:
                onboard_ms = int(str(item["onboardDate"]))
            except (KeyError, ValueError) as exc:
                raise ValueError(f"invalid onboardDate for {symbol}") from exc
            if onboard_ms <= 0:
                raise ValueError(f"invalid onboardDate for {symbol}")
            symbols[symbol] = onboard_ms
    return dict(sorted(symbols.items()))


async def _wait_event(event: asyncio.Event, delay_seconds: float) -> None:
    try:
        await asyncio.wait_for(event.wait(), timeout=delay_seconds)
    except TimeoutError:
        pass
