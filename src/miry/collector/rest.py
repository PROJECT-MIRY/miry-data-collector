from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar
from uuid import uuid4

import aiohttp

T = TypeVar("T")


class RecoveryRequestScheduler:
    """Prioritize rate-limited recovery snapshots over background REST work."""

    def __init__(self, *, snapshot_interval_seconds: float, snapshot_max_concurrency: int) -> None:
        self._snapshot_interval_seconds = snapshot_interval_seconds
        self._snapshot_lock = asyncio.Lock()
        self._snapshot_concurrency = asyncio.Semaphore(snapshot_max_concurrency)
        self._last_snapshot_at = 0.0
        self._snapshot_waiters = 0
        self._snapshot_idle = asyncio.Event()
        self._snapshot_idle.set()

    async def run_snapshot(self, request: Callable[[], Awaitable[T]]) -> T:
        self._snapshot_waiters += 1
        self._snapshot_idle.clear()
        try:
            async with self._snapshot_concurrency:
                async with self._snapshot_lock:
                    delay = self._snapshot_interval_seconds - (
                        time.monotonic() - self._last_snapshot_at
                    )
                    if delay > 0:
                        await asyncio.sleep(delay)
                    self._last_snapshot_at = time.monotonic()
                return await request()
        finally:
            self._snapshot_waiters -= 1
            if self._snapshot_waiters == 0:
                self._snapshot_idle.set()

    async def wait_for_snapshot_idle(self) -> None:
        await self._snapshot_idle.wait()


class BinanceRestClient:
    def __init__(
        self,
        base_url: str,
        session: aiohttp.ClientSession,
        *,
        snapshot_interval_seconds: float,
        snapshot_max_concurrency: int = 4,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._session = session
        self._scheduler = RecoveryRequestScheduler(
            snapshot_interval_seconds=snapshot_interval_seconds,
            snapshot_max_concurrency=snapshot_max_concurrency,
        )

    async def fetch(
        self, path: str, *, params: dict[str, str | int] | None = None
    ) -> tuple[bytes, int, int, str]:
        request_id = uuid4().hex
        requested_at = time.time_ns()
        async with self._session.get(
            f"{self._base_url}{path}", params=params, timeout=aiohttp.ClientTimeout(total=20)
        ) as response:
            payload = await response.read()
            observed_at = time.time_ns()
            response.raise_for_status()
        return payload, requested_at, observed_at, request_id

    async def fetch_background(
        self, path: str, *, params: dict[str, str | int] | None = None
    ) -> tuple[bytes, int, int, str]:
        await self._scheduler.wait_for_snapshot_idle()
        return await self.fetch(path, params=params)

    async def fetch_snapshot(self, path: str, *, symbol: str) -> tuple[bytes, int, int, str]:
        return await self._scheduler.run_snapshot(
            lambda: self.fetch(path, params={"symbol": symbol, "limit": 1000})
        )
