from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import orjson

from ft_shadow_data_plane.contracts.models import SYMBOL_PATTERN
from ft_shadow_data_plane.contracts.serde import atomic_write_bytes, canonical_json_bytes

logger = logging.getLogger(__name__)

MINUTES_PER_BLOCK = 60
OBSERVATION_BLOCKS = 24
STATE_VERSION = 1


@dataclass(frozen=True, slots=True)
class MessageRateBlock:
    start_minute: int
    end_minute: int
    rates: dict[str, int]


class PublicTrafficRecorder:
    """Persist bounded public-stream rates for the next process start."""

    def __init__(
        self,
        path: Path,
        baseline_rates: dict[str, int],
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._path = path
        self._baseline_rates = dict(baseline_rates)
        self._clock = clock
        self._started_minute = int(clock() // 60)
        self._last_minute: int | None = None
        self._minute_symbols: dict[int, Counter[str]] = {}
        self._minute_routes: dict[int, Counter[str]] = {}
        self._block_start: int | None = None
        self._block_minutes = 0
        self._block_peaks: Counter[str] = Counter()
        self._blocks = self._load()

    def record(self, route: str, symbol: str) -> None:
        minute = int(self._clock() // 60)
        self._minute_symbols.setdefault(minute, Counter())[symbol] += 1
        self._minute_routes.setdefault(minute, Counter())[route] += 1

    def effective_rates(self) -> dict[str, int]:
        if len(self._blocks) < OBSERVATION_BLOCKS:
            return dict(self._baseline_rates)
        rates: dict[str, int] = {}
        for block in self._blocks:
            for symbol, rate in block.rates.items():
                rates[symbol] = max(rates.get(symbol, 0), rate)
        return rates or dict(self._baseline_rates)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            now = self._clock()
            next_minute = (int(now // 60) + 1) * 60
            try:
                async with asyncio.timeout(max(0.01, next_minute - now)):
                    await stop.wait()
            except TimeoutError:
                finished_minute = int(self._clock() // 60) - 1
                if self.finish_minute(finished_minute):
                    await self._persist()

    def finish_minute(self, minute: int) -> bool:
        symbol_counts = self._minute_symbols.pop(minute, Counter())
        route_counts = self._minute_routes.pop(minute, Counter())
        self._discard_older_minutes(minute)
        logger.info(
            "public traffic minute minute=%d route_rates=%s",
            minute,
            dict(sorted(route_counts.items())),
        )

        if minute <= self._started_minute:
            return False
        if self._last_minute is not None and minute != self._last_minute + 1:
            self._reset_block()
        self._last_minute = minute
        if self._block_start is None:
            self._block_start = minute
        self._block_minutes += 1
        for symbol, rate in symbol_counts.items():
            self._block_peaks[symbol] = max(self._block_peaks[symbol], rate)
        if self._block_minutes < MINUTES_PER_BLOCK:
            return False

        self._blocks.append(
            MessageRateBlock(
                start_minute=self._block_start,
                end_minute=minute,
                rates=dict(sorted(self._block_peaks.items())),
            )
        )
        self._blocks = self._blocks[-OBSERVATION_BLOCKS:]
        self._reset_block()
        return True

    @property
    def blocks(self) -> tuple[MessageRateBlock, ...]:
        return tuple(self._blocks)

    def _reset_block(self) -> None:
        self._block_start = None
        self._block_minutes = 0
        self._block_peaks.clear()

    def _discard_older_minutes(self, minute: int) -> None:
        self._minute_symbols = {
            key: value for key, value in self._minute_symbols.items() if key > minute
        }
        self._minute_routes = {
            key: value for key, value in self._minute_routes.items() if key > minute
        }

    def _load(self) -> list[MessageRateBlock]:
        if not self._path.exists():
            return []
        try:
            payload = orjson.loads(self._path.read_bytes())
            return _parse_blocks(payload)[-OBSERVATION_BLOCKS:]
        except (OSError, orjson.JSONDecodeError, TypeError, ValueError):
            logger.warning(
                "ignoring invalid public traffic state path=%s",
                self._path,
                exc_info=True,
            )
            return []

    def _save(self) -> None:
        payload = {
            "version": STATE_VERSION,
            "blocks": [
                {
                    "start_minute": block.start_minute,
                    "end_minute": block.end_minute,
                    "rates": block.rates,
                }
                for block in self._blocks
            ],
        }
        atomic_write_bytes(self._path, canonical_json_bytes(payload))

    async def _persist(self) -> None:
        try:
            await asyncio.to_thread(self._save)
        except OSError:
            logger.warning(
                "public traffic state write failed path=%s",
                self._path,
                exc_info=True,
            )


def _parse_blocks(payload: Any) -> list[MessageRateBlock]:
    if not isinstance(payload, dict) or payload.get("version") != STATE_VERSION:
        raise ValueError("unsupported public traffic state")
    raw_blocks = payload.get("blocks")
    if not isinstance(raw_blocks, list):
        raise ValueError("public traffic blocks must be a list")
    blocks: list[MessageRateBlock] = []
    previous_end: int | None = None
    for value in raw_blocks:
        if not isinstance(value, dict):
            raise ValueError("public traffic block must be an object")
        start = value.get("start_minute")
        end = value.get("end_minute")
        raw_rates = value.get("rates")
        if not isinstance(start, int) or not isinstance(end, int):
            raise ValueError("public traffic block minutes must be integers")
        if end - start != MINUTES_PER_BLOCK - 1:
            raise ValueError("public traffic block must contain 60 minutes")
        if previous_end is not None and start <= previous_end:
            raise ValueError("public traffic blocks must be ordered")
        if not isinstance(raw_rates, dict):
            raise ValueError("public traffic rates must be an object")
        rates: dict[str, int] = {}
        for symbol, rate in raw_rates.items():
            if (
                not isinstance(symbol, str)
                or not SYMBOL_PATTERN.fullmatch(symbol)
                or not isinstance(rate, int)
                or isinstance(rate, bool)
                or rate <= 0
            ):
                raise ValueError("public traffic state contains an invalid rate")
            rates[symbol] = rate
        blocks.append(MessageRateBlock(start, end, rates))
        previous_end = end
    return blocks
