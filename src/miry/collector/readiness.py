from __future__ import annotations

import asyncio


def required_realtime_sources(public_route_count: int) -> frozenset[str]:
    return frozenset(
        {
            *(f"public-{index}" for index in range(public_route_count)),
            "market-0",
            "open_interest",
            "clock",
        }
    )


class SourceReadiness:
    """Track loss-critical realtime recovery separately from universe discovery."""

    def __init__(self, public_route_count: int) -> None:
        self._required_realtime = required_realtime_sources(public_route_count)
        self._seen: set[str] = set()
        self.realtime = asyncio.Event()
        self.discovery = asyncio.Event()

    def mark(self, name: str) -> None:
        self._seen.add(name)
        if name == "discovery":
            self.discovery.set()
        if self._required_realtime <= self._seen:
            self.realtime.set()
