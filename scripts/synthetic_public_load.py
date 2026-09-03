from __future__ import annotations

import argparse
import asyncio
import inspect
import math
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import orjson
from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

import miry.collector.websocket as websocket_module
from miry.collector.day_index import DayIndex
from miry.collector.ingest import IngestCoordinator
from miry.collector.queue import ByteBoundedQueues
from miry.collector.routes import RouteRunner
from miry.collector.traffic import PublicTrafficRecorder
from miry.collector.websocket import (
    BinanceWebSocketConnection,
    DecodedWebSocket,
    SourceIdentity,
)
from miry.collector.writer import ChunkLimits, WriterPool
from miry.contracts.models import StreamType
from miry.orderbook.bridge import SnapshotBridgeTracker, UpdateSpan

SYMBOLS = tuple(f"R{index:02d}USDT" for index in range(60))
BIDS = [[f"{100_000 - level}.1", f"{level + 1}.25"] for level in range(20)]
ASKS = [[f"{100_001 + level}.1", f"{level + 1}.75"] for level in range(20)]


def legacy_decode(raw: bytes) -> DecodedWebSocket:
    message = orjson.loads(raw)
    stream = str(message.get("stream", "")).lower()
    data = message.get("data", message)
    if not isinstance(data, dict):
        return DecodedWebSocket(StreamType.UNKNOWN, None, message, None)
    event_type = str(data.get("e", ""))
    symbol = str(data.get("s", "")).upper() or None
    if event_type == "depthUpdate":
        stream_type = StreamType.RPI_DEPTH if "@rpidepth@" in stream else StreamType.DEPTH
        return DecodedWebSocket(stream_type, symbol, message, data)
    mapping = {
        "bookTicker": StreamType.BOOK_TICKER,
        "aggTrade": StreamType.AGG_TRADE,
    }
    return DecodedWebSocket(mapping.get(event_type, StreamType.UNKNOWN), symbol, message, data)


class SyntheticSource:
    def __init__(self, route: int) -> None:
        self.symbols = SYMBOLS[route::4]
        self.sequence = dict.fromkeys(self.symbols, 1)
        self.index = 0

    def next(self) -> bytes:
        symbol = self.symbols[self.index % len(self.symbols)]
        kind = self.index & 3
        self.index += 1
        now_ms = time.time_ns() // 1_000_000
        if kind in (0, 2):
            previous = self.sequence[symbol]
            final = previous + 1
            self.sequence[symbol] = final
            return orjson.dumps(
                {
                    "stream": f"{symbol.lower()}@depth@100ms",
                    "data": {
                        "e": "depthUpdate",
                        "E": now_ms,
                        "T": now_ms,
                        "s": symbol,
                        "U": final,
                        "u": final,
                        "pu": previous,
                        "b": BIDS,
                        "a": ASKS,
                    },
                }
            )
        if kind == 1:
            return orjson.dumps(
                {
                    "stream": f"{symbol.lower()}@bookTicker",
                    "data": {
                        "e": "bookTicker",
                        "E": now_ms,
                        "T": now_ms,
                        "s": symbol,
                        "u": self.index,
                        "b": "100000.1",
                        "B": "1.25",
                        "a": "100001.1",
                        "A": "1.75",
                    },
                }
            )
        return orjson.dumps(
            {
                "stream": f"{symbol.lower()}@aggTrade",
                "data": {
                    "e": "aggTrade",
                    "E": now_ms,
                    "T": now_ms,
                    "s": symbol,
                    "a": self.index,
                    "p": "100000.1",
                    "q": "1.25",
                    "f": self.index,
                    "l": self.index,
                    "m": False,
                },
            }
        )


async def server_main(args: argparse.Namespace) -> None:
    connected = 0
    all_connected = asyncio.Event()
    route_lock = asyncio.Lock()

    async def handler(connection: ServerConnection) -> None:
        nonlocal connected
        async with route_lock:
            route = connected
            connected += 1
            if connected == 4:
                all_connected.set()
        request = orjson.loads(await connection.recv(decode=False))
        await connection.send(orjson.dumps({"result": None, "id": request["id"]}))
        await all_connected.wait()
        source = SyntheticSource(route)
        per_second = args.rate / 60 / 4
        tick_seconds = 0.02
        carry = 0.0
        deadline = time.monotonic() + tick_seconds
        stop_at = time.monotonic() + args.duration
        while time.monotonic() < stop_at:
            await asyncio.sleep(max(0.0, deadline - time.monotonic()))
            carry += per_second * tick_seconds
            count = math.floor(carry)
            carry -= count
            for _ in range(count):
                await connection.send(source.next())
            deadline += tick_seconds

    async with serve(
        handler,
        "127.0.0.1",
        args.port,
        compression="deflate",
        max_queue=16,
        max_size=2 * 1024**2,
    ):
        print("ready", flush=True)
        await all_connected.wait()
        await asyncio.sleep(args.duration + 5)


async def monitor(stop: asyncio.Event, lag: list[float], cpu: list[float]) -> None:
    interval = 0.05
    deadline = time.monotonic() + interval
    previous_wall = time.monotonic()
    previous_cpu = time.process_time()
    while not stop.is_set():
        await asyncio.sleep(max(0.0, deadline - time.monotonic()))
        observed = time.monotonic()
        current_cpu = time.process_time()
        lag.append(max(0.0, observed - deadline))
        wall_delta = observed - previous_wall
        cpu.append((current_cpu - previous_cpu) / wall_delta if wall_delta else 0.0)
        previous_wall = observed
        previous_cpu = current_cpu
        deadline += interval


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1)]


async def client_main(args: argparse.Namespace) -> None:
    if args.decoder == "legacy":
        websocket_module.decode_websocket = legacy_decode
    with tempfile.TemporaryDirectory(prefix="miry-synthetic-load-") as temporary:
        data_root = Path(temporary)
        queues = ByteBoundedQueues(192 * 1024**2, warn_ratio=0.70, resume_ratio=0.50)
        writers = WriterPool(
            data_root,
            collector_id="replay01",
            data_contract_hash="1" * 64,
            universe_hash="2" * 64,
            queues=queues,
            day_index=DayIndex(data_root, "replay01"),
            limits=ChunkLimits(300, 256 * 1024**2, 1_000_000, 8_000, 8 * 1024**2),
        )
        ingest = IngestCoordinator(queues, writers)
        writers.start()
        stop = asyncio.Event()
        lag_samples: list[float] = []
        cpu_samples: list[float] = []
        monitor_task = asyncio.create_task(monitor(stop, lag_samples, cpu_samples))
        received = 0
        gaps: list[tuple[Any, ...]] = []
        traffic = PublicTrafficRecorder(
            data_root / "control/public-message-rates.json",
            {},
        )

        async def run_route(route: int) -> None:
            nonlocal received
            identity = SourceIdentity("replay01", "boot0001", "segment1", f"public-{route}")

            runner = RouteRunner(
                name=f"public-{route}",
                url=f"ws://127.0.0.1:{args.port}",
                subscriptions=("replay",),
                instruments=SYMBOLS[route::4],
                stream_types=(StreamType.BOOK_TICKER, StreamType.DEPTH),
                collector_id="replay01",
                boot_id="boot0001",
                ingest=ingest,
                queues=queues,
                gaps=SimpleNamespace(),
                rest=SimpleNamespace(),
                rotation_seconds=82_800,
                overlap_seconds=15,
                receive_timeout_seconds=30,
                ping_interval_seconds=20,
                ping_timeout_seconds=20,
                service_stop=asyncio.Event(),
                on_message=traffic.record,
                liveness_timeout_seconds=30,
                liveness_stream_types=(StreamType.BOOK_TICKER, StreamType.DEPTH),
            )

            def count_event_with_timestamps(
                stream: StreamType,
                symbol: str | None,
                realtime_ns: int,
                monotonic_ns: int,
            ) -> None:
                nonlocal received
                received += 1
                runner._mark_event(stream, symbol, realtime_ns, monotonic_ns)

            def count_event_legacy(stream: StreamType, symbol: str | None) -> None:
                nonlocal received
                received += 1
                runner._mark_event(stream, symbol)

            observer: Any = (
                count_event_with_timestamps
                if len(inspect.signature(runner._mark_event).parameters) == 4
                else count_event_legacy
            )

            connection = BinanceWebSocketConnection(
                url=f"ws://127.0.0.1:{args.port}",
                subscriptions=("replay",),
                identity=identity,
                ingest=ingest,
                snapshot_requests=(),
                rest=SimpleNamespace(),
                ready=asyncio.Event(),
                stop=asyncio.Event(),
                receive_timeout_seconds=30,
                ping_interval_seconds=20,
                ping_timeout_seconds=20,
                max_queue=16,
                max_message_bytes=2 * 1024**2,
                updates=asyncio.Queue(),
                on_depth_gap=lambda *values: gaps.append(values) or asyncio.sleep(0, result="gap"),
                on_depth_reanchored=lambda *values: asyncio.sleep(0),
                on_event=observer,
            )
            for symbol in SYMBOLS[route::4]:
                tracker = SnapshotBridgeTracker()
                tracker.on_diff(UpdateSpan(0, 1, 1, 0))
                tracker.on_snapshot(1)
                connection._bridges[(StreamType.DEPTH, symbol)] = tracker
            try:
                async with connect(
                    f"ws://127.0.0.1:{args.port}",
                    compression="deflate",
                    max_queue=16,
                    max_size=2 * 1024**2,
                ) as ws:
                    await ws.send(
                        orjson.dumps(
                            {
                                "method": "SUBSCRIBE",
                                "params": ["replay"],
                                "id": route + 1,
                            }
                        )
                    )
                    control = SimpleNamespace(deliver=lambda *_values: None)
                    await connection._receive_loop(ws, control)
            except ConnectionClosed:
                return

        wall_started = time.monotonic()
        cpu_started = time.process_time()
        await asyncio.gather(*(run_route(route) for route in range(4)))
        receive_elapsed = time.monotonic() - wall_started
        await writers.stop()
        total_elapsed = time.monotonic() - wall_started
        total_cpu = time.process_time() - cpu_started
        stop.set()
        await monitor_task
        expected = args.rate / 60 * args.duration
        result = {
            "decoder": args.decoder,
            "target_rate_per_min": args.rate,
            "expected": round(expected),
            "received": received,
            "delivery_ratio": received / expected,
            "receive_elapsed_s": receive_elapsed,
            "total_elapsed_s": total_elapsed,
            "cpu_average_cores": total_cpu / total_elapsed,
            "cpu_p95_cores": percentile(cpu_samples, 0.95),
            "lag_p99_ms": percentile(lag_samples, 0.99) * 1000,
            "lag_max_ms": max(lag_samples) * 1000,
            "queue_high_water_ratio": queues.high_water_bytes / queues.max_bytes,
            "hard_rejections": queues.hard_rejections,
            "l2_sequence_gaps": len(gaps),
            "chunks": writers.metrics.chunks,
        }
        print(orjson.dumps(result, option=orjson.OPT_INDENT_2).decode())
        passed = (
            result["delivery_ratio"] >= 0.99
            and result["cpu_p95_cores"] < 0.80
            and result["lag_p99_ms"] < 100
            and result["hard_rejections"] == 0
            and result["l2_sequence_gaps"] == 0
        )
        if not passed:
            raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="mode", required=True)
    for mode in ("server", "client"):
        child = subparsers.add_parser(mode)
        child.add_argument("--rate", type=int, required=True)
        child.add_argument("--duration", type=float, default=10)
        child.add_argument("--port", type=int, default=18765)
        if mode == "client":
            child.add_argument("--decoder", choices=("legacy", "typed"), required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(server_main(args) if args.mode == "server" else client_main(args))


if __name__ == "__main__":
    main()
