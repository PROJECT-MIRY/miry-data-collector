from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import orjson
import pytest

from miry.collector.routes import RouteRunner
from miry.collector.websocket import BinanceWebSocketConnection
from miry.collector.ws_control import SubscriptionController, SubscriptionUpdate
from miry.contracts.models import GapReason, StreamType


class GapLedger:
    def __init__(self) -> None:
        self.opened: list[dict[str, object]] = []
        self.closed: list[str] = []

    async def open(self, reason: GapReason, **fields: object) -> str:
        self.opened.append({"reason": reason, **fields})
        return f"gap-{len(self.opened)}"

    async def close(self, gap_id: str, reason: GapReason, **fields: object) -> None:
        self.closed.append(gap_id)


class ObservedQueue(asyncio.Queue[SubscriptionUpdate]):
    def __init__(self) -> None:
        super().__init__(maxsize=1)
        self.enqueued = asyncio.Event()
        self.consumed = asyncio.Event()

    def put_nowait(self, item: SubscriptionUpdate) -> None:
        super().put_nowait(item)
        self.enqueued.set()

    async def get(self) -> SubscriptionUpdate:
        item = await super().get()
        self.consumed.set()
        return item


def make_runner(ledger: GapLedger) -> RouteRunner:
    return RouteRunner(
        name="public-1",
        url="wss://example.invalid",
        subscriptions=("btcusdt@depth@100ms",),
        instruments=("BTCUSDT",),
        stream_types=(StreamType.DEPTH,),
        collector_id="tokyo01",
        boot_id="boot-test",
        ingest=SimpleNamespace(),  # type: ignore[arg-type]
        queues=SimpleNamespace(),  # type: ignore[arg-type]
        gaps=ledger,  # type: ignore[arg-type]
        rest=SimpleNamespace(),  # type: ignore[arg-type]
        rotation_seconds=82_800,
        overlap_seconds=15,
        receive_timeout_seconds=30,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        service_stop=asyncio.Event(),
        subscriptions_for=lambda symbols: tuple(f"{s.lower()}@depth@100ms" for s in symbols),
        liveness_timeout_seconds=30,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["lock", "ack", "snapshot"])
@pytest.mark.parametrize("outcome", ["fresh", "removed", "silent"])
async def test_connection_cancel_does_not_kill_liveness(
    monkeypatch: pytest.MonkeyPatch, phase: str, outcome: str
) -> None:
    ledger = GapLedger()
    runner = make_runner(ledger)
    updates = ObservedQueue()
    runner._updates = updates
    runner._last_event[(StreamType.DEPTH, "BTCUSDT")] = (time.monotonic() - 60, 1)
    phase_reached = asyncio.Event()
    monitor_resumed = asyncio.Event()
    finish = asyncio.Event()
    checks = 0

    async def next_check(_seconds: float) -> None:
        nonlocal checks
        checks += 1
        if checks == 1:
            return
        if checks == 2:
            # A new connection has replaced the cancelled one.
            if outcome == "fresh":
                runner._mark_event(StreamType.DEPTH, "BTCUSDT")
            elif outcome == "removed":
                runner._last_event.clear()
                runner._instruments = ()
            monitor_resumed.set()
            if outcome == "silent":
                await finish.wait()
                runner._service_stop.set()
            return
        await finish.wait()
        runner._service_stop.set()

    monkeypatch.setattr(runner, "_wait_or_stop", next_check)

    class Socket:
        async def send(self, value: str) -> None:
            request = orjson.loads(value)
            if phase == "ack":
                phase_reached.set()
            else:
                controller.deliver({"id": request["id"], "result": None}, time.time_ns())

    async def blocked_bridge(_symbol: str, _stream: StreamType) -> None:
        phase_reached.set()
        await asyncio.Future()

    # Exercise the real snapshot callback, including its cancellation path.
    connection = object.__new__(BinanceWebSocketConnection)
    connection._snapshot_pending = set()
    monkeypatch.setattr(connection, "_recover_snapshot_bridge", blocked_bridge)
    controller = SubscriptionController(
        websocket=Socket(),  # type: ignore[arg-type]
        subscriptions=runner._subscriptions,
        updates=runner._updates,
        connection_id="old-connection",
        peer="test",
        audit_seconds=60,
        audit_timeout_seconds=20,
        audit_failures_before_reconnect=3,
        recover_snapshots=connection._fetch_requested_snapshots,
    )
    if phase == "lock":
        await controller._lock.acquire()

    monitor = asyncio.create_task(runner.liveness_loop())
    worker = asyncio.create_task(controller.run_updates())
    resumed = asyncio.create_task(monitor_resumed.wait())
    try:
        if phase == "lock":
            await asyncio.wait_for(updates.consumed.wait(), timeout=1)
        else:
            await asyncio.wait_for(phase_reached.wait(), timeout=1)
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        done, _ = await asyncio.wait(
            (monitor, resumed), timeout=1, return_when=asyncio.FIRST_COMPLETED
        )
        assert resumed in done, "connection cancellation killed the liveness monitor"
        assert not monitor.done()
        await asyncio.sleep(0)
        assert ledger.closed == ([] if outcome == "silent" else ["gap-1"])
        finish.set()
        await asyncio.wait_for(monitor, timeout=1)
    finally:
        for task in (monitor, worker, resumed):
            task.cancel()
        await asyncio.gather(monitor, worker, resumed, return_exceptions=True)
        controller.close()


@pytest.mark.asyncio
async def test_service_cancellation_is_not_converted_to_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = GapLedger()
    runner = make_runner(ledger)
    runner._last_event[(StreamType.DEPTH, "BTCUSDT")] = (time.monotonic() - 60, 1)

    async def no_wait(_seconds: float) -> None:
        return

    monkeypatch.setattr(runner, "_wait_or_stop", no_wait)
    monitor = asyncio.create_task(runner.liveness_loop())
    update = await asyncio.wait_for(runner._updates.get(), timeout=1)
    monitor.cancel()
    with pytest.raises(asyncio.CancelledError):
        await monitor
    assert ledger.closed == []
    assert all(f.cancelled() for f in (update.started, update.acknowledged, update.completion))


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["started", "acknowledged", "completion"])
async def test_cancelled_shared_future_is_a_recoverable_update_failure(phase: str) -> None:
    runner = make_runner(GapLedger())
    task = asyncio.create_task(runner._submit_update(add=(), remove=(), snapshot_requests=()))
    update = await asyncio.wait_for(runner._updates.get(), timeout=1)
    futures = (update.started, update.acknowledged, update.completion)
    for name, future in zip(("started", "acknowledged", "completion"), futures, strict=True):
        if name == phase:
            future.cancel()
            break
        future.set_result(None)
    with pytest.raises(ConnectionError, match="interrupted by connection shutdown"):
        await task
    assert all(f.done() for f in futures)


@pytest.mark.asyncio
async def test_full_subscription_queue_has_a_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = make_runner(GapLedger())
    updates = ObservedQueue()
    runner._updates = updates
    # Occupy the only queue slot without a controller consuming it.
    first = asyncio.create_task(runner._submit_update(add=(), remove=(), snapshot_requests=()))
    await asyncio.wait_for(updates.enqueued.wait(), timeout=1)
    timeout = asyncio.timeout
    monkeypatch.setattr("miry.collector.routes.asyncio.timeout", lambda _: timeout(0.01))
    second = asyncio.create_task(runner._submit_update(add=(), remove=(), snapshot_requests=()))
    try:
        done, _ = await asyncio.wait((second,), timeout=0.5)
        assert second in done, "queue admission has no deadline"
        with pytest.raises(TimeoutError):
            await second
        assert runner._updates.qsize() == 1
    finally:
        for task in (first, second):
            task.cancel()
        await asyncio.gather(first, second, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("fails", [False, True])
async def test_controller_completes_update_only_after_snapshot_recovery(
    monkeypatch: pytest.MonkeyPatch, fails: bool
) -> None:
    runner = make_runner(GapLedger())
    recovering = asyncio.Event()
    release = asyncio.Event()

    async def bridge(_symbol: str, _stream: StreamType) -> None:
        recovering.set()
        await release.wait()
        if fails:
            raise OSError("snapshot recovery failed")

    connection = object.__new__(BinanceWebSocketConnection)
    connection._snapshot_pending = set()
    monkeypatch.setattr(connection, "_recover_snapshot_bridge", bridge)
    controller = SubscriptionController(
        websocket=SimpleNamespace(),  # type: ignore[arg-type]
        subscriptions=(),
        updates=runner._updates,
        connection_id="test-connection",
        peer="test",
        audit_seconds=60,
        audit_timeout_seconds=20,
        audit_failures_before_reconnect=3,
        recover_snapshots=connection._fetch_requested_snapshots,
    )
    caller = asyncio.create_task(
        runner._submit_update(
            add=(), remove=(), snapshot_requests=(("BTCUSDT", StreamType.DEPTH_SNAPSHOT),)
        )
    )
    worker = asyncio.create_task(controller.run_updates())
    try:
        await asyncio.wait_for(recovering.wait(), timeout=1)
        assert not caller.done(), "subscription completed before snapshot recovery"
        release.set()
        if fails:
            with pytest.raises(OSError, match="snapshot recovery failed"):
                await asyncio.wait_for(caller, timeout=1)
            with pytest.raises(OSError, match="snapshot recovery failed"):
                await worker
        else:
            await asyncio.wait_for(caller, timeout=1)
            assert not worker.done()
    finally:
        for task in (caller, worker):
            task.cancel()
        await asyncio.gather(caller, worker, return_exceptions=True)
        controller.close()
