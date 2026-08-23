from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import orjson
from websockets.asyncio.client import ClientConnection

from miry.contracts.models import StreamType

logger = logging.getLogger(__name__)

SnapshotRecovery = Callable[
    [tuple[tuple[str, StreamType], ...], asyncio.Future[None]], Awaitable[None]
]


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


@dataclass(frozen=True, slots=True)
class ControlResponse:
    request_id: int
    message: dict[str, Any]
    observed_at_realtime_ns: int


@dataclass(frozen=True, slots=True)
class ControlRequest:
    request_id: int
    future: asyncio.Future[ControlResponse]


class ControlRequests:
    def __init__(self, initial_id: int) -> None:
        self._next_id = initial_id - 1
        self._pending: dict[int, asyncio.Future[ControlResponse]] = {}

    async def request(
        self,
        websocket: ClientConnection,
        method: str,
        *,
        params: tuple[str, ...] | None = None,
        timeout_seconds: float | None = None,
    ) -> ControlResponse:
        pending = await self.send(websocket, method, params=params)
        return await self.wait(pending, timeout_seconds=timeout_seconds)

    async def send(
        self,
        websocket: ClientConnection,
        method: str,
        *,
        params: tuple[str, ...] | None = None,
    ) -> ControlRequest:
        self._next_id += 1
        request_id = self._next_id
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        payload: dict[str, object] = {"method": method, "id": request_id}
        if params is not None:
            payload["params"] = list(params)
        try:
            await websocket.send(orjson.dumps(payload).decode())
        except BaseException:
            self._pending.pop(request_id, None)
            future.cancel()
            raise
        return ControlRequest(request_id, future)

    async def wait(
        self,
        pending: ControlRequest,
        *,
        timeout_seconds: float | None = None,
    ) -> ControlResponse:
        try:
            if timeout_seconds is None:
                return await pending.future
            async with asyncio.timeout(timeout_seconds):
                return await pending.future
        finally:
            self._pending.pop(pending.request_id, None)

    def deliver(self, message: dict[str, Any], observed_at_realtime_ns: int) -> None:
        request_id = message.get("id")
        if not isinstance(request_id, int):
            return
        future = self._pending.get(request_id)
        if future is not None and not future.done():
            future.set_result(ControlResponse(request_id, message, observed_at_realtime_ns))

    def cancel(self) -> None:
        for future in self._pending.values():
            if not future.done():
                future.cancel()
        self._pending.clear()


class SubscriptionController:
    def __init__(
        self,
        *,
        websocket: ClientConnection,
        subscriptions: tuple[str, ...],
        updates: asyncio.Queue[SubscriptionUpdate],
        connection_id: str,
        peer: str,
        audit_seconds: float,
        audit_timeout_seconds: float,
        audit_failures_before_reconnect: int,
        recover_snapshots: SnapshotRecovery,
    ) -> None:
        initial_id = int.from_bytes(uuid4().bytes[:4], "big")
        self._websocket = websocket
        self._subscriptions = subscriptions
        self._updates = updates
        self._connection_id = connection_id
        self._peer = peer
        self._audit_seconds = audit_seconds
        self._audit_timeout_seconds = audit_timeout_seconds
        self._audit_failures_before_reconnect = audit_failures_before_reconnect
        self._recover_snapshots = recover_snapshots
        self._requests = ControlRequests(initial_id)
        self._lock = asyncio.Lock()
        self._active = set(subscriptions)
        self._subscription_proven_realtime_ns: int | None = None

    async def send_initial(self) -> ControlRequest:
        return await self._requests.send(
            self._websocket, "SUBSCRIBE", params=self._subscriptions
        )

    async def complete_initial(self, request: ControlRequest, *, started: float) -> None:
        response = await self._requests.wait(request)
        if not _is_subscription_ack(response.message, response.request_id):
            raise OSError(f"Binance subscription rejected: {response.message}")
        self._subscription_proven_realtime_ns = response.observed_at_realtime_ns
        logger.info(
            "subscription ready connection_id=%s peer=%s rtt_ms=%.3f",
            self._connection_id,
            self._peer,
            (time.monotonic() - started) * 1_000,
        )

    def deliver(self, message: dict[str, Any], observed_at_realtime_ns: int) -> None:
        if message.get("code") is not None:
            raise OSError(f"Binance subscription rejected: {message}")
        self._requests.deliver(message, observed_at_realtime_ns)

    async def run_updates(self) -> None:
        while True:
            update = await self._updates.get()
            if update.acknowledged.cancelled() or update.completion.cancelled():
                continue
            try:
                await self._apply_update(update)
            except BaseException as error:
                _fail_subscription_update(update, error)
                raise

    async def _apply_update(self, update: SubscriptionUpdate) -> None:
        requests = []
        async with self._lock:
            for method, streams in (
                ("UNSUBSCRIBE", update.remove),
                ("SUBSCRIBE", update.add),
            ):
                if streams:
                    requests.append(
                        self._requests.request(self._websocket, method, params=streams)
                    )
            if requests:
                await asyncio.gather(*requests)
            self._active.difference_update(update.remove)
            self._active.update(update.add)
        if not update.acknowledged.done():
            update.acknowledged.set_result(None)
        if update.snapshot_requests:
            await self._recover_snapshots(update.snapshot_requests, update.completion)
        elif not update.completion.done():
            update.completion.set_result(None)

    async def run_audits(self) -> None:
        failures = 0
        await asyncio.sleep(self._audit_seconds)
        while True:
            started = time.monotonic()
            try:
                async with self._lock:
                    response = await self._requests.request(
                        self._websocket,
                        "LIST_SUBSCRIPTIONS",
                        timeout_seconds=self._audit_timeout_seconds,
                    )
            except TimeoutError as error:
                failures += 1
                if failures >= self._audit_failures_before_reconnect:
                    raise SubscriptionAuditError(
                        "subscription audit response was not received within "
                        f"{self._audit_timeout_seconds:g}s "
                        f"for {failures} consecutive attempts",
                        affected_from_realtime_ns=(
                            self._subscription_proven_realtime_ns or time.time_ns()
                        ),
                    ) from error
                logger.warning(
                    "subscription audit response missed connection_id=%s "
                    "failures=%d threshold=%d; retrying",
                    self._connection_id,
                    failures,
                    self._audit_failures_before_reconnect,
                )
                continue
            self._validate_audit(response)
            failures = 0
            self._subscription_proven_realtime_ns = response.observed_at_realtime_ns
            logger.info(
                "subscription audit connection_id=%s peer=%s rtt_ms=%.3f "
                "ping_rtt_ms=%s subscriptions=%d",
                self._connection_id,
                self._peer,
                (time.monotonic() - started) * 1_000,
                _latency_ms(getattr(self._websocket, "latency", None)),
                len(self._active),
            )
            await asyncio.sleep(self._audit_seconds)

    def _validate_audit(self, response: ControlResponse) -> None:
        result = response.message.get("result")
        actual = (
            set(result)
            if isinstance(result, list) and all(isinstance(value, str) for value in result)
            else set()
        )
        if actual == self._active:
            return
        missing = sorted(self._active - actual)
        unexpected = sorted(actual - self._active)
        raise SubscriptionAuditError(
            f"subscription audit mismatch missing={missing} unexpected={unexpected}",
            affected_from_realtime_ns=(
                self._subscription_proven_realtime_ns or response.observed_at_realtime_ns
            ),
        )

    def close(self) -> None:
        self._requests.cancel()


def _fail_subscription_update(update: SubscriptionUpdate, error: BaseException) -> None:
    if update.acknowledged.cancelled():
        if not update.completion.done():
            update.completion.cancel()
    elif not update.acknowledged.done():
        update.acknowledged.set_exception(error)
        if not update.completion.done():
            update.completion.cancel()
    elif not update.completion.done():
        update.completion.set_exception(error)


def _is_subscription_ack(value: dict[str, Any], expected_id: int) -> bool:
    return value.get("id") == expected_id and value.get("result") is None


def _latency_ms(value: object) -> str:
    return f"{value * 1_000:.3f}" if isinstance(value, int | float) else "unknown"
