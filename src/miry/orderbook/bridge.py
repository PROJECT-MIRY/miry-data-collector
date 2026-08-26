from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class BridgeStatus(StrEnum):
    WAITING_FOR_EVENTS = "WAITING_FOR_EVENTS"
    STALE_SNAPSHOT = "STALE_SNAPSHOT"
    BRIDGED = "BRIDGED"
    CONTIGUOUS = "CONTIGUOUS"
    SEQUENCE_GAP = "SEQUENCE_GAP"


@dataclass(frozen=True, slots=True)
class UpdateSpan:
    receive_seq: int
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int


@dataclass(frozen=True, slots=True)
class BridgeResult:
    status: BridgeStatus
    bridge_index: int | None = None
    discard_count: int = 0
    expected_previous_update_id: int | None = None
    received_previous_update_id: int | None = None


def sequence_continues(expected_previous_update_id: int, received_previous_update_id: int) -> bool:
    return received_previous_update_id == expected_previous_update_id


def locate_snapshot_bridge(last_update_id: int, spans: tuple[UpdateSpan, ...]) -> BridgeResult:
    """Apply Binance's official first-event overlap rule to an ordered diff buffer."""
    discard_count = next(
        (index for index, span in enumerate(spans) if span.final_update_id >= last_update_id),
        len(spans),
    )
    if discard_count == len(spans):
        return BridgeResult(
            BridgeStatus.WAITING_FOR_EVENTS,
            discard_count=discard_count,
        )
    candidate = spans[discard_count]
    if candidate.first_update_id > last_update_id:
        return BridgeResult(
            BridgeStatus.STALE_SNAPSHOT,
            discard_count=discard_count,
        )
    return BridgeResult(
        BridgeStatus.BRIDGED,
        bridge_index=discard_count,
        discard_count=discard_count,
    )


class SnapshotBridgeTracker:
    def __init__(self, *, maximum_buffered_events: int = 4096) -> None:
        if maximum_buffered_events < 1:
            raise ValueError("maximum buffered depth events must be positive")
        self._maximum_buffered_events = maximum_buffered_events
        self._pending: list[UpdateSpan] = []
        self._snapshot_last_update_id: int | None = None
        self._previous_update_id: int | None = None

    @property
    def has_events(self) -> bool:
        return bool(self._pending)

    @property
    def is_bridged(self) -> bool:
        return self._previous_update_id is not None

    @property
    def snapshot_last_update_id(self) -> int | None:
        return self._snapshot_last_update_id

    @property
    def previous_update_id(self) -> int | None:
        return self._previous_update_id

    def on_snapshot(self, last_update_id: int) -> BridgeResult:
        self._snapshot_last_update_id = last_update_id
        self._previous_update_id = None
        return self._evaluate_snapshot()

    def on_diff(self, span: UpdateSpan) -> BridgeResult:
        if self._previous_update_id is not None:
            expected = self._previous_update_id
            if not sequence_continues(expected, span.previous_final_update_id):
                self._previous_update_id = None
                self._snapshot_last_update_id = None
                self._pending = [span]
                return BridgeResult(
                    BridgeStatus.SEQUENCE_GAP,
                    expected_previous_update_id=expected,
                    received_previous_update_id=span.previous_final_update_id,
                )
            self._previous_update_id = span.final_update_id
            return BridgeResult(BridgeStatus.CONTIGUOUS)

        self._pending.append(span)
        if len(self._pending) > self._maximum_buffered_events:
            del self._pending[: len(self._pending) - self._maximum_buffered_events]
        return self._evaluate_snapshot()

    def invalidate(self) -> None:
        self._pending.clear()
        self._snapshot_last_update_id = None
        self._previous_update_id = None

    def _evaluate_snapshot(self) -> BridgeResult:
        if self._snapshot_last_update_id is None:
            return BridgeResult(BridgeStatus.WAITING_FOR_EVENTS)
        if len(self._pending) > 1 and any(
            previous.receive_seq > current.receive_seq
            for previous, current in zip(self._pending, self._pending[1:], strict=False)
        ):
            self._pending.sort(key=lambda span: span.receive_seq)
        decision = locate_snapshot_bridge(self._snapshot_last_update_id, tuple(self._pending))
        if decision.discard_count:
            del self._pending[: decision.discard_count]
        if decision.status is BridgeStatus.WAITING_FOR_EVENTS:
            return decision
        if decision.status is BridgeStatus.STALE_SNAPSHOT:
            self._snapshot_last_update_id = None
            return decision

        previous = self._pending[0].final_update_id
        for index, span in enumerate(self._pending[1:], start=1):
            if not sequence_continues(previous, span.previous_final_update_id):
                self._snapshot_last_update_id = None
                self._pending = self._pending[index:]
                return BridgeResult(
                    BridgeStatus.SEQUENCE_GAP,
                    bridge_index=0,
                    expected_previous_update_id=previous,
                    received_previous_update_id=span.previous_final_update_id,
                )
            previous = span.final_update_id
        self._previous_update_id = previous
        self._snapshot_last_update_id = None
        self._pending.clear()
        return BridgeResult(BridgeStatus.BRIDGED, bridge_index=0)
