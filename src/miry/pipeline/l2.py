from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

import orjson
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, model_validator

from miry.contracts.models import GapEvent, GapState, StreamType
from miry.contracts.serde import atomic_write_bytes, canonical_json_bytes
from miry.contracts.symbols import validate_exchange_symbol


class L2State(StrEnum):
    UNANCHORED = "UNANCHORED"
    VALID = "VALID"
    GAPPED = "GAPPED"


class DepthDiffCheckpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    connection_id: str
    receive_seq: int
    received_ns: int
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int
    payload_hash: str
    bids: tuple[tuple[str, str], ...]
    asks: tuple[tuple[str, str], ...]


class AnchoredBookCheckpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    connection_id: str
    state: L2State
    previous_update_id: int
    anchor_last_update_id: int
    anchor_received_ns: int
    bids: tuple[tuple[str, str], ...]
    asks: tuple[tuple[str, str], ...]
    pending: tuple[DepthDiffCheckpoint, ...] = ()

    @model_validator(mode="after")
    def validate_anchor(self) -> AnchoredBookCheckpoint:
        if self.state is L2State.VALID or not self.bids or not self.asks:
            raise ValueError("anchored checkpoint must be an unbridged book")
        return self


class L2Checkpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    collector_id: str
    utc_date: date
    exchange_symbol: str
    state: L2State
    connection_id: str | None = None
    previous_update_id: int | None = None
    valid_through_ns: int | None = None
    bids: tuple[tuple[str, str], ...] = ()
    asks: tuple[tuple[str, str], ...] = ()
    anchored_books: tuple[AnchoredBookCheckpoint, ...] = ()

    @model_validator(mode="after")
    def validate_state(self) -> L2Checkpoint:
        values = (
            self.connection_id,
            self.previous_update_id,
            self.valid_through_ns,
        )
        if self.state is L2State.VALID:
            if any(value is None for value in values) or not self.bids or not self.asks:
                raise ValueError("valid L2 checkpoint is incomplete")
        elif any(value is not None for value in values) or self.bids or self.asks:
            raise ValueError("invalid L2 checkpoint cannot carry book state")
        return self


@dataclass(frozen=True, slots=True)
class DepthDiff:
    connection_id: str
    receive_seq: int
    received_ns: int
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int
    payload_hash: bytes
    bids: tuple[tuple[str, str], ...]
    asks: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class DepthSnapshot:
    connection_id: str
    receive_seq: int
    received_ns: int
    last_update_id: int
    bids: tuple[tuple[str, str], ...]
    asks: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class StateChange:
    connection_id: str
    at_ns: int
    state: L2State
    update_id: int | None
    reason: str


@dataclass(slots=True)
class ConnectionBook:
    connection_id: str
    state: L2State = L2State.UNANCHORED
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    previous_update_id: int | None = None
    pending: list[DepthDiff] = field(default_factory=list)
    seen_diffs: set[tuple[int, int, int, bytes]] = field(default_factory=set)
    anchor_last_update_id: int | None = None
    anchor_received_ns: int | None = None

    @classmethod
    def from_checkpoint(cls, checkpoint: L2Checkpoint) -> ConnectionBook:
        if checkpoint.state is not L2State.VALID or checkpoint.connection_id is None:
            raise ValueError("only a valid checkpoint can restore a connection book")
        return cls(
            connection_id=checkpoint.connection_id,
            state=L2State.VALID,
            bids=_book_side(checkpoint.bids),
            asks=_book_side(checkpoint.asks),
            previous_update_id=checkpoint.previous_update_id,
        )

    @classmethod
    def from_anchor_checkpoint(cls, checkpoint: AnchoredBookCheckpoint) -> ConnectionBook:
        return cls(
            connection_id=checkpoint.connection_id,
            state=checkpoint.state,
            bids=_book_side(checkpoint.bids),
            asks=_book_side(checkpoint.asks),
            previous_update_id=checkpoint.previous_update_id,
            pending=[_diff_from_checkpoint(item) for item in checkpoint.pending],
            anchor_last_update_id=checkpoint.anchor_last_update_id,
            anchor_received_ns=checkpoint.anchor_received_ns,
        )

    def on_diff(self, diff: DepthDiff) -> StateChange | None:
        identity = (
            diff.first_update_id,
            diff.final_update_id,
            diff.previous_final_update_id,
            diff.payload_hash,
        )
        if identity in self.seen_diffs:
            return None
        self.seen_diffs.add(identity)
        if self.state is not L2State.VALID:
            self.pending.append(diff)
            return self._try_bridge()
        if diff.previous_final_update_id != self.previous_update_id:
            self.invalidate()
            self.pending = [diff]
            return StateChange(
                self.connection_id,
                diff.received_ns,
                L2State.GAPPED,
                diff.final_update_id,
                "pu_discontinuity",
            )
        self._apply(diff)
        return None

    def invalidate(self) -> None:
        self.state = L2State.GAPPED
        self.bids.clear()
        self.asks.clear()
        self.previous_update_id = None
        self.pending.clear()
        self.anchor_last_update_id = None
        self.anchor_received_ns = None

    def on_snapshot(self, snapshot: DepthSnapshot) -> StateChange | None:
        self.bids = _book_side(snapshot.bids)
        self.asks = _book_side(snapshot.asks)
        self.previous_update_id = snapshot.last_update_id
        self.anchor_last_update_id = snapshot.last_update_id
        self.anchor_received_ns = snapshot.received_ns
        return self._try_bridge()

    def _try_bridge(self) -> StateChange | None:
        if self.anchor_last_update_id is None or self.anchor_received_ns is None:
            return None
        candidates = sorted(self.pending, key=lambda diff: diff.receive_seq)
        candidates = [
            diff for diff in candidates if diff.final_update_id >= self.anchor_last_update_id
        ]
        bridge_index = next(
            (
                index
                for index, diff in enumerate(candidates)
                if diff.first_update_id <= self.anchor_last_update_id <= diff.final_update_id
            ),
            None,
        )
        if bridge_index is None:
            self.pending = candidates
            return None

        bridge = candidates[bridge_index]
        self._apply(bridge)
        for diff in candidates[bridge_index + 1 :]:
            if diff.previous_final_update_id != self.previous_update_id:
                self.invalidate()
                self.pending = [diff]
                return StateChange(
                    self.connection_id,
                    diff.received_ns,
                    L2State.GAPPED,
                    diff.final_update_id,
                    "buffered_pu_discontinuity",
                )
            self._apply(diff)
        self.pending.clear()
        self.state = L2State.VALID
        valid_at = max(self.anchor_received_ns, bridge.received_ns)
        self.anchor_last_update_id = None
        self.anchor_received_ns = None
        return StateChange(
            self.connection_id,
            valid_at,
            L2State.VALID,
            self.previous_update_id,
            "snapshot_bridge",
        )

    def _apply(self, diff: DepthDiff) -> None:
        _apply_levels(self.bids, diff.bids)
        _apply_levels(self.asks, diff.asks)
        self.previous_update_id = diff.final_update_id


class L2DayReconstructor:
    def __init__(
        self,
        *,
        derived_root: Path,
        collector_id: str,
        utc_date: date,
        exchange_symbol: str,
    ) -> None:
        exchange_symbol = validate_exchange_symbol(exchange_symbol)
        self._typed_root = (
            derived_root / "typed" / f"collector={collector_id}" / f"date={utc_date.isoformat()}"
        )
        self._quality_root = (
            derived_root
            / "quality"
            / f"collector={collector_id}"
            / f"date={utc_date.isoformat()}"
            / f"symbol={exchange_symbol}"
        )
        self._transport_gap_path = (
            derived_root
            / "quality"
            / f"collector={collector_id}"
            / f"date={utc_date.isoformat()}"
            / "transport-gaps.jsonl"
        )
        self._date = utc_date
        self._symbol = exchange_symbol
        self._collector_id = collector_id
        self._day_start_ns = int(
            datetime.combine(utc_date, datetime.min.time(), UTC).timestamp() * 1_000_000_000
        )
        self._day_end_ns = int(
            datetime.combine(utc_date + timedelta(days=1), datetime.min.time(), UTC).timestamp()
            * 1_000_000_000
        )

    def run(self) -> tuple[int, int]:
        books: dict[str, ConnectionBook] = {}
        changes: list[StateChange] = []
        intervals: list[dict[str, Any]] = []
        authority: str | None = None
        valid_from: int | None = None
        last_valid_connection: str | None = None
        active_gaps: set[str] = set()

        can_start_without_checkpoint = self._can_start_without_previous_checkpoint()
        checkpoint = self._load_previous_checkpoint()
        if checkpoint is None and not can_start_without_checkpoint:
            raise FileNotFoundError(
                f"previous L2 checkpoint is required for {self._date}: {self._symbol}"
            )
        if checkpoint is not None:
            if checkpoint.state is L2State.VALID:
                if checkpoint.connection_id is None:
                    raise ValueError("valid checkpoint has no connection ID")
                book = ConnectionBook.from_checkpoint(checkpoint)
                books[book.connection_id] = book
                authority = book.connection_id
                last_valid_connection = book.connection_id
                valid_from = self._day_start_ns
            for anchored in checkpoint.anchored_books:
                if anchored.connection_id in books:
                    raise ValueError("checkpoint repeats a connection ID")
                books[anchored.connection_id] = ConnectionBook.from_anchor_checkpoint(anchored)

        gap_events = iter(self._transport_gap_events())
        next_gap = next(gap_events, None)

        def handle_gap(event: GapEvent) -> None:
            nonlocal authority, valid_from, last_valid_connection
            effective_ns = _gap_effective_ns(event)
            if event.state is GapState.OPEN:
                if authority is not None and valid_from is not None:
                    intervals.append(
                        _interval(
                            valid_from,
                            effective_ns,
                            authority,
                            "transport_gap",
                        )
                    )
                authority = None
                valid_from = None
                last_valid_connection = None
                active_gaps.add(event.gap_id)
                for book in books.values():
                    if book.state is L2State.VALID:
                        changes.append(
                            StateChange(
                                book.connection_id,
                                effective_ns,
                                L2State.GAPPED,
                                book.previous_update_id,
                                event.reason.value,
                            )
                        )
                    book.invalidate()
                return

            active_gaps.discard(event.gap_id)
            if active_gaps or last_valid_connection is None:
                return
            candidate = books.get(last_valid_connection)
            if candidate is None or candidate.state is not L2State.VALID:
                return
            authority = last_valid_connection
            valid_from = event.observed_at_realtime_ns

        for row in self._depth_rows():
            received_ns = int(row["app_receive_realtime_ns"])
            while next_gap is not None and _gap_effective_ns(next_gap) <= received_ns:
                handle_gap(next_gap)
                next_gap = next(gap_events, None)
            connection_id = str(row["connection_id"])
            book = books.setdefault(connection_id, ConnectionBook(connection_id))
            stream = StreamType(str(row["stream_type"]))
            if stream is StreamType.DEPTH:
                change = book.on_diff(_diff_from_row(row))
            else:
                change = book.on_snapshot(_snapshot_from_row(row))
            if change is None:
                continue
            changes.append(change)
            if change.state is L2State.VALID:
                last_valid_connection = connection_id
                if active_gaps:
                    continue
                if authority is not None and valid_from is not None:
                    intervals.append(
                        _interval(valid_from, change.at_ns, authority, "connection_switch")
                    )
                authority = connection_id
                valid_from = change.at_ns
            elif authority == connection_id and valid_from is not None:
                intervals.append(_interval(valid_from, change.at_ns, authority, change.reason))
                authority = None
                valid_from = None
                if last_valid_connection == connection_id:
                    last_valid_connection = None

        while next_gap is not None:
            handle_gap(next_gap)
            next_gap = next(gap_events, None)
        if authority is not None and valid_from is not None:
            intervals.append(_interval(valid_from, self._day_end_ns, authority, "utc_day_end"))
        intervals = [
            interval
            for interval in intervals
            if interval["valid_to_ns"] > interval["valid_from_ns"]
        ]
        final_book = books.get(authority) if authority is not None and not active_gaps else None
        output_checkpoint = self._build_checkpoint(final_book, books)
        self._write(changes, intervals, output_checkpoint)
        return len(changes), len(intervals)

    def _can_start_without_previous_checkpoint(self) -> bool:
        path = self._typed_root / "_NORMALIZED.json"
        if not path.exists():
            raise FileNotFoundError(f"normalized day marker does not exist: {path}")
        marker = orjson.loads(path.read_bytes())
        formal_start_ns = marker.get("formal_start_realtime_ns")
        if formal_start_ns is not None:
            if not isinstance(formal_start_ns, int) or not (
                self._day_start_ns <= formal_start_ns < self._day_end_ns
            ):
                raise ValueError(f"normalized formal start is invalid: {path}")
            return True

        previous_date = self._date - timedelta(days=1)
        previous_path = (
            self._typed_root.parent / f"date={previous_date.isoformat()}" / "_NORMALIZED.json"
        )
        if not previous_path.exists():
            return False
        previous = orjson.loads(previous_path.read_bytes())
        expected_symbols = previous.get("expected_symbols")
        if not isinstance(expected_symbols, list) or any(
            not isinstance(symbol, str) for symbol in expected_symbols
        ):
            raise ValueError(f"previous normalized universe is invalid: {previous_path}")
        return self._symbol not in expected_symbols

    def _load_previous_checkpoint(self) -> L2Checkpoint | None:
        previous_date = self._date - timedelta(days=1)
        path = (
            self._quality_root.parent.parent
            / f"date={previous_date.isoformat()}"
            / f"symbol={self._symbol}"
            / "l2-checkpoint.json"
        )
        if not path.exists():
            return None
        checkpoint = L2Checkpoint.model_validate_json(path.read_bytes())
        if (
            checkpoint.collector_id != self._collector_id
            or checkpoint.utc_date != previous_date
            or checkpoint.exchange_symbol != self._symbol
        ):
            raise ValueError(f"previous L2 checkpoint identity mismatch: {path}")
        if checkpoint.state is L2State.VALID and checkpoint.valid_through_ns != self._day_start_ns:
            raise ValueError(f"previous L2 checkpoint does not reach UTC boundary: {path}")
        return checkpoint

    def _build_checkpoint(
        self, book: ConnectionBook | None, books: dict[str, ConnectionBook]
    ) -> L2Checkpoint:
        anchored_books = tuple(
            _anchor_checkpoint(candidate)
            for candidate in books.values()
            if candidate.anchor_last_update_id is not None
            and candidate.anchor_received_ns is not None
            and candidate.bids
            and candidate.asks
        )
        if (
            book is None
            or book.state is not L2State.VALID
            or book.previous_update_id is None
            or not book.bids
            or not book.asks
        ):
            return L2Checkpoint(
                collector_id=self._collector_id,
                utc_date=self._date,
                exchange_symbol=self._symbol,
                state=L2State.UNANCHORED,
                anchored_books=anchored_books,
            )
        return L2Checkpoint(
            collector_id=self._collector_id,
            utc_date=self._date,
            exchange_symbol=self._symbol,
            state=L2State.VALID,
            connection_id=book.connection_id,
            previous_update_id=book.previous_update_id,
            valid_through_ns=self._day_end_ns,
            bids=_checkpoint_levels(book.bids),
            asks=_checkpoint_levels(book.asks),
            anchored_books=anchored_books,
        )

    def _depth_rows(self) -> Any:
        files = sorted(self._typed_root.glob("*.typed.parquet"))
        columns = [
            "exchange_symbol",
            "stream_type",
            "connection_id",
            "receive_seq",
            "app_receive_realtime_ns",
            "payload_hash",
            "first_update_id",
            "final_update_id",
            "previous_final_update_id",
            "last_update_id",
            "bids",
            "asks",
        ]
        for path in files:
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(batch_size=10_000, columns=columns):
                for row in batch.to_pylist():
                    if row["exchange_symbol"] != self._symbol:
                        continue
                    if row["stream_type"] not in {
                        StreamType.DEPTH.value,
                        StreamType.DEPTH_SNAPSHOT.value,
                    }:
                        continue
                    yield row

    def _transport_gap_events(self) -> tuple[GapEvent, ...]:
        if not self._transport_gap_path.exists():
            return ()
        events = []
        with self._transport_gap_path.open("rb") as source:
            for line in source:
                event = GapEvent.model_validate_json(line)
                if event.exchange_symbols and self._symbol not in event.exchange_symbols:
                    continue
                if event.stream_types and StreamType.DEPTH not in event.stream_types:
                    continue
                events.append(event)
        return tuple(
            sorted(
                events,
                key=lambda event: (
                    _gap_effective_ns(event),
                    event.gap_id,
                    event.state,
                ),
            )
        )

    def _write(
        self,
        changes: list[StateChange],
        intervals: list[dict[str, Any]],
        checkpoint: L2Checkpoint,
    ) -> None:
        change_rows = [
            {
                "schema_version": 1,
                "exchange_symbol": self._symbol,
                "connection_id": change.connection_id,
                "at_ns": change.at_ns,
                "state": change.state.value,
                "update_id": change.update_id,
                "reason": change.reason,
            }
            for change in changes
        ]
        atomic_write_bytes(
            self._quality_root / "connection-states.jsonl",
            b"".join(canonical_json_bytes(row) for row in change_rows),
        )
        atomic_write_bytes(
            self._quality_root / "l2-validity.jsonl",
            b"".join(canonical_json_bytes(row) for row in intervals),
        )
        atomic_write_bytes(
            self._quality_root / "l2-checkpoint.json",
            canonical_json_bytes(checkpoint),
        )


def _gap_effective_ns(event: GapEvent) -> int:
    if event.state is GapState.OPEN and event.affected_from_realtime_ns is not None:
        return event.affected_from_realtime_ns
    return event.observed_at_realtime_ns


def _diff_from_row(row: dict[str, Any]) -> DepthDiff:
    return DepthDiff(
        connection_id=str(row["connection_id"]),
        receive_seq=int(row["receive_seq"]),
        received_ns=int(row["app_receive_realtime_ns"]),
        first_update_id=int(row["first_update_id"]),
        final_update_id=int(row["final_update_id"]),
        previous_final_update_id=int(row["previous_final_update_id"]),
        payload_hash=bytes(row["payload_hash"]),
        bids=_row_levels(row["bids"]),
        asks=_row_levels(row["asks"]),
    )


def _snapshot_from_row(row: dict[str, Any]) -> DepthSnapshot:
    return DepthSnapshot(
        connection_id=str(row["connection_id"]),
        receive_seq=int(row["receive_seq"]),
        received_ns=int(row["app_receive_realtime_ns"]),
        last_update_id=int(row["last_update_id"]),
        bids=_row_levels(row["bids"]),
        asks=_row_levels(row["asks"]),
    )


def _row_levels(values: list[dict[str, str]]) -> tuple[tuple[str, str], ...]:
    return tuple((value["price"], value["quantity"]) for value in values)


def _book_side(levels: tuple[tuple[str, str], ...]) -> dict[Decimal, Decimal]:
    return {
        Decimal(price): Decimal(quantity) for price, quantity in levels if Decimal(quantity) != 0
    }


def _checkpoint_levels(side: dict[Decimal, Decimal]) -> tuple[tuple[str, str], ...]:
    return tuple((str(price), str(quantity)) for price, quantity in sorted(side.items()))


def _anchor_checkpoint(book: ConnectionBook) -> AnchoredBookCheckpoint:
    if (
        book.previous_update_id is None
        or book.anchor_last_update_id is None
        or book.anchor_received_ns is None
    ):
        raise ValueError("connection book has no anchor")
    pending = tuple(
        DepthDiffCheckpoint(
            connection_id=diff.connection_id,
            receive_seq=diff.receive_seq,
            received_ns=diff.received_ns,
            first_update_id=diff.first_update_id,
            final_update_id=diff.final_update_id,
            previous_final_update_id=diff.previous_final_update_id,
            payload_hash=diff.payload_hash.hex(),
            bids=diff.bids,
            asks=diff.asks,
        )
        for diff in book.pending
        if diff.final_update_id >= book.anchor_last_update_id
    )
    return AnchoredBookCheckpoint(
        connection_id=book.connection_id,
        state=book.state,
        previous_update_id=book.previous_update_id,
        anchor_last_update_id=book.anchor_last_update_id,
        anchor_received_ns=book.anchor_received_ns,
        bids=_checkpoint_levels(book.bids),
        asks=_checkpoint_levels(book.asks),
        pending=pending,
    )


def _diff_from_checkpoint(checkpoint: DepthDiffCheckpoint) -> DepthDiff:
    return DepthDiff(
        connection_id=checkpoint.connection_id,
        receive_seq=checkpoint.receive_seq,
        received_ns=checkpoint.received_ns,
        first_update_id=checkpoint.first_update_id,
        final_update_id=checkpoint.final_update_id,
        previous_final_update_id=checkpoint.previous_final_update_id,
        payload_hash=bytes.fromhex(checkpoint.payload_hash),
        bids=checkpoint.bids,
        asks=checkpoint.asks,
    )


def _apply_levels(side: dict[Decimal, Decimal], levels: tuple[tuple[str, str], ...]) -> None:
    for price_text, quantity_text in levels:
        price = Decimal(price_text)
        quantity = Decimal(quantity_text)
        if quantity == 0:
            side.pop(price, None)
        else:
            side[price] = quantity


def _interval(start: int, end: int, connection_id: str, reason: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "valid_from_ns": start,
        "valid_to_ns": end,
        "connection_id": connection_id,
        "end_reason": reason,
    }
