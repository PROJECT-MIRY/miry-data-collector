from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import orjson

from miry.contracts.serde import atomic_write_bytes, canonical_json_bytes
from miry.pipeline.l2 import L2Checkpoint

QUALITY_POLICY_VERSION = "l2-coverage-v1"
MINIMUM_L2_VALID_RATIO_PER_MILLE = 999


def finalize_day(
    derived_root: Path, *, collector_id: str, utc_date: date, symbols: tuple[str, ...]
) -> None:
    typed_root = (
        derived_root / "typed" / f"collector={collector_id}" / f"date={utc_date.isoformat()}"
    )
    normalized_path = typed_root / "_NORMALIZED.json"
    if not normalized_path.exists():
        raise FileNotFoundError("day is not normalized")
    normalized = orjson.loads(normalized_path.read_bytes())
    expected_symbols = normalized.get("expected_symbols")
    if (
        not isinstance(expected_symbols, list)
        or len(expected_symbols) != 60
        or any(not isinstance(symbol, str) for symbol in expected_symbols)
        or len(set(expected_symbols)) != 60
    ):
        raise ValueError("normalized day has no authoritative 60-symbol universe")
    expected = tuple(sorted(expected_symbols))
    if tuple(sorted(symbols)) != expected:
        raise ValueError("requested symbols do not match sealed universe")

    day_start_ns = int(
        datetime.combine(utc_date, datetime.min.time(), UTC).timestamp() * 1_000_000_000
    )
    day_end_ns = day_start_ns + 86_400 * 1_000_000_000
    window_start_ns = int(normalized.get("collection_window_start_ns", day_start_ns))
    window_end_ns = int(normalized.get("collection_window_end_ns", day_end_ns))
    if not day_start_ns <= window_start_ns < window_end_ns <= day_end_ns:
        raise ValueError("normalized collection window falls outside UTC day")
    quality_root = (
        derived_root / "quality" / f"collector={collector_id}" / f"date={utc_date.isoformat()}"
    )
    missing = []
    coverage: dict[str, dict[str, int | str]] = {}
    rejected: list[str] = []
    gap_path = quality_root / "transport-gaps.jsonl"
    for symbol in symbols:
        symbol_root = quality_root / f"symbol={symbol}"
        validity_path = symbol_root / "l2-validity.jsonl"
        checkpoint_path = symbol_root / "l2-checkpoint.json"
        if not validity_path.exists() or not checkpoint_path.exists():
            missing.append(symbol)
            continue
        valid_intervals = _validate_validity(validity_path, utc_date=utc_date)
        checkpoint = L2Checkpoint.model_validate_json(checkpoint_path.read_bytes())
        if (
            checkpoint.collector_id != collector_id
            or checkpoint.utc_date != utc_date
            or checkpoint.exchange_symbol != symbol
        ):
            raise ValueError(f"L2 checkpoint identity mismatch: {checkpoint_path}")
        explicit_invalid = _explicit_invalid_intervals(
            gap_path,
            symbol=symbol,
            window_start_ns=window_start_ns,
            window_end_ns=window_end_ns,
        )
        explicit_invalid = _extend_invalid_until_valid(
            explicit_invalid,
            valid_intervals=valid_intervals,
            window_end_ns=window_end_ns,
        )
        valid_ns = _union_duration(
            valid_intervals,
            window_start_ns=window_start_ns,
            window_end_ns=window_end_ns,
        )
        invalid_ns = _union_duration(
            explicit_invalid,
            window_start_ns=window_start_ns,
            window_end_ns=window_end_ns,
        )
        accounted_ns = _union_duration(
            (*valid_intervals, *explicit_invalid),
            window_start_ns=window_start_ns,
            window_end_ns=window_end_ns,
        )
        conflicting_ns = _intersection_duration(
            valid_intervals,
            explicit_invalid,
            window_start_ns=window_start_ns,
            window_end_ns=window_end_ns,
        )
        expected_ns = window_end_ns - window_start_ns
        unclassified_ns = expected_ns - accounted_ns
        coverage[symbol] = {
            "expected_ns": expected_ns,
            "valid_ns": valid_ns,
            "explicit_invalid_ns": invalid_ns,
            "conflicting_ns": conflicting_ns,
            "unclassified_ns": unclassified_ns,
            "valid_ratio": f"{valid_ns / expected_ns:.9f}",
            "accounted_ratio": f"{accounted_ns / expected_ns:.9f}",
        }
        if (
            valid_ns * 1_000 < expected_ns * MINIMUM_L2_VALID_RATIO_PER_MILLE
            or unclassified_ns != 0
            or conflicting_ns != 0
        ):
            rejected.append(symbol)
    if missing:
        raise FileNotFoundError(f"missing L2 outputs: {','.join(missing)}")
    quality_marker = {
        "schema_version": 1,
        "collector_id": collector_id,
        "utc_date": utc_date.isoformat(),
        "symbols": list(symbols),
        "core_generation": normalized.get("core_generation"),
        "candidate_revision": normalized.get("candidate_revision"),
        "decision_sequence": normalized.get("decision_sequence"),
        "universe_version": normalized.get("universe_version"),
        "universe_hash": normalized.get("universe_hash"),
        "sealed_manifest_sha256": normalized.get("sealed_manifest_sha256"),
        "collection_window_start_ns": window_start_ns,
        "collection_window_end_ns": window_end_ns,
        "quality_policy": QUALITY_POLICY_VERSION,
        "minimum_l2_valid_ratio": (f"{MINIMUM_L2_VALID_RATIO_PER_MILLE / 1_000:.3f}"),
        "l2_coverage": coverage,
    }
    if rejected:
        atomic_write_bytes(
            quality_root / "_QUALITY_REJECTED.json",
            canonical_json_bytes({**quality_marker, "rejected_symbols": rejected}),
        )
        raise ValueError(f"L2 coverage below quality contract: {','.join(rejected)}")
    atomic_write_bytes(
        quality_root / "_PROCESSED.json",
        canonical_json_bytes(quality_marker),
    )
    (quality_root / "_QUALITY_REJECTED.json").unlink(missing_ok=True)


def _validate_validity(path: Path, *, utc_date: date) -> tuple[tuple[int, int], ...]:
    day_start = int(
        datetime.combine(utc_date, datetime.min.time(), UTC).timestamp() * 1_000_000_000
    )
    day_end = day_start + 86_400 * 1_000_000_000
    previous_end: int | None = None
    intervals = []
    with path.open("rb") as source:
        for line_number, line in enumerate(source, start=1):
            try:
                interval = orjson.loads(line)
                start = int(interval["valid_from_ns"])
                end = int(interval["valid_to_ns"])
            except (KeyError, TypeError, ValueError, orjson.JSONDecodeError) as exc:
                raise ValueError(f"invalid L2 validity row {path}:{line_number}") from exc
            if (
                interval.get("schema_version") != 1
                or not isinstance(interval.get("connection_id"), str)
                or not interval["connection_id"]
                or not day_start <= start < end <= day_end
                or (previous_end is not None and start < previous_end)
            ):
                raise ValueError(f"invalid L2 validity ordering {path}:{line_number}")
            previous_end = end
            intervals.append((start, end))
    return tuple(intervals)


def _explicit_invalid_intervals(
    path: Path,
    *,
    symbol: str,
    window_start_ns: int,
    window_end_ns: int,
) -> tuple[tuple[int, int], ...]:
    if not path.exists():
        return ()
    events = []
    with path.open("rb") as source:
        for line in source:
            event = orjson.loads(line)
            symbols = event.get("exchange_symbols") or []
            streams = event.get("stream_types") or []
            if symbols and symbol not in symbols:
                continue
            if streams and "depth" not in streams:
                continue
            state = event.get("state")
            observed_ns = int(event["observed_at_realtime_ns"])
            effective_ns = (
                int(event.get("affected_from_realtime_ns") or observed_ns)
                if state == "OPEN"
                else observed_ns
            )
            events.append((effective_ns, str(event["gap_id"]), state, event.get("detail")))
    events.sort()
    opened: dict[str, int] = {}
    intervals = []
    for effective_ns, gap_id, state, detail in events:
        if state == "OPEN":
            opened[gap_id] = effective_ns
        elif state == "CLOSED" and gap_id in opened:
            end_ns = effective_ns + (1 if detail == "gap continues into the next UTC day" else 0)
            intervals.append((opened.pop(gap_id), end_ns))
    intervals.extend((start, window_end_ns) for start in opened.values())
    return tuple(
        (max(start, window_start_ns), min(end, window_end_ns))
        for start, end in intervals
        if max(start, window_start_ns) < min(end, window_end_ns)
    )


def _union_duration(
    intervals: tuple[tuple[int, int], ...], *, window_start_ns: int, window_end_ns: int
) -> int:
    clipped = sorted(
        (max(start, window_start_ns), min(end, window_end_ns))
        for start, end in intervals
        if max(start, window_start_ns) < min(end, window_end_ns)
    )
    total = 0
    current_start: int | None = None
    current_end: int | None = None
    for start, end in clipped:
        if current_start is None or current_end is None:
            current_start, current_end = start, end
        elif start <= current_end:
            current_end = max(current_end, end)
        else:
            total += current_end - current_start
            current_start, current_end = start, end
    if current_start is not None and current_end is not None:
        total += current_end - current_start
    return total


def _extend_invalid_until_valid(
    intervals: tuple[tuple[int, int], ...],
    *,
    valid_intervals: tuple[tuple[int, int], ...],
    window_end_ns: int,
) -> tuple[tuple[int, int], ...]:
    valid = sorted(valid_intervals)
    extended = []
    for start, end in intervals:
        if not any(valid_start <= end < valid_end for valid_start, valid_end in valid):
            end = next(
                (valid_start for valid_start, _valid_end in valid if valid_start >= end),
                window_end_ns,
            )
        extended.append((start, end))
    return tuple(extended)


def _intersection_duration(
    left: tuple[tuple[int, int], ...],
    right: tuple[tuple[int, int], ...],
    *,
    window_start_ns: int,
    window_end_ns: int,
) -> int:
    intersections = tuple(
        (max(left_start, right_start), min(left_end, right_end))
        for left_start, left_end in left
        for right_start, right_end in right
        if max(left_start, right_start) < min(left_end, right_end)
    )
    return _union_duration(
        intersections,
        window_start_ns=window_start_ns,
        window_end_ns=window_end_ns,
    )
