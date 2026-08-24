from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import orjson
import pyarrow.parquet as pq
import pytest

from miry.contracts.collection import data_contract_hash
from miry.contracts.models import (
    ChunkManifest,
    ContentType,
    DayManifest,
    RawEvent,
    StreamType,
    UniverseDecision,
    UniverseDecisionReason,
)
from miry.contracts.raw import raw_events_to_table
from miry.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    sha256_file,
    universe_hash,
)
from miry.pipeline.normalize import DEDUP_WINDOW_NS, DayNormalizer, _Deduplicator

UTC_DATE = date(2026, 8, 10)
RECEIVED_NS = int(datetime(2026, 8, 10, 12, tzinfo=UTC).timestamp() * 1_000_000_000)
UNIVERSE_HASH = "a" * 64
CONTRACT_HASH = data_contract_hash()


def test_normalizer_accepts_manifest_that_matches_raw_chunk(tmp_path: Path) -> None:
    raw_root, derived_root = _write_day(tmp_path)

    result = DayNormalizer(
        raw_root=raw_root,
        derived_root=derived_root,
        collector_id="tokyo01",
        utc_date=UTC_DATE,
    ).run()

    assert result.raw_events == 1
    assert result.typed_events == 1


def test_normalizer_rejects_manifest_event_count_mismatch(tmp_path: Path) -> None:
    raw_root, derived_root = _write_day(tmp_path, manifest_event_count=2)

    with pytest.raises(ValueError, match="raw event count mismatch"):
        DayNormalizer(
            raw_root=raw_root,
            derived_root=derived_root,
            collector_id="tokyo01",
            utc_date=UTC_DATE,
        ).run()


def test_normalizer_rejects_parquet_metadata_mismatch(tmp_path: Path) -> None:
    raw_root, derived_root = _write_day(tmp_path, parquet_universe_hash="b" * 64)

    with pytest.raises(ValueError, match=r"raw metadata mismatch.*universe_hash"):
        DayNormalizer(
            raw_root=raw_root,
            derived_root=derived_root,
            collector_id="tokyo01",
            utc_date=UTC_DATE,
        ).run()


def test_normalizer_binds_formal_window_to_raw_universe_evidence(tmp_path: Path) -> None:
    core = tuple(f"C{index:03}USDT" for index in range(50))
    boundary = tuple(f"B{index:03}USDT" for index in range(5))
    probe = tuple(f"P{index:03}USDT" for index in range(5))
    active_hash = universe_hash(core, boundary, probe)
    effective_at = datetime(2026, 8, 10, tzinfo=UTC)
    decision = UniverseDecision(
        core_generation=6,
        candidate_revision=2,
        decision_sequence=8,
        universe_version="6.2",
        created_at=effective_at,
        effective_at=effective_at,
        reason=UniverseDecisionReason.FORMAL_BOOTSTRAP,
        core=core,
        boundary=boundary,
        probe=probe,
        universe_hash=active_hash,
    )
    formal_start_ns = RECEIVED_NS + 1
    events = [
        _raw_event(
            sequence=1,
            stream_type=StreamType.UNIVERSE_DECISION,
            received_ns=RECEIVED_NS,
            payload=canonical_json_bytes(decision),
            symbol=None,
        ),
        _raw_event(
            sequence=2,
            stream_type=StreamType.FORMAL_COLLECTION_STARTED,
            received_ns=formal_start_ns,
            payload=orjson.dumps(
                {
                    "event": "FORMAL_COLLECTION_STARTED",
                    "experiment_id": "formal-current",
                    "core_generation": 6,
                    "candidate_revision": 2,
                    "decision_sequence": 8,
                    "universe_version": "6.2",
                    "started_at": "2026-08-10T12:00:00+00:00",
                    "universe_hash": active_hash,
                }
            ),
            symbol=None,
        ),
    ]
    raw_root, derived_root = _write_day(
        tmp_path,
        events=events,
        universe_hash_value=active_hash,
    )

    DayNormalizer(
        raw_root=raw_root,
        derived_root=derived_root,
        collector_id="tokyo01",
        utc_date=UTC_DATE,
    ).run()

    marker = orjson.loads(
        (derived_root / "typed/collector=tokyo01/date=2026-08-10/_NORMALIZED.json").read_bytes()
    )
    assert marker["collection_window_start_ns"] == formal_start_ns
    assert marker["expected_symbols"] == list(decision.members)
    assert marker["universe_hash"] == active_hash
    assert marker["schema_version"] == 1
    assert marker["core_generation"] == 6
    assert marker["candidate_revision"] == 2
    assert marker["decision_sequence"] == 8
    assert marker["universe_version"] == "6.2"
    assert marker["formal_start_experiment_id"] == "formal-current"


def test_normalizer_rejects_formal_start_universe_mismatch(tmp_path: Path) -> None:
    core = tuple(f"C{index:03}USDT" for index in range(50))
    boundary = tuple(f"B{index:03}USDT" for index in range(5))
    probe = tuple(f"P{index:03}USDT" for index in range(5))
    active_hash = universe_hash(core, boundary, probe)
    effective_at = datetime(2026, 8, 10, tzinfo=UTC)
    decision = UniverseDecision(
        core_generation=6,
        candidate_revision=2,
        decision_sequence=8,
        universe_version="6.2",
        created_at=effective_at,
        effective_at=effective_at,
        reason=UniverseDecisionReason.FORMAL_BOOTSTRAP,
        core=core,
        boundary=boundary,
        probe=probe,
        universe_hash=active_hash,
    )
    events = [
        _raw_event(
            sequence=1,
            stream_type=StreamType.UNIVERSE_DECISION,
            received_ns=RECEIVED_NS,
            payload=canonical_json_bytes(decision),
            symbol=None,
        ),
        _raw_event(
            sequence=2,
            stream_type=StreamType.FORMAL_COLLECTION_STARTED,
            received_ns=RECEIVED_NS + 1,
            payload=orjson.dumps(
                {
                    "event": "FORMAL_COLLECTION_STARTED",
                    "experiment_id": "formal-current",
                    "core_generation": 6,
                    "candidate_revision": 2,
                    "decision_sequence": 8,
                    "universe_version": "6.2",
                    "started_at": "2026-08-10T12:00:00+00:00",
                    "universe_hash": "f" * 64,
                }
            ),
            symbol=None,
        ),
    ]
    raw_root, derived_root = _write_day(
        tmp_path,
        events=events,
        universe_hash_value=active_hash,
    )

    with pytest.raises(ValueError, match="formal start/universe identity mismatch"):
        DayNormalizer(
            raw_root=raw_root,
            derived_root=derived_root,
            collector_id="tokyo01",
            utc_date=UTC_DATE,
        ).run()


def test_normalizer_marks_market_overlap_replay(tmp_path: Path) -> None:
    payload = orjson.dumps(
        {
            "e": "markPriceUpdate",
            "E": 1,
            "s": "BTCUSDT",
            "p": "100",
            "i": "99",
            "P": "0",
            "r": "0",
            "T": 8,
        }
    )
    events = [
        _raw_event(
            sequence=sequence,
            stream_type=StreamType.MARK_PRICE,
            received_ns=RECEIVED_NS + sequence,
            payload=payload,
            symbol="BTCUSDT",
            connection_id=connection_id,
        )
        for sequence, connection_id in ((1, "old"), (2, "replacement"))
    ]
    raw_root, derived_root = _write_day(tmp_path, events=events)

    result = DayNormalizer(
        raw_root=raw_root,
        derived_root=derived_root,
        collector_id="tokyo01",
        utc_date=UTC_DATE,
    ).run()

    assert result.duplicate_events == 1
    typed_path = next(
        (derived_root / "typed/collector=tokyo01/date=2026-08-10").glob("*.typed.parquet")
    )
    assert pq.read_table(typed_path, columns=["is_duplicate"])["is_duplicate"].to_pylist() == [
        False,
        True,
    ]


def test_parallel_normalizer_matches_serial_cross_chunk_dedup(tmp_path: Path) -> None:
    payload = orjson.dumps(
        {
            "e": "aggTrade",
            "E": 1,
            "T": 2,
            "s": "BTCUSDT",
            "a": 3,
            "p": "100",
            "q": "1",
            "f": 4,
            "l": 5,
            "m": True,
        }
    )
    events = [
        _raw_event(
            sequence=sequence,
            stream_type=StreamType.AGG_TRADE,
            received_ns=RECEIVED_NS + sequence,
            payload=payload,
            symbol="BTCUSDT",
            connection_id=connection_id,
        )
        for sequence, connection_id in ((1, "old"), (2, "replacement"))
    ]
    raw_root, _derived_root = _write_day(
        tmp_path,
        event_groups=[[events[0]], [events[1]]],
    )
    serial_root = tmp_path / "serial"
    parallel_root = tmp_path / "parallel"

    serial = DayNormalizer(
        raw_root=raw_root,
        derived_root=serial_root,
        collector_id="tokyo01",
        utc_date=UTC_DATE,
        max_workers=1,
    ).run()
    parallel = DayNormalizer(
        raw_root=raw_root,
        derived_root=parallel_root,
        collector_id="tokyo01",
        utc_date=UTC_DATE,
        max_workers=2,
    ).run()

    assert parallel == serial
    serial_files = sorted(
        (serial_root / "typed/collector=tokyo01/date=2026-08-10").glob("*.parquet")
    )
    parallel_files = sorted(
        (parallel_root / "typed/collector=tokyo01/date=2026-08-10").glob("*.parquet")
    )
    assert [path.name for path in parallel_files] == [path.name for path in serial_files]
    for serial_path, parallel_path in zip(serial_files, parallel_files, strict=True):
        assert pq.read_table(parallel_path).equals(pq.read_table(serial_path))
    duplicate_flags = [
        flag
        for path in parallel_files
        for flag in pq.read_table(path, columns=["is_duplicate"])["is_duplicate"].to_pylist()
    ]
    assert duplicate_flags == [False, True]


def test_normalizer_rejects_conflicting_payload_for_same_exchange_id(tmp_path: Path) -> None:
    events = [
        _raw_event(
            sequence=sequence,
            stream_type=StreamType.DEPTH,
            received_ns=RECEIVED_NS + sequence,
            payload=orjson.dumps(
                {
                    "e": "depthUpdate",
                    "E": 1,
                    "T": 2,
                    "s": "BTCUSDT",
                    "U": 10,
                    "u": 11,
                    "pu": 9,
                    "b": [["100", quantity]],
                    "a": [],
                }
            ),
            symbol="BTCUSDT",
            connection_id=connection_id,
        )
        for sequence, connection_id, quantity in (
            (1, "old", "1"),
            (2, "replacement", "2"),
        )
    ]
    raw_root, derived_root = _write_day(tmp_path, events=events)

    with pytest.raises(ValueError, match="conflicting payload"):
        DayNormalizer(
            raw_root=raw_root,
            derived_root=derived_root,
            collector_id="tokyo01",
            utc_date=UTC_DATE,
        ).run()


def test_dedup_checkpoint_carries_overlap_identity_across_midnight(tmp_path: Path) -> None:
    day_end_ns = int(datetime(2026, 8, 11, tzinfo=UTC).timestamp() * 1_000_000_000)
    identity = (StreamType.AGG_TRADE, "BTCUSDT", 123)
    payload_hash = b"x" * 32
    first = _Deduplicator()
    assert first.observe(identity, payload_hash, day_end_ns - 1) is False
    checkpoint = tmp_path / "_DEDUP_CHECKPOINT.json"
    atomic_write_bytes(
        checkpoint,
        canonical_json_bytes(first.checkpoint(day_end_ns=day_end_ns)),
    )

    restored = _Deduplicator.load(checkpoint)

    assert restored.observe(identity, payload_hash, day_end_ns + 1) is True
    assert first.checkpoint(day_end_ns=day_end_ns)["window_ns"] == DEDUP_WINDOW_NS


def _write_day(
    tmp_path: Path,
    *,
    manifest_event_count: int | None = None,
    parquet_universe_hash: str | None = None,
    universe_hash_value: str = UNIVERSE_HASH,
    events: list[RawEvent] | None = None,
    event_groups: list[list[RawEvent]] | None = None,
) -> tuple[Path, Path]:
    raw_root = tmp_path / "raw"
    derived_root = tmp_path / "derived"
    collector_root = raw_root / "collector=tokyo01"
    groups = event_groups or [events or [_default_event()]]
    manifests = []
    for index, event_rows in enumerate(groups):
        writer_group = event_rows[0].writer_group
        assert all(event.writer_group is writer_group for event in event_rows)
        chunk_id = f"chunk-test-{index}"
        relative = Path(f"date=2026-08-10/writer={writer_group.value}/{chunk_id}.parquet")
        raw_path = collector_root / relative
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        parquet_hash = parquet_universe_hash or universe_hash_value
        metadata = {
            b"chunk_id": chunk_id.encode(),
            b"collector_id": b"tokyo01",
            b"data_contract_hash": CONTRACT_HASH.encode(),
            b"universe_hash": parquet_hash.encode(),
            b"utc_date": UTC_DATE.isoformat().encode(),
            b"writer_group": writer_group.value.encode(),
        }
        pq.write_table(raw_events_to_table(event_rows).replace_schema_metadata(metadata), raw_path)
        event_count = len(event_rows) if manifest_event_count is None else manifest_event_count
        manifest = ChunkManifest(
            chunk_id=chunk_id,
            data_path=relative.as_posix(),
            sha256=sha256_file(raw_path),
            size_bytes=raw_path.stat().st_size,
            content_type=ContentType.PARQUET,
            collector_id="tokyo01",
            writer_group=writer_group,
            utc_date=UTC_DATE,
            event_count=event_count,
            min_app_receive_realtime_ns=min(event.app_receive_realtime_ns for event in event_rows),
            max_app_receive_realtime_ns=max(event.app_receive_realtime_ns for event in event_rows),
            data_contract_hash=CONTRACT_HASH,
            universe_hash=universe_hash_value,
            created_at=datetime(2026, 8, 10, 12, tzinfo=UTC),
        )
        atomic_write_bytes(raw_path.with_suffix(".manifest.json"), canonical_json_bytes(manifest))
        manifests.append(manifest)
    day = DayManifest(
        collector_id="tokyo01",
        utc_date=UTC_DATE,
        sealed_at=datetime(2026, 8, 11, tzinfo=UTC),
        chunks=tuple(manifest.as_ref() for manifest in manifests),
    )
    atomic_write_bytes(
        collector_root / "day-manifests/date=2026-08-10/SEALED.json",
        canonical_json_bytes(day),
    )
    return raw_root, derived_root


def _default_event() -> RawEvent:
    return _raw_event(
        sequence=1,
        stream_type=StreamType.AGG_TRADE,
        received_ns=RECEIVED_NS,
        payload=(
            b'{"e":"aggTrade","E":1,"T":2,"s":"BTCUSDT","a":3,'
            b'"p":"100","q":"1","f":4,"l":5,"m":true}'
        ),
        symbol="BTCUSDT",
    )


def _raw_event(
    *,
    sequence: int,
    stream_type: StreamType,
    received_ns: int,
    payload: bytes,
    symbol: str | None,
    connection_id: str = "connection",
) -> RawEvent:
    return RawEvent(
        schema_version=1,
        exchange_symbol=symbol,
        stream_type=stream_type,
        collector_id="tokyo01",
        boot_id="boot",
        segment_id="segment",
        connection_id=connection_id,
        receive_seq=sequence,
        app_receive_realtime_ns=received_ns,
        app_receive_monotonic_ns=sequence,
        payload_bytes=payload,
    )
