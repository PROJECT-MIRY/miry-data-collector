from __future__ import annotations

import asyncio
import json
import os
import time as wall_time
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pytest

from miry.collector.day_index import DayIndex
from miry.collector.gaps import GapJournal
from miry.collector.ingest import IngestCoordinator
from miry.collector.lease import CollectorLease
from miry.collector.queue import ByteBoundedQueues, QueueOverloaded
from miry.collector.spool import SpoolManager
from miry.collector.writer import ChunkLimits, WriterPool
from miry.contracts.models import (
    Ack,
    ChunkManifest,
    ChunkRef,
    DayManifest,
    GapEvent,
    GapReason,
    GapState,
    RawEvent,
    StreamType,
    WriterGroup,
)
from miry.contracts.serde import atomic_write_bytes, canonical_json_bytes

HASH_A = "a" * 64
HASH_B = "b" * 64


@pytest.mark.asyncio
async def test_queue_boundary_rotation_and_exact_ack(tmp_path: Path) -> None:
    queues = ByteBoundedQueues(2_000, warn_ratio=0.7, resume_ratio=0.5)
    day_index = DayIndex(tmp_path, "tokyo01")
    writers = WriterPool(
        tmp_path,
        collector_id="tokyo01",
        data_contract_hash=HASH_A,
        universe_hash=HASH_A,
        queues=queues,
        day_index=day_index,
        limits=ChunkLimits(60, 1_000_000, 10_000, 1, 1),
    )
    writers.start()
    ingest = IngestCoordinator(queues, writers)
    await ingest.put(_event(1, b'{"first":1}'))
    await ingest.rotate(universe_hash=HASH_B)
    await ingest.put(_event(2, b'{"second":2}'))
    await writers.stop()

    manifests = sorted((tmp_path / "ready").rglob("*.manifest.json"))
    assert len(manifests) == 2
    parsed = [__import__("json").loads(path.read_bytes()) for path in manifests]
    assert {item["universe_hash"] for item in parsed} == {HASH_A, HASH_B}

    spool = SpoolManager(tmp_path, max_bytes=10**9, minimum_free_bytes=0)
    spool.initialize()
    first = parsed[0]
    wrong = Ack(chunk_id=first["chunk_id"], sha256=HASH_A, durable_at=datetime.now(UTC))
    ack_path = tmp_path / "control/acks" / f"{first['chunk_id']}.ack.json"
    atomic_write_bytes(ack_path, canonical_json_bytes(wrong))
    rejected = spool.apply_acks()
    assert rejected.applied == 0
    assert rejected.hash_mismatches == 1
    assert (tmp_path / "ready" / first["data_path"]).exists()

    exact = Ack(chunk_id=first["chunk_id"], sha256=first["sha256"], durable_at=datetime.now(UTC))
    atomic_write_bytes(ack_path, canonical_json_bytes(exact))
    assert spool.apply_acks().applied == 1
    assert not (tmp_path / "ready" / first["data_path"]).exists()
    chunk_date = date.fromisoformat(first["utc_date"])
    sealed = await day_index.seal(
        chunk_date,
        sealed_at=datetime.combine(chunk_date + timedelta(days=1), time.min, UTC),
    )
    assert len(DayManifest.model_validate_json(sealed.read_bytes()).chunks) == 2
    assert not (tmp_path / "control/acked-manifests" / f"date={chunk_date}").exists()
    second = parsed[1]
    atomic_write_bytes(
        tmp_path / "control/acks" / f"{second['chunk_id']}.ack.json",
        canonical_json_bytes(
            Ack(
                chunk_id=second["chunk_id"],
                sha256=second["sha256"],
                durable_at=datetime.now(UTC),
            )
        ),
    )
    assert spool.apply_acks().applied == 1
    assert not (tmp_path / "control/acked-manifests" / f"date={chunk_date}").exists()


@pytest.mark.asyncio
async def test_queue_hard_limit_rejects_without_silent_eviction() -> None:
    queues = ByteBoundedQueues(300, warn_ratio=0.7, resume_ratio=0.5)
    with pytest.raises(QueueOverloaded):
        await queues.put(_event(1, b"x" * 100))
    assert queues.used_bytes == 0


@pytest.mark.asyncio
async def test_queue_tracks_activity_per_writer_group(monkeypatch: pytest.MonkeyPatch) -> None:
    queues = ByteBoundedQueues(2_000, warn_ratio=0.7, resume_ratio=0.5)
    monkeypatch.setattr("miry.collector.queue.time.monotonic", lambda: 100.0)

    await queues.put(_event(1, b'{"depth":1}'))

    assert queues.idle_seconds(WriterGroup.TRADES_MARKET, now=103.5) == 3.5
    assert queues.idle_seconds(WriterGroup.DEPTH, now=103.5) is None


def test_spool_skips_manifest_scan_when_there_are_no_acks(tmp_path: Path) -> None:
    spool = SpoolManager(tmp_path, max_bytes=10**9, minimum_free_bytes=0)
    spool.initialize()
    malformed = tmp_path / "ready/date=2026-08-10/writer=depth/bad.manifest.json"
    malformed.parent.mkdir(parents=True)
    malformed.write_bytes(b"not-json")

    assert spool.apply_acks().seen == 0


def test_spool_quarantines_malformed_ack_without_blocking_valid_ack(tmp_path: Path) -> None:
    data = b"durable chunk"
    relative = "date=2026-08-10/writer=depth/chunk-valid.parquet"
    manifest = ChunkManifest(
        chunk_id="chunk-valid",
        data_path=relative,
        sha256=__import__("hashlib").sha256(data).hexdigest(),
        size_bytes=len(data),
        content_type="application/vnd.apache.parquet",
        collector_id="tokyo01",
        writer_group=WriterGroup.DEPTH,
        utc_date=date(2026, 8, 10),
        event_count=1,
        min_app_receive_realtime_ns=1,
        max_app_receive_realtime_ns=1,
        data_contract_hash=HASH_A,
        universe_hash=HASH_A,
        created_at=datetime(2026, 8, 10, tzinfo=UTC),
    )
    data_path = tmp_path / "ready" / relative
    atomic_write_bytes(data_path, data)
    atomic_write_bytes(
        data_path.with_suffix(".manifest.json"), canonical_json_bytes(manifest)
    )
    ack_root = tmp_path / "control/acks"
    atomic_write_bytes(ack_root / "a-malformed.ack.json", b"not-json")
    atomic_write_bytes(
        ack_root / "chunk-valid.ack.json",
        canonical_json_bytes(
            Ack(
                chunk_id=manifest.chunk_id,
                sha256=manifest.sha256,
                durable_at=datetime.now(UTC),
            )
        ),
    )
    spool = SpoolManager(tmp_path, max_bytes=10**9, minimum_free_bytes=0)
    spool.initialize()

    result = spool.apply_acks()

    assert result.applied == 1
    assert result.invalid == 1
    assert not data_path.exists()
    assert not any(ack_root.iterdir())
    assert len(tuple((tmp_path / "control/rejected-acks").glob("*.rejected"))) == 1


def test_spool_accepts_exact_ack_replay_after_gc(tmp_path: Path) -> None:
    data = b"durable chunk"
    relative = "date=2026-08-10/writer=depth/chunk-replay.parquet"
    manifest = ChunkManifest(
        chunk_id="chunk-replay",
        data_path=relative,
        sha256=__import__("hashlib").sha256(data).hexdigest(),
        size_bytes=len(data),
        content_type="application/vnd.apache.parquet",
        collector_id="tokyo01",
        writer_group=WriterGroup.DEPTH,
        utc_date=date(2026, 8, 10),
        event_count=1,
        min_app_receive_realtime_ns=1,
        max_app_receive_realtime_ns=1,
        data_contract_hash=HASH_A,
        universe_hash=HASH_A,
        created_at=datetime(2026, 8, 10, tzinfo=UTC),
    )
    data_path = tmp_path / "ready" / relative
    manifest_path = data_path.with_suffix(".manifest.json")
    ack_path = tmp_path / "control/acks/chunk-replay.ack.json"
    ack = Ack(
        chunk_id=manifest.chunk_id,
        sha256=manifest.sha256,
        durable_at=datetime.now(UTC),
    )
    atomic_write_bytes(data_path, data)
    atomic_write_bytes(manifest_path, canonical_json_bytes(manifest))
    atomic_write_bytes(ack_path, canonical_json_bytes(ack))
    spool = SpoolManager(tmp_path, max_bytes=10**9, minimum_free_bytes=0)
    spool.initialize()

    assert spool.apply_acks().applied == 1
    atomic_write_bytes(ack_path, canonical_json_bytes(ack))

    replay = spool.apply_acks()

    assert replay.replayed == 1
    assert replay.unknown == 0
    assert not any((tmp_path / "control/rejected-acks").iterdir())

    atomic_write_bytes(
        ack_path,
        canonical_json_bytes(ack.model_copy(update={"sha256": HASH_A})),
    )
    mismatch = spool.apply_acks()
    assert mismatch.replayed == 0
    assert mismatch.hash_mismatches == 1
    assert len(tuple((tmp_path / "control/rejected-acks").iterdir())) == 1


def test_spool_recovers_gc_transaction_after_audit_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = b"durable chunk"
    relative = "date=2026-08-10/writer=depth/chunk-recovery.parquet"
    manifest = ChunkManifest(
        chunk_id="chunk-recovery",
        data_path=relative,
        sha256=__import__("hashlib").sha256(data).hexdigest(),
        size_bytes=len(data),
        content_type="application/vnd.apache.parquet",
        collector_id="tokyo01",
        writer_group=WriterGroup.DEPTH,
        utc_date=date(2026, 8, 10),
        event_count=1,
        min_app_receive_realtime_ns=1,
        max_app_receive_realtime_ns=1,
        data_contract_hash=HASH_A,
        universe_hash=HASH_A,
        created_at=datetime(2026, 8, 10, tzinfo=UTC),
    )
    data_path = tmp_path / "ready" / relative
    atomic_write_bytes(data_path, data)
    atomic_write_bytes(
        data_path.with_suffix(".manifest.json"), canonical_json_bytes(manifest)
    )
    atomic_write_bytes(
        tmp_path / "control/acks/chunk-recovery.ack.json",
        canonical_json_bytes(
            Ack(
                chunk_id=manifest.chunk_id,
                sha256=manifest.sha256,
                durable_at=datetime.now(UTC),
            )
        ),
    )
    spool = SpoolManager(tmp_path, max_bytes=10**9, minimum_free_bytes=0)
    spool.initialize()
    original_append = spool.transfer_journal.append

    def fail_audit(_events: object) -> int:
        raise OSError("simulated audit filesystem failure")

    monkeypatch.setattr(spool.transfer_journal, "append", fail_audit)
    with pytest.raises(OSError, match="simulated audit filesystem failure"):
        spool.apply_acks()

    assert not data_path.exists()
    assert len(tuple((tmp_path / "control/applying-acks").glob("*.transaction.json"))) == 1
    monkeypatch.setattr(spool.transfer_journal, "append", original_append)

    recovered = spool.apply_acks()

    assert recovered.applied == 1
    assert recovered.recovered == 1
    assert not any((tmp_path / "control/applying-acks").iterdir())
    events = [
        json.loads(line)
        for path in (tmp_path / "control/transfer-ledger").rglob("events.jsonl")
        for line in path.read_text(encoding="ascii").splitlines()
    ]
    assert any(event["event"] == "REMOTE_GC" and event["recovered"] for event in events)


@pytest.mark.asyncio
async def test_day_index_fsync_does_not_block_the_event_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_fsync = os.fsync

    def slow_fsync(descriptor: int) -> None:
        wall_time.sleep(0.08)
        original_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", slow_fsync)
    day_index = DayIndex(tmp_path, "tokyo01")
    chunk = ChunkRef(
        chunk_id="chunk-test",
        data_path="date=2026-08-10/writer=depth/chunk-test.parquet",
        sha256=HASH_A,
        size_bytes=1,
        content_type="application/vnd.apache.parquet",
    )
    loop = asyncio.get_running_loop()

    async def heartbeat() -> float:
        started = loop.time()
        await asyncio.sleep(0.005)
        return loop.time() - started

    heartbeat_task = asyncio.create_task(heartbeat())
    await asyncio.sleep(0)
    await day_index.record(date(2026, 8, 10), chunk)

    assert await heartbeat_task < 0.04


@pytest.mark.asyncio
async def test_gap_artifact_write_does_not_block_the_event_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day_index = DayIndex(tmp_path, "tokyo01")
    gaps = GapJournal(
        tmp_path,
        collector_id="tokyo01",
        data_contract_hash=HASH_A,
        universe_hash=HASH_A,
        day_index=day_index,
        reserve_bytes=4096,
    )
    gaps.initialize()
    original_write = gaps._write

    def slow_write(event: GapEvent) -> ChunkManifest:
        wall_time.sleep(0.08)
        return original_write(event)

    monkeypatch.setattr(gaps, "_write", slow_write)
    loop = asyncio.get_running_loop()

    async def heartbeat() -> float:
        started = loop.time()
        await asyncio.sleep(0.005)
        return loop.time() - started

    heartbeat_task = asyncio.create_task(heartbeat())
    await asyncio.sleep(0)
    await gaps.open(GapReason.CONNECTION_LOST, exchange_symbols=("BTCUSDT",))

    assert await heartbeat_task < 0.04


@pytest.mark.asyncio
async def test_gap_reserve_recovers_day_index_write_in_the_same_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    day_index = DayIndex(tmp_path, "tokyo01")
    gaps = GapJournal(
        tmp_path,
        collector_id="tokyo01",
        data_contract_hash=HASH_A,
        universe_hash=HASH_A,
        day_index=day_index,
        reserve_bytes=4096,
    )
    gaps.initialize()
    original_record = day_index._record
    attempts = 0

    def fail_first_record(utc_date: date, chunk: ChunkRef) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("simulated full filesystem")
        original_record(utc_date, chunk)

    monkeypatch.setattr(day_index, "_record", fail_first_record)

    await gaps.open(GapReason.CONNECTION_LOST, exchange_symbols=("BTCUSDT",))

    assert attempts == 2
    assert not (tmp_path / "control/gap-journal.reserve").exists()


def test_unclean_collector_lease_recovers_from_durable_watermark(tmp_path: Path) -> None:
    first = CollectorLease(tmp_path, "boot-one")
    first.write_running(123)

    assert CollectorLease(tmp_path, "boot-two").recovery_start_ns() == 123

    second = CollectorLease(tmp_path, "boot-two")
    second.write_clean(456)
    assert CollectorLease(tmp_path, "boot-three").recovery_start_ns() is None


@pytest.mark.asyncio
async def test_open_gap_survives_restart_until_explicit_close(tmp_path: Path) -> None:
    day_index = DayIndex(tmp_path, "tokyo01")
    gaps = GapJournal(
        tmp_path,
        collector_id="tokyo01",
        data_contract_hash=HASH_A,
        universe_hash=HASH_A,
        day_index=day_index,
        reserve_bytes=4096,
    )
    gaps.initialize()
    opened_at = datetime(2026, 8, 10, 23, 55, tzinfo=UTC)
    closed_at = datetime(2026, 8, 13, 0, 5, tzinfo=UTC)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            "miry.collector.gaps.time_ns",
            lambda: int(opened_at.timestamp() * 1_000_000_000),
        )
        gap_id = await gaps.open(GapReason.COLLECTOR_STOPPED, exchange_symbols=("BTCUSDT",))
    await gaps.rollover(datetime(2026, 8, 11, tzinfo=UTC))
    restarted = GapJournal(
        tmp_path,
        collector_id="tokyo01",
        data_contract_hash=HASH_A,
        universe_hash=HASH_A,
        day_index=day_index,
        reserve_bytes=4096,
    )
    restarted.initialize()
    assert [event.gap_id for event in restarted.stale_open_events()] == [gap_id]
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            "miry.collector.gaps.time_ns",
            lambda: int(closed_at.timestamp() * 1_000_000_000),
        )
        await restarted.close(gap_id, GapReason.COLLECTOR_STOPPED, exchange_symbols=("BTCUSDT",))
    assert restarted.stale_open_events() == ()

    middle_date = date(2026, 8, 12)
    sealed = await day_index.seal(middle_date, sealed_at=datetime(2026, 8, 13, 0, 6, tzinfo=UTC))
    manifest = DayManifest.model_validate_json(sealed.read_bytes())
    events = [
        GapEvent.model_validate_json((tmp_path / "ready" / chunk.data_path).read_bytes())
        for chunk in manifest.chunks
    ]
    events.sort(key=lambda event: event.observed_at_realtime_ns)
    assert [event.state for event in events] == [GapState.OPEN, GapState.CLOSED]


def _event(sequence: int, payload: bytes) -> RawEvent:
    return RawEvent(
        schema_version=1,
        exchange_symbol="BTCUSDT",
        stream_type=StreamType.AGG_TRADE,
        collector_id="tokyo01",
        boot_id="boot",
        segment_id="segment",
        connection_id="connection",
        receive_seq=sequence,
        app_receive_realtime_ns=1_786_320_000_000_000_000 + sequence,
        app_receive_monotonic_ns=sequence,
        payload_bytes=payload,
    )
