from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import BinaryIO

import pytest

from miry.cli.pull import main as pull_main
from miry.contracts.models import (
    Ack,
    ChunkManifest,
    ContentType,
    DayManifest,
    WriterGroup,
)
from miry.contracts.serde import canonical_json_bytes, sha256_bytes
from miry.pipeline.config import PullConfig
from miry.pipeline.pull import Puller, RsyncTransport
from miry.transfer import TransferJournal

HASH = "a" * 64


class MemoryRemote:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = files
        self.writes: dict[str, bytes] = {}

    def list_files(self, root: str) -> tuple[str, ...]:
        return tuple(sorted(path for path in self.files if path.startswith(f"{root}/")))

    def read_bytes(self, path: str) -> bytes:
        return self.files[path]

    def download(self, remote_path: str, local_file: BinaryIO) -> None:
        local_file.write(self.files[remote_path])

    def write_atomic(self, path: str, content: bytes) -> None:
        self.writes[path] = content


def test_pull_is_durable_idempotent_and_publishes_complete_day(tmp_path: Path) -> None:
    remote, manifest = _remote_fixture(b"valid parquet stand-in")
    puller = Puller(
        remote,
        remote_ready_root="ready",
        remote_ack_root="control/acks",
        local_raw_root=tmp_path,
    )
    first = puller.run()
    second = puller.run()
    assert first.new_chunks == 1
    assert first.failures == ()
    assert second.new_chunks == 0
    assert second.existing_chunks == 1
    assert second.failures == ()
    assert f"control/acks/{manifest.chunk_id}.ack.json" in remote.writes
    assert (
        tmp_path
        / "collector=tokyo01/day-manifests/date=2026-08-10/SEALED.json"
    ).exists()


def test_hash_mismatch_never_acknowledges(tmp_path: Path) -> None:
    remote, manifest = _remote_fixture(b"expected")
    remote.files[f"ready/{manifest.data_path}"] = b"corrupted"
    puller = Puller(
        remote,
        remote_ready_root="ready",
        remote_ack_root="control/acks",
        local_raw_root=tmp_path,
    )
    result = puller.run()
    assert result.new_chunks == 0
    assert len(result.failures) == 1
    assert f"control/acks/{manifest.chunk_id}.ack.json" not in remote.writes


def test_manifest_collector_id_cannot_escape_local_raw_root(tmp_path: Path) -> None:
    remote, manifest = _remote_fixture(b"valid parquet stand-in")
    unsafe = manifest.model_copy(update={"collector_id": "../../../escape"})
    manifest_path = "ready/date=2026-08-10/writer=depth/chunk-test.manifest.json"
    remote.files[manifest_path] = canonical_json_bytes(unsafe)
    local_raw_root = tmp_path / "safe/raw"
    puller = Puller(
        remote,
        remote_ready_root="ready",
        remote_ack_root="control/acks",
        local_raw_root=local_raw_root,
    )

    result = puller.run()

    assert len(result.failures) == 1
    assert not (tmp_path / "safe/escape").exists()
    assert f"control/acks/{manifest.chunk_id}.ack.json" not in remote.writes


def test_matching_sealed_day_is_not_rehashed_on_every_pull(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    remote, _manifest = _remote_fixture(b"valid parquet stand-in")
    puller = Puller(
        remote,
        remote_ready_root="ready",
        remote_ack_root="control/acks",
        local_raw_root=tmp_path,
    )
    first = puller.run()
    assert first.new_chunks == 1
    assert first.failures == ()
    remote.files = {
        path: content for path, content in remote.files.items() if path.endswith("/SEALED.json")
    }

    def unexpected_hash(_path: Path) -> str:
        raise AssertionError("an already-published sealed day must not be rehashed")

    monkeypatch.setattr("miry.pipeline.pull.sha256_file", unexpected_hash)

    repeated = puller.run()
    assert repeated.new_chunks == 0
    assert repeated.failures == ()


def test_rsync_transport_pins_ssh_identity_and_host_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    def record(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr("miry.pipeline.pull.subprocess.run", record)
    config = PullConfig(
        host="167.179.115.243",
        username="data-puller",
        client_key=tmp_path / "key",
        known_hosts=tmp_path / "known_hosts",
        local_raw_root=tmp_path / "raw",
        local_staging_root=tmp_path / "staging",
    )
    transport = RsyncTransport(config)
    transport.pull_ready()
    ack = tmp_path / "staging/control/acks/chunk-test.ack.json"
    ack.parent.mkdir(parents=True)
    ack.write_bytes(
        canonical_json_bytes(
            Ack(chunk_id="chunk-test", sha256=HASH, durable_at=datetime.now(UTC))
        )
    )
    journal = TransferJournal(tmp_path / "transfer-ledger")
    journal.initialize()
    result = transport.push_acks(run_id="run-test", journal=journal)

    assert len(calls) == 2
    assert "StrictHostKeyChecking=yes" in calls[0][4]
    assert f"UserKnownHostsFile={config.known_hosts}" in calls[0][4]
    assert calls[0][-2] == "data-puller@167.179.115.243:ready/"
    assert calls[1][-1] == "data-puller@167.179.115.243:control/acks/"
    assert not ack.exists()
    assert result.queued == 1
    assert result.pushed == 1
    assert result.invalid == 0
    assert b'"event":"ACK_PUSHED"' in next(
        (tmp_path / "transfer-ledger").rglob("events.jsonl")
    ).read_bytes()


def test_pull_cli_persists_success_status_and_transfer_ledger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    remote, _manifest = _remote_fixture(b"valid parquet stand-in")
    config = _pull_config(tmp_path)
    for relative, content in remote.files.items():
        destination = config.local_staging_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
    monkeypatch.setattr(
        "miry.cli.pull.load_pull_config", lambda _path: config
    )
    monkeypatch.setattr(RsyncTransport, "pull_ready", lambda _self: None)
    monkeypatch.setattr(RsyncTransport, "_run", lambda _self, *_arguments: None)
    monkeypatch.setattr(sys, "argv", ["ft-data-pull", "--config", "unused.yaml"])

    pull_main()

    status = json.loads(
        (config.local_staging_root.parent / "status/last-pull.json").read_bytes()
    )
    assert status["state"] == "ok"
    assert status["new_chunks"] == 1
    assert status["acks_queued"] == 1
    assert status["acks_pushed"] == 1
    events = _transfer_events(config.local_raw_root.parent / "transfer-ledger")
    assert {event["event"] for event in events} >= {
        "LOCAL_DURABLE",
        "ACK_PUSHED",
        "PULL_RUN_COMPLETED",
    }


def test_pull_cli_persists_failure_status_and_keeps_staged_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    remote, manifest = _remote_fixture(b"valid parquet stand-in")
    config = _pull_config(tmp_path)
    for relative, content in remote.files.items():
        destination = config.local_staging_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
    monkeypatch.setattr(
        "miry.cli.pull.load_pull_config", lambda _path: config
    )
    monkeypatch.setattr(RsyncTransport, "pull_ready", lambda _self: None)

    def fail_rsync(_self: RsyncTransport, *_arguments: str) -> None:
        raise OSError("simulated ACK upload failure")

    monkeypatch.setattr(RsyncTransport, "_run", fail_rsync)
    monkeypatch.setattr(sys, "argv", ["ft-data-pull", "--config", "unused.yaml"])

    with pytest.raises(OSError, match="simulated ACK upload failure"):
        pull_main()

    status = json.loads(
        (config.local_staging_root.parent / "status/last-pull.json").read_bytes()
    )
    assert status["state"] == "failed"
    assert "simulated ACK upload failure" in status["fatal_error"]
    assert (
        config.local_staging_root
        / f"control/acks/{manifest.chunk_id}.ack.json"
    ).exists()
    events = _transfer_events(config.local_raw_root.parent / "transfer-ledger")
    assert any(event["event"] == "PULL_RUN_FAILED" for event in events)


def _pull_config(tmp_path: Path) -> PullConfig:
    return PullConfig(
        host="167.179.115.243",
        username="data-puller",
        client_key=tmp_path / "key",
        known_hosts=tmp_path / "known_hosts",
        local_raw_root=tmp_path / "data/raw",
        local_staging_root=tmp_path / "runtime/rsync",
    )


def _transfer_events(root: Path) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for path in root.rglob("events.jsonl")
        for line in path.read_text(encoding="ascii").splitlines()
    ]


def _remote_fixture(data: bytes) -> tuple[MemoryRemote, ChunkManifest]:
    relative = "date=2026-08-10/writer=depth/chunk-test.parquet"
    manifest = ChunkManifest(
        chunk_id="chunk-test",
        data_path=relative,
        sha256=sha256_bytes(data),
        size_bytes=len(data),
        content_type=ContentType.PARQUET,
        collector_id="tokyo01",
        writer_group=WriterGroup.DEPTH,
        utc_date=date(2026, 8, 10),
        event_count=1,
        min_app_receive_realtime_ns=1,
        max_app_receive_realtime_ns=1,
        data_contract_hash=HASH,
        universe_hash=HASH,
        created_at=datetime(2026, 8, 10, tzinfo=UTC),
    )
    day = DayManifest(
        collector_id="tokyo01",
        utc_date=date(2026, 8, 10),
        sealed_at=datetime(2026, 8, 11, tzinfo=UTC),
        chunks=(manifest.as_ref(),),
    )
    files = {
        f"ready/{relative}": data,
        "ready/date=2026-08-10/writer=depth/chunk-test.manifest.json": (
            canonical_json_bytes(manifest)
        ),
        "ready/day-manifests/date=2026-08-10/SEALED.json": canonical_json_bytes(day),
    }
    return MemoryRemote(files), manifest
