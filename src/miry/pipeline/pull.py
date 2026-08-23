from __future__ import annotations

import logging
import os
import posixpath
import re
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from time import time_ns
from typing import BinaryIO, Protocol
from uuid import uuid4

from miry.contracts.models import Ack, ChunkManifest, DayManifest
from miry.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    fsync_directory,
    sha256_file,
)
from miry.pipeline.config import PullConfig
from miry.transfer import TransferJournal, transfer_event, utc_text, write_transfer_status

logger = logging.getLogger(__name__)
COLLECTOR_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,100}$")


@dataclass(frozen=True, slots=True)
class PulledChunk:
    manifest: ChunkManifest
    ack: Ack
    wrote_data: bool


@dataclass(frozen=True, slots=True)
class PullFailure:
    kind: str
    remote_path: str
    error: str


@dataclass(frozen=True, slots=True)
class PullResult:
    manifests_seen: int
    chunks: tuple[PulledChunk, ...]
    day_manifests_seen: int
    days_published: int
    failures: tuple[PullFailure, ...]

    @property
    def new_chunks(self) -> int:
        return sum(chunk.wrote_data for chunk in self.chunks)

    @property
    def existing_chunks(self) -> int:
        return len(self.chunks) - self.new_chunks

    @property
    def verified_bytes(self) -> int:
        return sum(chunk.manifest.size_bytes for chunk in self.chunks)


@dataclass(frozen=True, slots=True)
class AckPushResult:
    queued: int
    pushed: int
    invalid: int


class RemoteStore(Protocol):
    def list_files(self, root: str) -> tuple[str, ...]: ...

    def read_bytes(self, path: str) -> bytes: ...

    def download(self, remote_path: str, local_file: BinaryIO) -> None: ...

    def write_atomic(self, path: str, content: bytes) -> None: ...


class FilesystemRemoteStore:
    """Expose an rsync mirror through the same durable ingest contract used by tests."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def list_files(self, root: str) -> tuple[str, ...]:
        directory = self._path(root)
        if not directory.exists():
            return ()
        files = (
            path.relative_to(self._root).as_posix()
            for path in directory.rglob("*")
            if path.is_file()
        )
        return tuple(
            sorted(files)
        )

    def read_bytes(self, path: str) -> bytes:
        return self._path(path).read_bytes()

    def download(self, remote_path: str, local_file: BinaryIO) -> None:
        with self._path(remote_path).open("rb") as source:
            shutil.copyfileobj(source, local_file, length=1024 * 1024)

    def write_atomic(self, path: str, content: bytes) -> None:
        atomic_write_bytes(self._path(path), content)

    def _path(self, value: str) -> Path:
        relative = safe_remote_root(value)
        path = self._root / relative
        if not path.resolve(strict=False).is_relative_to(self._root.resolve()):
            raise ValueError("rsync mirror path escapes its root")
        return path


class RsyncTransport:
    def __init__(self, config: PullConfig) -> None:
        self._config = config
        self._ready_root = safe_remote_root(config.remote_ready_root)
        self._ack_root = safe_remote_root(config.remote_ack_root)
        self._mirror = config.local_staging_root

    @property
    def store(self) -> FilesystemRemoteStore:
        return FilesystemRemoteStore(self._mirror)

    def pull_ready(self) -> None:
        destination = self._mirror / self._ready_root
        destination.mkdir(parents=True, exist_ok=True)
        self._run(
            "--archive",
            "--delete-delay",
            "--delay-updates",
            "--no-links",
            "--partial",
            self._remote(f"{self._ready_root}/"),
            f"{destination}/",
        )

    def push_acks(self, *, run_id: str, journal: TransferJournal) -> AckPushResult:
        source = self._mirror / self._ack_root
        source.mkdir(parents=True, exist_ok=True)
        ack_paths = sorted(source.glob("*.ack.json"))
        if not ack_paths:
            return AckPushResult(queued=0, pushed=0, invalid=0)
        now = datetime.now(UTC)
        valid: list[tuple[Path, Ack]] = []
        rejected_events: list[dict[str, object]] = []
        rejected_root = source.parent / "rejected-acks"
        rejected_root.mkdir(parents=True, exist_ok=True)
        for path in ack_paths:
            try:
                ack = Ack.model_validate_json(path.read_bytes())
                if path.name != f"{ack.chunk_id}.ack.json":
                    raise ValueError("ACK filename does not match chunk_id")
            except (OSError, ValueError) as exc:
                destination = rejected_root / f"{path.name}.{time_ns()}.invalid.rejected"
                path.replace(destination)
                rejected_events.append(
                    transfer_event(
                        "ACK_STAGING_INVALID",
                        occurred_at=now,
                        event_id=f"{run_id}:{path.name}:ACK_STAGING_INVALID",
                        run_id=run_id,
                        path=str(path.relative_to(self._mirror)),
                        error=repr(exc)[:500],
                    )
                )
                continue
            valid.append((path, ack))
        if rejected_events:
            fsync_directory(source)
            fsync_directory(rejected_root)
            journal.append(rejected_events)
        if not valid:
            return AckPushResult(queued=len(ack_paths), pushed=0, invalid=len(ack_paths))
        self._run(
            "--archive",
            "--delay-updates",
            f"{source}/",
            self._remote(f"{self._ack_root}/"),
        )
        pushed_at = datetime.now(UTC)
        journal.append(
            transfer_event(
                "ACK_PUSHED",
                occurred_at=pushed_at,
                event_id=f"{run_id}:{ack.chunk_id}:ACK_PUSHED",
                run_id=run_id,
                chunk_id=ack.chunk_id,
                sha256=ack.sha256,
                durable_at=utc_text(ack.durable_at),
            )
            for _, ack in valid
        )
        for path, _ in valid:
            path.unlink()
        fsync_directory(source)
        return AckPushResult(
            queued=len(ack_paths),
            pushed=len(valid),
            invalid=len(ack_paths) - len(valid),
        )

    def _run(self, *arguments: str) -> None:
        command = [
            str(self._config.rsync_binary),
            "--timeout",
            str(self._config.io_timeout_seconds),
            "--rsh",
            shlex.join(self._ssh_command()),
            *arguments,
        ]
        result = subprocess.run(command, check=False, capture_output=True, text=True)
        if result.returncode != 0:
            raise OSError(
                f"rsync failed exit={result.returncode}: {result.stderr.strip()}"
            )

    def _ssh_command(self) -> list[str]:
        return [
            str(self._config.ssh_binary),
            "-p",
            str(self._config.port),
            "-i",
            str(self._config.client_key),
            "-o",
            "BatchMode=yes",
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            f"UserKnownHostsFile={self._config.known_hosts}",
            "-o",
            f"ConnectTimeout={self._config.connect_timeout_seconds}",
        ]

    def _remote(self, path: str) -> str:
        return f"{self._config.username}@{self._config.host}:{path}"


class Puller:
    def __init__(
        self,
        remote: RemoteStore,
        *,
        remote_ready_root: str,
        remote_ack_root: str,
        local_raw_root: Path,
    ) -> None:
        self._remote = remote
        self._remote_ready_root = remote_ready_root.rstrip("/")
        self._remote_ack_root = remote_ack_root.rstrip("/")
        self._local_raw_root = local_raw_root

    def run(self) -> PullResult:
        files = tuple(
            path
            for path in self._remote.list_files(self._remote_ready_root)
            if not any(part.startswith(".") for part in PurePosixPath(path).parts)
        )
        chunk_manifests = [path for path in files if path.endswith(".manifest.json")]
        day_manifests = [path for path in files if path.endswith("/SEALED.json")]
        chunks: list[PulledChunk] = []
        failures: list[PullFailure] = []
        for remote_manifest_path in chunk_manifests:
            try:
                chunks.append(self._ingest_chunk(remote_manifest_path))
            except (OSError, ValueError) as exc:
                failures.append(
                    PullFailure("chunk", remote_manifest_path, repr(exc)[:500])
                )
                logger.exception("chunk ingest failed remote_path=%s", remote_manifest_path)
        days_published = 0
        for remote_day_path in day_manifests:
            try:
                days_published += self._publish_complete_day(remote_day_path)
            except (OSError, ValueError) as exc:
                failures.append(PullFailure("day", remote_day_path, repr(exc)[:500]))
                logger.exception("day manifest ingest failed remote_path=%s", remote_day_path)
        return PullResult(
            manifests_seen=len(chunk_manifests),
            chunks=tuple(chunks),
            day_manifests_seen=len(day_manifests),
            days_published=days_published,
            failures=tuple(failures),
        )

    def _ingest_chunk(self, remote_manifest_path: str) -> PulledChunk:
        manifest_bytes = self._remote.read_bytes(remote_manifest_path)
        manifest = ChunkManifest.model_validate_json(manifest_bytes)
        collector_root = self._collector_root(manifest.collector_id)
        destination = collector_root / manifest.data_path
        if destination.exists():
            self._verify_local(destination, manifest)
            wrote_data = False
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            partial = destination.with_name(f".{destination.name}.partial")
            with partial.open("wb") as output:
                self._remote.download(
                    posixpath.join(self._remote_ready_root, manifest.data_path), output
                )
                output.flush()
                os.fsync(output.fileno())
            self._verify_local(partial, manifest)
            partial.replace(destination)
            fsync_directory(destination.parent)
            wrote_data = True

        local_manifest = destination.with_suffix(".manifest.json")
        if local_manifest.exists() and local_manifest.read_bytes() != manifest_bytes:
            raise ValueError(f"conflicting local manifest: {local_manifest}")
        if not local_manifest.exists():
            atomic_write_bytes(local_manifest, manifest_bytes)

        ack = Ack(
            chunk_id=manifest.chunk_id,
            sha256=manifest.sha256,
            durable_at=datetime.now(UTC),
        )
        ack_path = posixpath.join(self._remote_ack_root, f"{manifest.chunk_id}.ack.json")
        self._remote.write_atomic(ack_path, canonical_json_bytes(ack))
        return PulledChunk(manifest=manifest, ack=ack, wrote_data=wrote_data)

    def _publish_complete_day(self, remote_day_path: str) -> int:
        content = self._remote.read_bytes(remote_day_path)
        manifest = DayManifest.model_validate_json(content)
        collector_root = self._collector_root(manifest.collector_id)
        destination = (
            collector_root
            / "day-manifests"
            / f"date={manifest.utc_date.isoformat()}"
            / "SEALED.json"
        )
        if destination.exists():
            if destination.read_bytes() != content:
                raise ValueError(f"conflicting sealed day manifest: {destination}")
            return 0
        for chunk in manifest.chunks:
            path = collector_root / chunk.data_path
            if not path.exists() or path.stat().st_size != chunk.size_bytes:
                return 0
            if sha256_file(path) != chunk.sha256:
                raise ValueError(f"day manifest hash mismatch: {path}")
        atomic_write_bytes(destination, content)
        return 1

    @staticmethod
    def _verify_local(path: Path, manifest: ChunkManifest) -> None:
        if path.stat().st_size != manifest.size_bytes:
            raise ValueError(f"chunk size mismatch: {path}")
        if sha256_file(path) != manifest.sha256:
            raise ValueError(f"chunk hash mismatch: {path}")

    def _collector_root(self, collector_id: str) -> Path:
        if not COLLECTOR_ID_PATTERN.fullmatch(collector_id):
            raise ValueError(f"unsafe collector_id: {collector_id!r}")
        return self._local_raw_root / f"collector={collector_id}"


def safe_remote_root(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("remote paths must be relative to the restricted rsync root")
    return value.rstrip("/")


def run_pull(config: PullConfig) -> int:
    started_at = datetime.now(UTC)
    started_monotonic = time.monotonic()
    run_id = f"{started_at.strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:12]}"
    journal = TransferJournal(config.local_raw_root.parent / "transfer-ledger")
    status_path = config.local_staging_root.parent / "status" / "last-pull.json"
    journal.initialize()
    transport = RsyncTransport(config)
    try:
        transport.pull_ready()
        result = Puller(
            transport.store,
            remote_ready_root=safe_remote_root(config.remote_ready_root),
            remote_ack_root=safe_remote_root(config.remote_ack_root),
            local_raw_root=config.local_raw_root,
        ).run()
        journal.append(
            transfer_event(
                "LOCAL_DURABLE",
                occurred_at=chunk.ack.durable_at,
                event_id=f"{run_id}:{chunk.manifest.chunk_id}:LOCAL_DURABLE",
                run_id=run_id,
                collector_id=chunk.manifest.collector_id,
                chunk_id=chunk.manifest.chunk_id,
                sha256=chunk.manifest.sha256,
                size_bytes=chunk.manifest.size_bytes,
                utc_date=chunk.manifest.utc_date.isoformat(),
            )
            for chunk in result.chunks
            if chunk.wrote_data
        )
        ack_result = transport.push_acks(run_id=run_id, journal=journal)
        completed_at = datetime.now(UTC)
        failures = len(result.failures) + ack_result.invalid
        duration_seconds = round(time.monotonic() - started_monotonic, 3)
        status = {
            "run_id": run_id,
            "state": "ok" if failures == 0 else "failed",
            "started_at": utc_text(started_at),
            "completed_at": utc_text(completed_at),
            "duration_seconds": duration_seconds,
            "remote_manifests": result.manifests_seen,
            "new_chunks": result.new_chunks,
            "existing_chunks_verified": result.existing_chunks,
            "verified_bytes": result.verified_bytes,
            "acks_queued": ack_result.queued,
            "acks_pushed": ack_result.pushed,
            "invalid_staged_acks": ack_result.invalid,
            "day_manifests_seen": result.day_manifests_seen,
            "days_published": result.days_published,
            "failures": failures,
            "ingest_failures": len(result.failures),
            "failure_details": [
                {
                    "kind": failure.kind,
                    "remote_path": failure.remote_path,
                    "error": failure.error,
                }
                for failure in result.failures[:20]
            ],
        }
        journal.append(
            (
                transfer_event(
                    "PULL_RUN_COMPLETED" if failures == 0 else "PULL_RUN_FAILED",
                    occurred_at=completed_at,
                    event_id=f"{run_id}:PULL_RUN",
                    **status,
                ),
            )
        )
        write_transfer_status(status_path, status)
    except Exception as error:
        _record_pull_failure(
            error,
            run_id=run_id,
            started_at=started_at,
            started_monotonic=started_monotonic,
            journal=journal,
            status_path=status_path,
        )
        raise

    logger.info(
        "pull %s run_id=%s remote_manifests=%d new_chunks=%d existing_verified=%d "
        "verified_bytes=%d acks_queued=%d acks_pushed=%d failures=%d duration_seconds=%.3f",
        "complete" if failures == 0 else "failed",
        run_id,
        result.manifests_seen,
        result.new_chunks,
        result.existing_chunks,
        result.verified_bytes,
        ack_result.queued,
        ack_result.pushed,
        failures,
        duration_seconds,
    )
    return failures


def _record_pull_failure(
    error: Exception,
    *,
    run_id: str,
    started_at: datetime,
    started_monotonic: float,
    journal: TransferJournal,
    status_path: Path,
) -> None:
    failed_at = datetime.now(UTC)
    failure_status = {
        "run_id": run_id,
        "state": "failed",
        "started_at": utc_text(started_at),
        "completed_at": utc_text(failed_at),
        "duration_seconds": round(time.monotonic() - started_monotonic, 3),
        "fatal_error": repr(error)[:1000],
    }
    try:
        journal.append(
            (
                transfer_event(
                    "PULL_RUN_FAILED",
                    occurred_at=failed_at,
                    event_id=f"{run_id}:PULL_RUN",
                    **failure_status,
                ),
            )
        )
        write_transfer_status(status_path, failure_status)
    except Exception:
        logger.exception("failed to persist pull failure status run_id=%s", run_id)
    logger.exception("pull failed run_id=%s", run_id)
