from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import time_ns
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, TypeAdapter

from miry.contracts.models import Ack, ChunkManifest
from miry.contracts.serde import (
    atomic_write_bytes,
    canonical_json_bytes,
    fsync_directory,
)
from miry.transfer import (
    TransferJournal,
    transfer_event,
    utc_text,
    write_transfer_status,
)

logger = logging.getLogger(__name__)
ACK_ADAPTER = TypeAdapter(Ack)
MANIFEST_ADAPTER = TypeAdapter(ChunkManifest)


class AckTransaction(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    ack: Ack
    manifest: ChunkManifest
    accepted_at: datetime


TRANSACTION_ADAPTER = TypeAdapter(AckTransaction)


@dataclass(frozen=True, slots=True)
class SpoolStatus:
    used_bytes: int
    free_bytes: int
    hard_limited: bool


@dataclass(frozen=True, slots=True)
class AckApplyResult:
    seen: int = 0
    applied: int = 0
    replayed: int = 0
    gc_bytes: int = 0
    invalid: int = 0
    hash_mismatches: int = 0
    unknown: int = 0
    manifest_errors: int = 0
    recovered: int = 0


class SpoolManager:
    def __init__(self, data_root: Path, *, max_bytes: int, minimum_free_bytes: int) -> None:
        self.data_root = data_root
        self.ready_root = data_root / "ready"
        self.ack_root = data_root / "control" / "acks"
        self.applying_root = data_root / "control" / "applying-acks"
        self.rejected_root = data_root / "control" / "rejected-acks"
        self.transfer_status_path = data_root / "control" / "transfer-status.json"
        self.transfer_journal = TransferJournal(data_root / "control" / "transfer-ledger")
        self.max_bytes = max_bytes
        self.minimum_free_bytes = minimum_free_bytes

    def initialize(self) -> None:
        for path in (
            self.ready_root,
            self.ack_root,
            self.applying_root,
            self.rejected_root,
            self.data_root / "writing",
        ):
            path.mkdir(parents=True, exist_ok=True)
        self.transfer_journal.initialize()

    def status(self) -> SpoolStatus:
        used = _tree_size(self.data_root / "ready") + _tree_size(self.data_root / "writing")
        free = shutil.disk_usage(self.data_root).free
        return SpoolStatus(
            used_bytes=used,
            free_bytes=free,
            hard_limited=used >= self.max_bytes or free < self.minimum_free_bytes,
        )

    def apply_acks(self) -> AckApplyResult:
        ack_paths = sorted(self.ack_root.glob("*.ack.json"))
        transaction_paths = sorted(self.applying_root.glob("*.transaction.json"))
        if not ack_paths and not transaction_paths:
            return AckApplyResult()

        apply_id = uuid4().hex
        now = datetime.now(UTC)
        events: list[dict[str, object]] = []
        manifests, manifest_errors = self._ready_manifests(apply_id, now, events)
        invalid = 0
        mismatches = 0
        unknown = 0
        replayed = 0
        for ack_path in ack_paths:
            try:
                ack = ACK_ADAPTER.validate_json(ack_path.read_bytes())
            except (OSError, ValueError) as exc:
                invalid += 1
                events.append(
                    self._rejection_event(
                        "ACK_INVALID", apply_id, ack_path, now, error=repr(exc)
                    )
                )
                self._quarantine(ack_path, "invalid")
                continue
            if ack_path.name != f"{ack.chunk_id}.ack.json":
                invalid += 1
                events.append(
                    self._rejection_event(
                        "ACK_INVALID",
                        apply_id,
                        ack_path,
                        now,
                        chunk_id=ack.chunk_id,
                        error="ACK filename does not match chunk_id",
                    )
                )
                self._quarantine(ack_path, "invalid-name")
                continue
            item = manifests.get(ack.chunk_id)
            if item is None:
                known_manifest, known_manifest_errors = self._acked_manifest(
                    ack.chunk_id, apply_id, now, events
                )
                manifest_errors += known_manifest_errors
                if known_manifest is not None:
                    if ack.sha256 != known_manifest.sha256:
                        mismatches += 1
                        events.append(
                            self._rejection_event(
                                "ACK_HASH_MISMATCH",
                                apply_id,
                                ack_path,
                                now,
                                chunk_id=ack.chunk_id,
                                expected_sha256=known_manifest.sha256,
                                received_sha256=ack.sha256,
                            )
                        )
                        self._quarantine(ack_path, "hash-mismatch")
                        continue
                    replayed += 1
                    events.append(
                        transfer_event(
                            "ACK_REPLAYED",
                            occurred_at=now,
                            event_id=f"{apply_id}:{ack.chunk_id}:ACK_REPLAYED",
                            apply_id=apply_id,
                            collector_id=known_manifest.collector_id,
                            chunk_id=ack.chunk_id,
                            sha256=ack.sha256,
                            ack_durable_at=utc_text(ack.durable_at),
                        )
                    )
                    ack_path.unlink()
                    continue
                unknown += 1
                events.append(
                    self._rejection_event(
                        "ACK_UNKNOWN", apply_id, ack_path, now, chunk_id=ack.chunk_id
                    )
                )
                self._quarantine(ack_path, "unknown")
                continue
            _manifest_path, manifest = item
            if ack.sha256 != manifest.sha256:
                mismatches += 1
                logger.error("quarantining mismatched ACK for chunk_id=%s", ack.chunk_id)
                events.append(
                    self._rejection_event(
                        "ACK_HASH_MISMATCH",
                        apply_id,
                        ack_path,
                        now,
                        chunk_id=ack.chunk_id,
                        expected_sha256=manifest.sha256,
                        received_sha256=ack.sha256,
                    )
                )
                self._quarantine(ack_path, "hash-mismatch")
                continue
            transaction = AckTransaction(ack=ack, manifest=manifest, accepted_at=now)
            transaction_path = self.applying_root / f"{ack.chunk_id}.transaction.json"
            content = canonical_json_bytes(transaction)
            if transaction_path.exists():
                try:
                    existing_transaction = TRANSACTION_ADAPTER.validate_json(
                        transaction_path.read_bytes()
                    )
                except (OSError, ValueError):
                    existing_transaction = None
                if not (
                    existing_transaction is not None
                    and existing_transaction.manifest == manifest
                    and existing_transaction.ack.chunk_id == ack.chunk_id
                    and existing_transaction.ack.sha256 == ack.sha256
                ):
                    invalid += 1
                    events.append(
                        self._rejection_event(
                            "ACK_TRANSACTION_CONFLICT",
                            apply_id,
                            ack_path,
                            now,
                            chunk_id=ack.chunk_id,
                        )
                    )
                    self._quarantine(ack_path, "transaction-conflict")
                    continue
            else:
                atomic_write_bytes(transaction_path, content)
            ack_path.unlink()
            manifests.pop(ack.chunk_id, None)
            transaction_paths.append(transaction_path)

        if ack_paths:
            fsync_directory(self.ack_root)

        applied = 0
        gc_bytes = 0
        recovered = 0
        completed_transactions: list[Path] = []
        for transaction_path in sorted(set(transaction_paths)):
            try:
                transaction = TRANSACTION_ADAPTER.validate_json(transaction_path.read_bytes())
            except (OSError, ValueError) as exc:
                invalid += 1
                events.append(
                    self._rejection_event(
                        "ACK_TRANSACTION_INVALID",
                        apply_id,
                        transaction_path,
                        now,
                        error=repr(exc),
                    )
                )
                self._quarantine(transaction_path, "invalid-transaction")
                continue
            ack = transaction.ack
            manifest = transaction.manifest
            if ack.sha256 != manifest.sha256:
                mismatches += 1
                events.append(
                    self._rejection_event(
                        "ACK_TRANSACTION_HASH_MISMATCH",
                        apply_id,
                        transaction_path,
                        now,
                        chunk_id=ack.chunk_id,
                        expected_sha256=manifest.sha256,
                        received_sha256=ack.sha256,
                    )
                )
                self._quarantine(transaction_path, "transaction-hash-mismatch")
                continue

            data_path = self.ready_root / manifest.data_path
            manifest_path = data_path.with_suffix(".manifest.json")
            data_existed = data_path.exists()
            manifest_existed = manifest_path.exists()
            sealed = (
                self.ready_root
                / "day-manifests"
                / f"date={manifest.utc_date.isoformat()}"
                / "SEALED.json"
            )
            if manifest_path.exists() and sealed.exists():
                manifest_path.unlink()
                fsync_directory(manifest_path.parent)
            elif manifest_path.exists():
                acked_manifest = (
                    self.data_root
                    / "control"
                    / "acked-manifests"
                    / f"date={manifest.utc_date.isoformat()}"
                    / manifest_path.name
                )
                acked_manifest.parent.mkdir(parents=True, exist_ok=True)
                manifest_path.replace(acked_manifest)
                fsync_directory(manifest_path.parent)
                fsync_directory(acked_manifest.parent)
            if data_path.exists():
                data_path.unlink()
                fsync_directory(data_path.parent)

            was_recovered = not data_existed or not manifest_existed
            recovered += int(was_recovered)
            event_fields = {
                "apply_id": apply_id,
                "collector_id": manifest.collector_id,
                "chunk_id": manifest.chunk_id,
                "sha256": manifest.sha256,
                "size_bytes": manifest.size_bytes,
                "utc_date": manifest.utc_date.isoformat(),
                "ack_durable_at": utc_text(ack.durable_at),
            }
            identity = f"{manifest.chunk_id}:{utc_text(ack.durable_at)}"
            events.extend(
                (
                    transfer_event(
                        "ACK_VALIDATED",
                        occurred_at=now,
                        event_id=f"{identity}:ACK_VALIDATED",
                        **event_fields,
                    ),
                    transfer_event(
                        "REMOTE_GC",
                        occurred_at=now,
                        event_id=f"{identity}:REMOTE_GC",
                        recovered=was_recovered,
                        **event_fields,
                    ),
                )
            )
            applied += 1
            gc_bytes += manifest.size_bytes
            completed_transactions.append(transaction_path)

        result = AckApplyResult(
            seen=len(ack_paths),
            applied=applied,
            replayed=replayed,
            gc_bytes=gc_bytes,
            invalid=invalid,
            hash_mismatches=mismatches,
            unknown=unknown,
            manifest_errors=manifest_errors,
            recovered=recovered,
        )
        self.transfer_journal.append(events)
        write_transfer_status(
            self.transfer_status_path,
            {
                "apply_id": apply_id,
                "updated_at": utc_text(now),
                "state": "ok"
                if not (invalid or mismatches or unknown or manifest_errors)
                else "attention",
                "acks_seen": result.seen,
                "acks_applied": result.applied,
                "acks_replayed": result.replayed,
                "gc_bytes": result.gc_bytes,
                "invalid_acks": result.invalid,
                "hash_mismatches": result.hash_mismatches,
                "unknown_acks": result.unknown,
                "manifest_errors": result.manifest_errors,
                "recovered_transactions": result.recovered,
                "acks_pending": sum(1 for _ in self.ack_root.glob("*.ack.json")),
                "transactions_pending": max(
                    0,
                    sum(1 for _ in self.applying_root.glob("*.transaction.json"))
                    - len(completed_transactions),
                ),
                "ready_manifests_remaining": sum(
                    1 for _ in self.ready_root.rglob("*.manifest.json")
                ),
            },
        )
        for transaction_path in completed_transactions:
            transaction_path.unlink(missing_ok=True)
        if completed_transactions:
            fsync_directory(self.applying_root)
        return result

    def _acked_manifest(
        self,
        chunk_id: str,
        apply_id: str,
        now: datetime,
        events: list[dict[str, object]],
    ) -> tuple[ChunkManifest | None, int]:
        root = self.data_root / "control" / "acked-manifests"
        manifest: ChunkManifest | None = None
        errors = 0
        for path in sorted(root.glob(f"date=*/{chunk_id}.manifest.json")):
            try:
                candidate = MANIFEST_ADAPTER.validate_json(path.read_bytes())
            except (OSError, ValueError) as exc:
                errors += 1
                events.append(
                    transfer_event(
                        "ACKED_MANIFEST_INVALID",
                        occurred_at=now,
                        event_id=f"{apply_id}:{path.name}:ACKED_MANIFEST_INVALID",
                        apply_id=apply_id,
                        path=str(path.relative_to(self.data_root)),
                        error=repr(exc)[:500],
                    )
                )
                continue
            expected_path = (
                root
                / f"date={candidate.utc_date.isoformat()}"
                / Path(candidate.data_path).with_suffix(".manifest.json").name
            )
            if path != expected_path:
                errors += 1
                events.append(
                    transfer_event(
                        "ACKED_MANIFEST_MISPLACED",
                        occurred_at=now,
                        event_id=f"{apply_id}:{path.name}:ACKED_MANIFEST_MISPLACED",
                        apply_id=apply_id,
                        chunk_id=candidate.chunk_id,
                        path=str(path.relative_to(self.data_root)),
                        expected_path=str(expected_path.relative_to(self.data_root)),
                    )
                )
                continue
            if candidate.chunk_id != chunk_id:
                errors += 1
                events.append(
                    transfer_event(
                        "ACKED_MANIFEST_ID_MISMATCH",
                        occurred_at=now,
                        event_id=f"{apply_id}:{path.name}:ACKED_MANIFEST_ID_MISMATCH",
                        apply_id=apply_id,
                        expected_chunk_id=chunk_id,
                        received_chunk_id=candidate.chunk_id,
                        path=str(path.relative_to(self.data_root)),
                    )
                )
                continue
            if manifest is not None and manifest != candidate:
                errors += 1
                manifest = None
                events.append(
                    transfer_event(
                        "ACKED_MANIFEST_CONFLICT",
                        occurred_at=now,
                        event_id=f"{apply_id}:{chunk_id}:ACKED_MANIFEST_CONFLICT",
                        apply_id=apply_id,
                        chunk_id=chunk_id,
                    )
                )
                return None, errors
            manifest = candidate
        return manifest, errors

    def _ready_manifests(
        self,
        apply_id: str,
        now: datetime,
        events: list[dict[str, object]],
    ) -> tuple[dict[str, tuple[Path, ChunkManifest]], int]:
        manifests: dict[str, tuple[Path, ChunkManifest]] = {}
        conflicted_ids: set[str] = set()
        errors = 0
        for path in self.ready_root.rglob("*.manifest.json"):
            try:
                manifest = MANIFEST_ADAPTER.validate_json(path.read_bytes())
            except (OSError, ValueError) as exc:
                errors += 1
                logger.error("ignoring unreadable ready manifest path=%s error=%r", path, exc)
                events.append(
                    transfer_event(
                        "READY_MANIFEST_INVALID",
                        occurred_at=now,
                        event_id=f"{apply_id}:{path.name}:READY_MANIFEST_INVALID",
                        apply_id=apply_id,
                        path=str(path.relative_to(self.data_root)),
                        error=repr(exc)[:500],
                    )
                )
                continue
            expected_path = (self.ready_root / manifest.data_path).with_suffix(".manifest.json")
            if path != expected_path:
                errors += 1
                logger.error(
                    "ignoring misplaced ready manifest path=%s expected=%s", path, expected_path
                )
                events.append(
                    transfer_event(
                        "READY_MANIFEST_MISPLACED",
                        occurred_at=now,
                        event_id=f"{apply_id}:{path.name}:READY_MANIFEST_MISPLACED",
                        apply_id=apply_id,
                        chunk_id=manifest.chunk_id,
                        path=str(path.relative_to(self.data_root)),
                        expected_path=str(expected_path.relative_to(self.data_root)),
                    )
                )
                continue
            if manifest.chunk_id in conflicted_ids:
                continue
            existing = manifests.get(manifest.chunk_id)
            if existing is not None and existing[1] != manifest:
                errors += 1
                logger.error("ignoring conflicting ready manifests chunk_id=%s", manifest.chunk_id)
                manifests.pop(manifest.chunk_id, None)
                conflicted_ids.add(manifest.chunk_id)
                events.append(
                    transfer_event(
                        "READY_MANIFEST_CONFLICT",
                        occurred_at=now,
                        event_id=f"{apply_id}:{manifest.chunk_id}:READY_MANIFEST_CONFLICT",
                        apply_id=apply_id,
                        chunk_id=manifest.chunk_id,
                    )
                )
                continue
            manifests[manifest.chunk_id] = (path, manifest)
        return manifests, errors

    def _rejection_event(
        self,
        event: str,
        apply_id: str,
        path: Path,
        now: datetime,
        **fields: object,
    ) -> dict[str, object]:
        return transfer_event(
            event,
            occurred_at=now,
            event_id=f"{apply_id}:{path.name}:{event}",
            apply_id=apply_id,
            path=str(path.relative_to(self.data_root)),
            **fields,
        )

    def _quarantine(self, path: Path, reason: str) -> None:
        destination = self.rejected_root / f"{path.name}.{time_ns()}.{reason}.rejected"
        path.replace(destination)
        fsync_directory(path.parent)
        fsync_directory(destination.parent)


def _tree_size(root: Path) -> int:
    if not root.exists():
        return 0
    total = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        if entry.is_symlink():
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            total += entry.stat(follow_symlinks=False).st_size
                    except FileNotFoundError:
                        continue
        except FileNotFoundError:
            continue
    return total
