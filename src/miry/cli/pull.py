from __future__ import annotations

import argparse
import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from miry.pipeline.config import load_pull_config
from miry.pipeline.pull import Puller, RsyncTransport, safe_remote_root
from miry.transfer import (
    TransferJournal,
    transfer_event,
    utc_text,
    write_transfer_status,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull durable raw chunks from the collector")
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = load_pull_config(args.config)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    started_at = datetime.now(UTC)
    started_monotonic = time.monotonic()
    run_id = f"{started_at.strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:12]}"
    journal = TransferJournal(config.local_raw_root.parent / "transfer-ledger")
    status_path = config.local_staging_root.parent / "status" / "last-pull.json"
    journal.initialize()
    transport = RsyncTransport(config)
    try:
        transport.pull_ready()
        puller = Puller(
            transport.store,
            remote_ready_root=safe_remote_root(config.remote_ready_root),
            remote_ack_root=safe_remote_root(config.remote_ack_root),
            local_raw_root=config.local_raw_root,
        )
        result = puller.run()
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
    except Exception as exc:
        failed_at = datetime.now(UTC)
        failure_status = {
            "run_id": run_id,
            "state": "failed",
            "started_at": utc_text(started_at),
            "completed_at": utc_text(failed_at),
            "duration_seconds": round(time.monotonic() - started_monotonic, 3),
            "fatal_error": repr(exc)[:1000],
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
            logging.exception("failed to persist pull failure status run_id=%s", run_id)
        logging.exception("pull failed run_id=%s", run_id)
        raise

    logging.info(
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
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
