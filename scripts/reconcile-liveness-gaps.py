"""Audited derived-only repair of orphaned scoped liveness gaps; raw is immutable."""

from __future__ import annotations

import argparse
import json
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from miry.contracts.serde import atomic_write_bytes, canonical_json_bytes, sha256_file

COLUMNS = (
    "exchange_symbol",
    "stream_type",
    "app_receive_realtime_ns",
    "connection_id",
    "receive_seq",
    "payload_hash",
)


def candidates(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    continued = {
        event["gap_id"]
        for event in events
        if event["state"] == "CLOSED"
        and event.get("detail") == "gap continues into the next UTC day"
    }
    closed = {
        event["gap_id"]
        for event in events
        if event["state"] == "CLOSED"
        and event.get("detail") != "gap continues into the next UTC day"
    }
    return {
        event["gap_id"]: event
        for event in events
        if event["state"] == "OPEN"
        and event["gap_id"] in continued - closed
        and event.get("reason") == "CONNECTION_LOST_GAP"
        and event.get("connection_id") is None
        and len(event.get("exchange_symbols", [])) == 1
        and len(event.get("stream_types", [])) == 1
        and re.fullmatch(
            r"[^:]+: no (depth|book_ticker|mark_price) event for [0-9.]+s",
            event.get("detail", ""),
        )
        and f"no {event['stream_types'][0]} event" in event["detail"]
    }


def find_evidence(typed: Path, opened: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    pending = dict(opened)
    evidence = {}
    for path in sorted(typed.glob("*.typed.parquet")):
        if not pending:
            break
        parquet = pq.ParquetFile(path)
        time_index = parquet.schema_arrow.get_field_index("app_receive_realtime_ns")
        floor = min(e["observed_at_realtime_ns"] for e in pending.values())
        groups = []
        for index in range(parquet.num_row_groups):
            stats = parquet.metadata.row_group(index).column(time_index).statistics
            if stats is None or not stats.has_min_max or stats.max > floor:
                groups.append(index)
        if not groups:
            continue
        file_hash = None
        for batch in parquet.iter_batches(batch_size=8192, row_groups=groups, columns=COLUMNS):
            for row in batch.to_pylist():
                for gap_id, event in tuple(pending.items()):
                    if (
                        row["exchange_symbol"] == event["exchange_symbols"][0]
                        and row["stream_type"] == event["stream_types"][0]
                        and row["app_receive_realtime_ns"] > event["observed_at_realtime_ns"]
                    ):
                        if file_hash is None:
                            file_hash = sha256_file(path)
                        evidence[gap_id] = {
                            **row,
                            "payload_hash": row["payload_hash"].hex(),
                            "typed_file": str(path),
                            "typed_sha256": file_hash,
                        }
                        del pending[gap_id]
                if not pending:
                    break
            if not pending:
                break
    return evidence


def reconcile(
    events: list[dict[str, Any]],
    opened: dict[str, dict[str, Any]],
    evidence: dict[str, dict[str, Any]],
    utc_date: date,
) -> list[dict[str, Any]]:
    result = []
    for event in events:
        proof = evidence.get(event["gap_id"])
        if proof is not None:
            event_ns = (
                event.get("affected_from_realtime_ns") or event["observed_at_realtime_ns"]
                if event["state"] == "OPEN"
                else event["observed_at_realtime_ns"]
            )
            if event_ns >= proof["app_receive_realtime_ns"]:
                continue
        result.append(event)
    for gap_id, proof in evidence.items():
        recovered_ns = proof["app_receive_realtime_ns"]
        if datetime.fromtimestamp(recovered_ns // 10**9, UTC).date() == utc_date:
            result.append(
                {
                    **opened[gap_id],
                    "state": "CLOSED",
                    "affected_from_realtime_ns": None,
                    "observed_at_realtime_ns": recovered_ns,
                    "detail": "liveness recovery proven by typed event; see liveness-recovery.json",
                }
            )
    return sorted(result, key=lambda e: (e["observed_at_realtime_ns"], e["gap_id"], e["state"]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--derived-root", type=Path, required=True)
    parser.add_argument("--collector", required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--through", type=date.fromisoformat, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9._-]+", args.collector) or args.start > args.through:
        parser.error("invalid collector or date range")
    quality = args.derived_root / "quality" / f"collector={args.collector}"
    first = quality / f"date={args.start}"
    original = first / "transport-gaps.unreconciled.jsonl"
    source = original if original.exists() else first / "transport-gaps.jsonl"
    opened = candidates([json.loads(line) for line in source.read_bytes().splitlines()])
    if not opened:
        raise ValueError("no orphaned scoped liveness gaps proven on the starting day")
    typed = args.derived_root / "typed" / f"collector={args.collector}" / f"date={args.start}"
    proof = find_evidence(typed, opened)
    if set(proof) != set(opened):
        raise ValueError(
            f"no post-open event for {sorted(set(opened) - set(proof))}; no files changed"
        )
    report = {
        "schema_version": 1,
        "policy": "scoped-liveness-recovery-by-observed-event",
        "collector_id": args.collector,
        "origin_date": args.start.isoformat(),
        "origin_gap_sha256": sha256_file(source),
        "normalized_sha256": sha256_file(typed / "_NORMALIZED.json"),
        "evidence": proof,
    }
    # Validate every destination before mutating any derived output.
    plans = []
    day = args.start
    while day <= args.through:
        root = quality / f"date={day}"
        gap_path = root / "transport-gaps.jsonl"
        backup = root / "transport-gaps.unreconciled.jsonl"
        content = (backup if backup.exists() else gap_path).read_bytes()
        events = [json.loads(line) for line in content.splitlines()]
        changed = reconcile(events, opened, proof, day)
        plans.append((root, gap_path, backup, content, changed))
        day += timedelta(days=1)
    print(json.dumps({"gaps_proven": len(proof), "days": len(plans), "apply": args.apply}))
    for root, gap_path, backup, content, changed in plans:
        if not args.apply:
            continue
        report_path = root / "liveness-recovery.json"
        if report_path.exists():
            previous = json.loads(report_path.read_bytes())
            if previous["evidence"] != proof:
                raise ValueError(f"recovery evidence changed: {report_path}")
        if not backup.exists():
            atomic_write_bytes(backup, content)
        atomic_write_bytes(report_path, canonical_json_bytes(report))
        atomic_write_bytes(gap_path, b"".join(canonical_json_bytes(e) + b"\n" for e in changed))
        # Preserve old results and force reconstruction, without re-normalizing raw.
        archive = root / "before-liveness-recovery"
        archive.mkdir(exist_ok=True)
        outputs = [
            *root.glob("symbol=*"),
            root / "_PROCESSED.json",
            root / "_QUALITY_REJECTED.json",
        ]
        for path in outputs:
            if path.exists() and not (archive / path.name).exists():
                path.rename(archive / path.name)
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
