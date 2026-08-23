#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import tarfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath
from typing import Any

REF_KEYS = ("chunk_id", "content_type", "data_path", "sha256", "size_bytes")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recover a sealed day manifest from an archived durable day index"
    )
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--archive-member", required=True)
    parser.add_argument("--raw-collector-root", type=Path, required=True)
    parser.add_argument("--collector", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--sealed-at", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    if args.workers < 1:
        raise ValueError("workers must be positive")
    refs = load_refs(args.archive, args.archive_member)
    if not refs:
        raise ValueError("archived day index is empty")

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        list(
            pool.map(
                lambda ref: verify_ref(
                    ref,
                    raw_collector_root=args.raw_collector_root,
                    collector=args.collector,
                    utc_date=args.date,
                ),
                refs,
            )
        )

    manifest = {
        "schema_version": 1,
        "collector_id": args.collector,
        "utc_date": args.date,
        "sealed_at": args.sealed_at,
        "chunks": refs,
    }
    content = canonical_json(manifest)
    if args.output.exists():
        if args.output.read_bytes() != content:
            raise FileExistsError(f"refusing to replace different manifest: {args.output}")
        print(f"manifest already matches chunks={len(refs)} output={args.output}")
        return
    atomic_write(args.output, content)
    print(f"recovered sealed manifest chunks={len(refs)} output={args.output}")


def load_refs(archive: Path, member_name: str) -> list[dict[str, Any]]:
    by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    with tarfile.open(archive, "r:gz") as bundle:
        member = bundle.getmember(member_name)
        source = bundle.extractfile(member)
        if source is None:
            raise FileNotFoundError(f"archive member is not a file: {member_name}")
        for line_number, line in enumerate(source, start=1):
            raw = json.loads(line)
            ref = {key: raw[key] for key in REF_KEYS}
            identity = (str(ref["chunk_id"]), str(ref["sha256"]))
            existing = by_identity.get(identity)
            if existing is not None and existing != ref:
                raise ValueError(f"conflicting day-index row at line {line_number}")
            by_identity[identity] = ref
    return [by_identity[key] for key in sorted(by_identity)]


def verify_ref(
    ref: dict[str, Any], *, raw_collector_root: Path, collector: str, utc_date: str
) -> None:
    relative = PurePosixPath(str(ref["data_path"]))
    expected_parent = PurePosixPath(f"date={utc_date}")
    if relative.is_absolute() or ".." in relative.parts or expected_parent not in relative.parents:
        raise ValueError(f"unsafe or wrong-date data path: {relative}")
    data_path = raw_collector_root.joinpath(*relative.parts)
    if not data_path.is_file():
        raise FileNotFoundError(f"missing data file: {data_path}")
    if data_path.stat().st_size != int(ref["size_bytes"]):
        raise ValueError(f"size mismatch: {data_path}")
    if sha256_file(data_path) != ref["sha256"]:
        raise ValueError(f"SHA-256 mismatch: {data_path}")

    manifest_path = data_path.with_suffix(".manifest.json")
    manifest = json.loads(manifest_path.read_bytes())
    manifest_ref = {key: manifest[key] for key in REF_KEYS}
    if manifest_ref != ref:
        raise ValueError(f"chunk manifest disagrees with day index: {manifest_path}")
    if manifest.get("collector_id") != collector or manifest.get("utc_date") != utc_date:
        raise ValueError(f"chunk manifest identity mismatch: {manifest_path}")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_json(value: dict[str, Any]) -> bytes:
    serialized = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return f"{serialized}\n".encode()


def atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.partial")
    with temporary.open("xb") as destination:
        destination.write(content)
        destination.flush()
        os.fsync(destination.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


if __name__ == "__main__":
    main()
