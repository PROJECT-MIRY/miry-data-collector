from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

import orjson
from pydantic import BaseModel


def canonical_json_bytes(value: BaseModel | dict[str, Any]) -> bytes:
    data = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    return orjson.dumps(data, option=orjson.OPT_SORT_KEYS | orjson.OPT_APPEND_NEWLINE)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def universe_hash(
    core: tuple[str, ...], boundary: tuple[str, ...], probe: tuple[str, ...]
) -> str:
    return sha256_bytes(
        orjson.dumps(
            {"boundary": list(boundary), "core": list(core), "probe": list(probe)},
            option=orjson.OPT_SORT_KEYS,
        )
    )


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: Path, content: bytes, *, mode: int = 0o640) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.partial")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as destination:
            destination.write(content)
            destination.flush()
            os.fsync(destination.fileno())
    finally:
        os.close(descriptor)
    temporary.replace(path)
    fsync_directory(path.parent)
