from __future__ import annotations

import hashlib
import re
from pathlib import Path, PurePosixPath
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from miry.contracts.serde import canonical_json_bytes, sha256_file
from miry.contracts.typed import TYPED_EVENT_SCHEMA

L2_SYMBOL_PROJECTION_SCHEMA_ID = "miry.market-data/l2-symbol-projection/v1"
L2_SYMBOL_PROJECTION_SCHEMA_HASH = (
    "sha256:987bf197829779e96a7e5bee9b57d90b267e5388d75de108ec75dce57fc60457"
)
L2_PROJECTION_COLUMNS = (
    "stream_type",
    "connection_id",
    "receive_seq",
    "app_receive_realtime_ns",
    "app_receive_monotonic_ns",
    "exchange_event_time_ms",
    "exchange_transaction_time_ms",
    "payload_hash",
    "is_duplicate",
    "first_update_id",
    "final_update_id",
    "previous_final_update_id",
    "last_update_id",
    "bids",
    "asks",
)
L2_PROJECTION_ARROW_SCHEMA = pa.schema(
    [TYPED_EVENT_SCHEMA.field(name) for name in L2_PROJECTION_COLUMNS]
)
_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
_HEX_HASH = re.compile(r"^[0-9a-f]{64}$")
_MARKER_FIELDS = frozenset(
    {
        "schema_version",
        "schema_id",
        "schema_hash",
        "layout",
        "canonical_replay",
        "data_role",
        "retention_policy",
        "minimum_retention_days",
        "collector_id",
        "utc_date",
        "normalized_sha256",
        "typed_source_files",
        "typed_source_file_set_hash",
        "symbols",
        "files",
        "schedule",
        "ignored_rows",
        "total_rows",
        "total_bytes",
        "output_root",
    }
)


def content_hash(value: object) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def source_file_identity(path: Path, *, uri: str) -> dict[str, object]:
    payload = {
        "uri": uri,
        "size_bytes": path.stat().st_size,
        "content_hash": "sha256:" + sha256_file(path),
    }
    return {**payload, "identity_hash": content_hash(payload)}


def validate_source_file_identities(
    values: object,
    *,
    expected_set_hash: object,
) -> tuple[dict[str, Any], ...]:
    if not isinstance(values, list):
        raise ValueError("typed source identities must be an array")
    identities = tuple(values)
    if content_hash(identities) != expected_set_hash:
        raise ValueError("typed source file set hash invalid")
    uris = []
    for item in identities:
        if not isinstance(item, dict) or set(item) != {
            "uri",
            "size_bytes",
            "content_hash",
            "identity_hash",
        }:
            raise ValueError("typed source identity invalid")
        uri = item.get("uri")
        size_bytes = item.get("size_bytes")
        source_hash = item.get("content_hash")
        identity_hash = item.get("identity_hash")
        path = PurePosixPath(uri) if isinstance(uri, str) else PurePosixPath("/")
        if (
            not isinstance(uri, str)
            or not uri
            or path.is_absolute()
            or ".." in path.parts
            or not isinstance(size_bytes, int)
            or isinstance(size_bytes, bool)
            or size_bytes < 1
            or not isinstance(source_hash, str)
            or _HASH.fullmatch(source_hash) is None
            or not isinstance(identity_hash, str)
            or _HASH.fullmatch(identity_hash) is None
        ):
            raise ValueError("typed source identity invalid")
        identity = {key: item.get(key) for key in ("uri", "size_bytes", "content_hash")}
        if identity_hash != content_hash(identity):
            raise ValueError("typed source identity hash invalid")
        uris.append(uri)
    if tuple(uris) != tuple(sorted(set(uris))):
        raise ValueError("typed source identities must be ordered and unique")
    return identities


def validate_marker(
    marker: dict[str, Any],
    *,
    collector_id: str,
    utc_date: str,
    normalized_sha256: str,
    expected_symbols: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    typed_sources = marker.get("typed_source_files")
    files = marker.get("files")
    schedule = tuple(marker.get("schedule") or ())
    if (
        set(marker) != _MARKER_FIELDS
        or marker.get("schema_version") != 3
        or marker.get("schema_id") != L2_SYMBOL_PROJECTION_SCHEMA_ID
        or marker.get("schema_hash") != L2_SYMBOL_PROJECTION_SCHEMA_HASH
        or marker.get("layout") != "PER_SYMBOL_L2_CAUSAL_V1"
        or marker.get("canonical_replay") is not False
        or marker.get("data_role") != "PERFORMANCE_PROJECTION"
        or marker.get("retention_policy") != "BOUNDED_REGENERABLE"
        or not isinstance(marker.get("minimum_retention_days"), int)
        or isinstance(marker.get("minimum_retention_days"), bool)
        or int(marker["minimum_retention_days"]) < 7
        or marker.get("collector_id") != collector_id
        or marker.get("utc_date") != utc_date
        or marker.get("normalized_sha256") != normalized_sha256
        or tuple(marker.get("symbols") or ()) != expected_symbols
        or len(schedule) != len(set(schedule))
        or set(schedule) != set(expected_symbols)
        or not isinstance(typed_sources, list)
        or not isinstance(files, dict)
    ):
        raise ValueError("L2 symbol projection marker identity invalid")
    validate_source_file_identities(
        typed_sources,
        expected_set_hash=marker.get("typed_source_file_set_hash"),
    )
    if set(files) != set(expected_symbols):
        raise ValueError("L2 symbol projection file universe invalid")
    rows = 0
    size_bytes = 0
    for symbol, item in files.items():
        if (
            not isinstance(item, dict)
            or set(item) != {"rows", "size_bytes", "sha256"}
            or not isinstance(item.get("rows"), int)
            or isinstance(item.get("rows"), bool)
            or int(item["rows"]) < 1
            or not isinstance(item.get("size_bytes"), int)
            or isinstance(item.get("size_bytes"), bool)
            or int(item["size_bytes"]) < 1
            or not isinstance(item.get("sha256"), str)
            or _HEX_HASH.fullmatch(item["sha256"]) is None
        ):
            raise ValueError(f"L2 symbol projection file record invalid: {symbol}")
        rows += int(item["rows"])
        size_bytes += int(item["size_bytes"])
    ignored_rows = marker.get("ignored_rows")
    if (
        not isinstance(ignored_rows, dict)
        or any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in ignored_rows.values()
        )
        or marker.get("total_rows") != rows
        or marker.get("total_bytes") != size_bytes
        or not isinstance(marker.get("output_root"), str)
        or not marker["output_root"]
    ):
        raise ValueError("L2 symbol projection marker totals invalid")
    return files


def validate_shard(path: Path, details: dict[str, Any]) -> None:
    if (
        not path.is_file()
        or path.stat().st_size != int(details.get("size_bytes", -1))
        or sha256_file(path) != details.get("sha256")
    ):
        raise ValueError(f"L2 symbol projection shard invalid: {path}")
    parquet = pq.ParquetFile(path)
    if (
        parquet.metadata.num_rows != int(details["rows"])
        or not parquet.schema_arrow.equals(L2_PROJECTION_ARROW_SCHEMA, check_metadata=False)
    ):
        raise ValueError(f"L2 symbol projection Parquet contract invalid: {path}")
