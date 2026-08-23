from __future__ import annotations

import pyarrow as pa

from miry.contracts.models import RawEvent

RAW_EVENT_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.int16(), nullable=False),
        pa.field("exchange_symbol", pa.string()),
        pa.field("stream_type", pa.dictionary(pa.int16(), pa.string()), nullable=False),
        pa.field("collector_id", pa.string(), nullable=False),
        pa.field("boot_id", pa.string(), nullable=False),
        pa.field("segment_id", pa.string(), nullable=False),
        pa.field("connection_id", pa.string(), nullable=False),
        pa.field("receive_seq", pa.int64(), nullable=False),
        pa.field("app_receive_realtime_ns", pa.int64(), nullable=False),
        pa.field("app_receive_monotonic_ns", pa.int64(), nullable=False),
        pa.field("payload_bytes", pa.binary(), nullable=False),
        pa.field("request_id", pa.string()),
        pa.field("request_realtime_ns", pa.int64()),
    ]
)


def raw_events_to_table(events: list[RawEvent]) -> pa.Table:
    return pa.Table.from_pydict(
        {
            "schema_version": [event.schema_version for event in events],
            "exchange_symbol": [event.exchange_symbol for event in events],
            "stream_type": [event.stream_type.value for event in events],
            "collector_id": [event.collector_id for event in events],
            "boot_id": [event.boot_id for event in events],
            "segment_id": [event.segment_id for event in events],
            "connection_id": [event.connection_id for event in events],
            "receive_seq": [event.receive_seq for event in events],
            "app_receive_realtime_ns": [
                event.app_receive_realtime_ns for event in events
            ],
            "app_receive_monotonic_ns": [
                event.app_receive_monotonic_ns for event in events
            ],
            "payload_bytes": [event.payload_bytes for event in events],
            "request_id": [event.request_id for event in events],
            "request_realtime_ns": [event.request_realtime_ns for event in events],
        },
        schema=RAW_EVENT_SCHEMA,
    )
