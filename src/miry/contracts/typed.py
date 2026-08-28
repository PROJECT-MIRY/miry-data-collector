from __future__ import annotations

import pyarrow as pa

BOOK_LEVEL = pa.struct(
    [
        pa.field("price", pa.string(), nullable=False),
        pa.field("quantity", pa.string(), nullable=False),
    ]
)

TYPED_EVENT_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.int16(), nullable=False),
        pa.field("exchange_symbol", pa.string()),
        pa.field("stream_type", pa.dictionary(pa.int16(), pa.string()), nullable=False),
        pa.field("connection_id", pa.string(), nullable=False),
        pa.field("receive_seq", pa.int64(), nullable=False),
        pa.field("app_receive_realtime_ns", pa.int64(), nullable=False),
        pa.field("app_receive_monotonic_ns", pa.int64(), nullable=False),
        pa.field("request_realtime_ns", pa.int64()),
        pa.field("exchange_event_time_ms", pa.int64()),
        pa.field("exchange_transaction_time_ms", pa.int64()),
        pa.field("payload_hash", pa.binary(32), nullable=False),
        pa.field("is_duplicate", pa.bool_(), nullable=False),
        pa.field("update_id", pa.int64()),
        pa.field("first_update_id", pa.int64()),
        pa.field("final_update_id", pa.int64()),
        pa.field("previous_final_update_id", pa.int64()),
        pa.field("last_update_id", pa.int64()),
        pa.field("trade_id", pa.int64()),
        pa.field("aggregate_trade_id", pa.int64()),
        pa.field("first_trade_id", pa.int64()),
        pa.field("last_trade_id", pa.int64()),
        pa.field("price", pa.string()),
        pa.field("quantity", pa.string()),
        pa.field("non_rpi_quantity", pa.string()),
        pa.field("buyer_is_maker", pa.bool_()),
        pa.field("bid_price", pa.string()),
        pa.field("bid_quantity", pa.string()),
        pa.field("ask_price", pa.string()),
        pa.field("ask_quantity", pa.string()),
        pa.field("mark_price", pa.string()),
        pa.field("index_price", pa.string()),
        pa.field("estimated_settle_price", pa.string()),
        pa.field("funding_rate", pa.string()),
        pa.field("next_funding_time_ms", pa.int64()),
        pa.field("open_interest", pa.string()),
        pa.field("side", pa.string()),
        pa.field("order_type", pa.string()),
        pa.field("time_in_force", pa.string()),
        pa.field("order_status", pa.string()),
        pa.field("average_price", pa.string()),
        pa.field("last_filled_quantity", pa.string()),
        pa.field("accumulated_filled_quantity", pa.string()),
        pa.field("contract_type", pa.string()),
        pa.field("delivery_date_ms", pa.int64()),
        pa.field("onboard_date_ms", pa.int64()),
        pa.field("contract_status", pa.string()),
        pa.field("bids", pa.list_(BOOK_LEVEL)),
        pa.field("asks", pa.list_(BOOK_LEVEL)),
    ]
)
