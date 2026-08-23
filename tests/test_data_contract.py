from __future__ import annotations

import orjson

from miry.contracts.collection import (
    data_contract,
    data_contract_hash,
)


def test_data_contract_tracks_d0_and_open_interest_configuration() -> None:
    default = orjson.loads(data_contract())
    d0 = orjson.loads(data_contract(d0_enabled=True))

    assert default["d0_enabled"] is False
    assert default["gap_schema"] == 2
    assert {"daily_klines", "liquidity_depth"} <= set(default["streams"])
    assert {"trade", "rpi_depth", "rpi_depth_snapshot"}.isdisjoint(default["streams"])
    assert {"trade", "rpi_depth", "rpi_depth_snapshot"} <= set(d0["streams"])
    assert data_contract_hash(d0_enabled=True) != data_contract_hash()
    assert data_contract_hash(open_interest_interval_seconds=60) != data_contract_hash()
