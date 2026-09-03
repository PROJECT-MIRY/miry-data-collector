from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest
import yaml

from miry.collector.config import CollectorConfig
from miry.collector.sharding import TrafficSharder
from miry.collector.traffic import OBSERVATION_BLOCKS, PublicTrafficRecorder

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class FakeClock:
    def __init__(self) -> None:
        self.minute = 0

    def __call__(self) -> float:
        return self.minute * 60 + 1


def _record_block(
    recorder: PublicTrafficRecorder,
    clock: FakeClock,
    *,
    btc_rate: int,
    eth_rate: int,
) -> None:
    for _ in range(60):
        clock.minute += 1
        for _ in range(btc_rate):
            recorder.record("public-0", "BTCUSDT")
        for _ in range(eth_rate):
            recorder.record("public-1", "ETHUSDT")
        recorder.finish_minute(clock.minute)


def test_traffic_sharder_balances_rates_and_preserves_existing_routes() -> None:
    rates = {
        "BTCUSDT": 9_300,
        "ETHUSDT": 16_900,
        "XRPUSDT": 8_500,
        "DOGEUSDT": 8_400,
        "SOLUSDT": 4_100,
        "PUMPUSDT": 7_100,
        "ZECUSDT": 6_300,
        "ADAUSDT": 2_300,
        "BNBUSDT": 3_000,
        "LINKUSDT": 4_500,
        "LTCUSDT": 2_300,
        "SUIUSDT": 3_500,
    }
    sharder = TrafficSharder(4, rates)
    initial = tuple(rates)

    shards = sharder.shards(initial)
    route_rates = [sum(rates[symbol] for symbol in shard) for shard in shards]
    assignments = {
        symbol: index for index, shard in enumerate(shards) for symbol in shard
    }

    assert max(route_rates) / min(route_rates) < 1.10
    updated = sharder.shards((*initial[1:], "NEWUSDT"))
    updated_assignments = {
        symbol: index for index, shard in enumerate(updated) for symbol in shard
    }
    assert all(
        updated_assignments[symbol] == route
        for symbol, route in assignments.items()
        if symbol != initial[0]
    )


def test_eight_shards_bound_fault_scope_and_snapshot_wait() -> None:
    raw = yaml.safe_load((PROJECT_ROOT / "deploy/vultr/edge.yaml.example").read_text())
    raw["public_connection_shards"] = 8
    config = CollectorConfig.model_validate(raw)

    shards = TrafficSharder(8, config.message_rates).shards(config.universe.members)
    route_rates = [
        sum(config.message_rates[symbol] for symbol in shard) for shard in shards
    ]

    assert len(shards) == 8
    assert sum(map(len, shards)) == 60
    four_shards = TrafficSharder(4, config.message_rates).shards(config.universe.members)
    four_route_max = max(
        sum(config.message_rates[symbol] for symbol in shard) for shard in four_shards
    )
    assert max(map(len, shards)) <= 9
    assert max(route_rates) < four_route_max
    assert (max(map(len, shards)) - 1) * config.snapshot_request_interval_seconds <= 6


def test_live_rebalance_moves_at_most_one_symbol_pair() -> None:
    rates = {
        "AUSDT": 100,
        "BUSDT": 90,
        "CUSDT": 10,
        "DUSDT": 5,
    }
    current = (("AUSDT", "BUSDT"), ("CUSDT", "DUSDT"))

    rebalanced = TrafficSharder(2, rates).rebalance(tuple(rates), current)

    moved = {
        symbol
        for symbol in rates
        if next(index for index, shard in enumerate(current) if symbol in shard)
        != next(index for index, shard in enumerate(rebalanced) if symbol in shard)
    }
    assert rebalanced == (("BUSDT", "CUSDT"), ("AUSDT", "DUSDT"))
    assert len(moved) == 2


def test_live_rebalance_keeps_assignment_below_imbalance_trigger() -> None:
    rates = {
        "AUSDT": 60,
        "BUSDT": 50,
        "CUSDT": 55,
        "DUSDT": 45,
    }
    current = (("AUSDT", "DUSDT"), ("BUSDT", "CUSDT"))

    assert TrafficSharder(2, rates).rebalance(tuple(rates), current) == current


def test_observed_rates_require_24_complete_blocks_and_keep_24(tmp_path: Path) -> None:
    clock = FakeClock()
    recorder = PublicTrafficRecorder(
        tmp_path / "public-message-rates.json",
        {"BTCUSDT": 100, "ETHUSDT": 200},
        clock=clock,
    )

    for block in range(23):
        _record_block(recorder, clock, btc_rate=block + 1, eth_rate=block + 2)
    assert not recorder.has_complete_evidence
    assert recorder.effective_rates() == {"BTCUSDT": 100, "ETHUSDT": 200}

    _record_block(recorder, clock, btc_rate=24, eth_rate=25)
    assert recorder.has_complete_evidence
    assert recorder.effective_rates() == {"BTCUSDT": 24, "ETHUSDT": 25}

    for block in range(24, 26):
        _record_block(recorder, clock, btc_rate=block + 1, eth_rate=block + 2)
    assert len(recorder.blocks) == OBSERVATION_BLOCKS
    assert recorder.blocks[0].start_minute == 121
    assert recorder.effective_rates() == {"BTCUSDT": 26, "ETHUSDT": 27}


@pytest.mark.asyncio
async def test_traffic_state_write_failure_does_not_stop_collection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorder = PublicTrafficRecorder(tmp_path / "state.json", {"BTCUSDT": 100})

    def fail_write(_path: Path, _content: bytes) -> None:
        raise OSError("disk unavailable")

    monkeypatch.setattr(
        "miry.collector.traffic.atomic_write_bytes",
        fail_write,
    )

    await recorder._persist()


def test_corrupt_traffic_state_falls_back_to_baseline(tmp_path: Path) -> None:
    state = tmp_path / "control/public-message-rates.json"
    state.parent.mkdir(parents=True)
    state.write_bytes(b"not-json")

    recorder = PublicTrafficRecorder(state, {"BTCUSDT": 100})

    assert recorder.blocks == ()
    assert recorder.effective_rates() == {"BTCUSDT": 100}


def test_message_rates_are_decoupled_from_current_universe() -> None:
    raw = yaml.safe_load((PROJECT_ROOT / "deploy/vultr/edge.yaml.example").read_bytes())
    raw["message_rates"] = {"BTCUSDT": 100}

    config = CollectorConfig.model_validate(raw)

    assert config.message_rates == {"BTCUSDT": 100}


def test_record_uses_receive_timestamp_without_reading_clock(tmp_path: Path) -> None:
    clock = FakeClock()
    recorder = PublicTrafficRecorder(tmp_path / "state.json", {"BTCUSDT": 100}, clock=clock)

    def fail_clock() -> float:
        raise AssertionError("record read the wall clock")

    recorder._clock = fail_clock
    recorder.record("public-0", "BTCUSDT", realtime_ns=121_000_000_000)
    recorder.finish_minute(2)

    assert recorder._minute_routes == {}


def test_record_reuses_existing_minute_counters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = 0

    def counting_counter() -> Counter[str]:
        nonlocal created
        created += 1
        return Counter()

    monkeypatch.setattr("miry.collector.traffic.Counter", counting_counter)
    recorder = PublicTrafficRecorder(tmp_path / "state.json", {})

    recorder.record("public-0", "BTCUSDT", realtime_ns=121_000_000_000)
    recorder.record("public-0", "ETHUSDT", realtime_ns=122_000_000_000)

    assert created == 3


def test_old_load_weight_field_is_rejected() -> None:
    raw = yaml.safe_load((PROJECT_ROOT / "deploy/vultr/edge.yaml.example").read_bytes())
    raw["public_symbol_load_weights"] = raw.pop("message_rates")

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        CollectorConfig.model_validate(raw)
