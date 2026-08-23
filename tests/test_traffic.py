from __future__ import annotations

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


def test_old_load_weight_field_is_rejected() -> None:
    raw = yaml.safe_load((PROJECT_ROOT / "deploy/vultr/edge.yaml.example").read_bytes())
    raw["public_symbol_load_weights"] = raw.pop("message_rates")

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        CollectorConfig.model_validate(raw)
