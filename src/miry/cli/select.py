from __future__ import annotations

import argparse
import gzip
from datetime import UTC, datetime
from pathlib import Path

from miry.contracts.models import UniverseDecision, UniverseDecisionReason
from miry.contracts.serde import universe_hash
from miry.universe.models import DiscoverySnapshot, RollingPolicy
from miry.universe.selection import select_bootstrap_universe, write_formal_bundle


def main() -> None:
    parser = argparse.ArgumentParser(description="Write a formal 60-instrument universe bundle")
    parser.add_argument("--exchange-info", type=Path, required=True)
    parser.add_argument("--exchange-info-confirmation", type=Path, required=True)
    parser.add_argument("--market-tickers", type=Path, required=True)
    parser.add_argument("--daily-klines", type=Path, required=True)
    parser.add_argument("--liquidity-depth", type=Path, required=True)
    parser.add_argument("--observed-at", type=_utc_datetime, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    now = datetime.now(UTC)
    snapshot = DiscoverySnapshot(
        observed_at=args.observed_at,
        exchange_info=_read(args.exchange_info),
        exchange_info_confirmation=_read(args.exchange_info_confirmation),
        market_tickers=_read(args.market_tickers),
        daily_klines=_read(args.daily_klines),
        liquidity_depth=_read(args.liquidity_depth),
    )
    selected = select_bootstrap_universe(snapshot, policy=RollingPolicy())
    core = selected.core
    boundary = selected.boundary
    probe = selected.probe
    decision = UniverseDecision(
        core_generation=1,
        candidate_revision=0,
        decision_sequence=1,
        universe_version="1.0",
        created_at=now,
        effective_at=now,
        reason=UniverseDecisionReason.FORMAL_BOOTSTRAP,
        core=core,
        boundary=boundary,
        probe=probe,
        source_hashes=snapshot.source_hashes,
        universe_hash=universe_hash(core, boundary, probe),
    )
    write_formal_bundle(decision, args.output_dir, snapshot=snapshot)


def _read(path: Path) -> bytes:
    content = path.read_bytes()
    return gzip.decompress(content) if path.suffix == ".gz" else content


def _utc_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise argparse.ArgumentTypeError("timestamp must include UTC offset")
    return parsed


if __name__ == "__main__":
    main()
