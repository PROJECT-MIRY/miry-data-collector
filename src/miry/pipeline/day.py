from __future__ import annotations

from datetime import date
from pathlib import Path

from miry.contracts.symbols import validate_exchange_symbol
from miry.pipeline.clock import build_clock_quality
from miry.pipeline.d0 import build_d0_audit
from miry.pipeline.gaps import build_transport_gap_ledger
from miry.pipeline.l2 import L2DayReconstructor, partitioned_l2_input
from miry.pipeline.normalize import DayNormalizer, NormalizeResult


def normalize_day(
    *,
    raw_root: Path,
    derived_root: Path,
    collector_id: str,
    utc_date: date,
    max_workers: int = 1,
) -> NormalizeResult:
    result = DayNormalizer(
        raw_root=raw_root,
        derived_root=derived_root,
        collector_id=collector_id,
        utc_date=utc_date,
        max_workers=max_workers,
    ).run()
    build_transport_gap_ledger(
        raw_root=raw_root,
        derived_root=derived_root,
        collector_id=collector_id,
        utc_date=utc_date,
    )
    build_clock_quality(
        derived_root=derived_root,
        collector_id=collector_id,
        utc_date=utc_date,
    )
    return result


def reconstruct_l2_day(
    *,
    derived_root: Path,
    collector_id: str,
    utc_date: date,
    exchange_symbol: str,
) -> tuple[int, int]:
    exchange_symbol = validate_exchange_symbol(exchange_symbol.upper())
    return L2DayReconstructor(
        derived_root=derived_root,
        collector_id=collector_id,
        utc_date=utc_date,
        exchange_symbol=exchange_symbol,
        input_path=partitioned_l2_input(
            derived_root=derived_root,
            collector_id=collector_id,
            utc_date=utc_date,
            exchange_symbol=exchange_symbol,
        ),
    ).run()


def audit_d0_day(*, derived_root: Path, collector_id: str, utc_date: date) -> Path:
    return build_d0_audit(
        derived_root=derived_root,
        collector_id=collector_id,
        utc_date=utc_date,
    )
