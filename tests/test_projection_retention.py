from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

SCRIPT = (
    Path(__file__).parents[1]
    / "deploy/campus-107/prune-l2-projections.py"
)


def test_retention_deletes_oldest_verified_day_to_meet_budget(
    tmp_path: Path, monkeypatch
) -> None:
    module = _load_script()
    root = tmp_path / "derived/l2-symbol-projections/collector=tokyo01"
    for utc_date in ("2026-08-20", "2026-08-21", "2026-08-28"):
        day = root / f"date={utc_date}"
        day.mkdir(parents=True)
        (day / "payload").write_bytes(b"x" * 10)
    verified: list[str] = []
    monkeypatch.setattr(
        module,
        "validate_projection_day",
        lambda **values: verified.append(values["utc_date"]),
    )

    plan = module.retention_plan(
        derived_root=tmp_path / "derived",
        projection_root=root,
        collector="tokyo01",
        max_bytes=20,
        minimum_days=7,
        as_of=date(2026, 8, 29),
    )

    assert plan["delete"] == [{"utc_date": "2026-08-20", "size_bytes": 10}]
    assert plan["projected_bytes"] == 20
    assert verified == ["2026-08-20"]


def test_retention_fails_if_minimum_window_alone_exceeds_budget(
    tmp_path: Path, monkeypatch
) -> None:
    module = _load_script()
    root = tmp_path / "derived/l2-symbol-projections/collector=tokyo01"
    for utc_date in ("2026-08-20", "2026-08-28"):
        day = root / f"date={utc_date}"
        day.mkdir(parents=True)
        (day / "payload").write_bytes(b"x" * 10)
    monkeypatch.setattr(module, "validate_projection_day", lambda **_: None)

    with pytest.raises(ValueError, match="minimum retention"):
        module.retention_plan(
            derived_root=tmp_path / "derived",
            projection_root=root,
            collector="tokyo01",
            max_bytes=5,
            minimum_days=7,
            as_of=date(2026, 8, 29),
        )


def _load_script():
    import importlib.util

    spec = importlib.util.spec_from_file_location("projection_retention", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
