from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import orjson
import pytest
from universe_fixtures import formal_roles, liquidity_snapshot

from miry.cli.control import main as control_main
from miry.collector.config import UniversePolicyConfig
from miry.collector.membership import UniverseStore, _next_version
from miry.contracts.models import UniverseDecision, UniverseDecisionReason
from miry.contracts.serde import universe_hash

EVIDENCE_HASH = "d" * 64


def test_clean_store_starts_version_one_zero_with_research_hash(tmp_path: Path) -> None:
    now = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)
    store = UniverseStore(tmp_path, _policy())

    active = store.initialize(now)

    assert active.universe_version == "1.0"
    assert active.core_generation == 1
    assert active.candidate_revision == 0
    assert active.decision_sequence == 1
    assert (active.core, active.boundary, active.probe) == formal_roles()
    assert active.source_hashes == (EVIDENCE_HASH,)


def test_formal_bootstrap_validates_members_and_binds_live_sources(tmp_path: Path) -> None:
    now = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)
    store = UniverseStore(tmp_path, _policy())
    store.initialize(now)
    snapshot = liquidity_snapshot(now)

    assert store.observe_and_plan(snapshot, now=now) is None
    assert store.active.source_hashes == (EVIDENCE_HASH, *snapshot.source_hashes)
    assert (tmp_path / "control/universe/evaluations").is_dir()


def test_formal_bootstrap_rejects_configured_members_without_complete_evidence(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 17, 23, 50, tzinfo=UTC)
    core, _, _ = formal_roles()
    store = UniverseStore(tmp_path, _policy())
    store.initialize(now)

    with pytest.raises(ValueError, match="lack complete cross-sectional evidence"):
        store.observe_and_plan(
            liquidity_snapshot(now, missing_depth=frozenset({core[0]})), now=now
        )

    assert core == store.active.core


def test_confirmed_inactive_candidate_is_planned_after_formal_start(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 18, 23, 50, tzinfo=UTC)
    store = UniverseStore(tmp_path, _policy())
    active = store.initialize(now - timedelta(days=3))
    (tmp_path / "control/formal-start.json").write_text("{}", encoding="ascii")

    decision = store.observe_and_plan(
        liquidity_snapshot(now, inactive=active.boundary[0]), now=now
    )

    assert decision is not None
    assert active.boundary[0] not in decision.members
    assert decision.effective_at == datetime(2026, 8, 19, tzinfo=UTC)
    assert decision.universe_version == "1.1"
    assert decision.decision_sequence == 2


def test_unchanged_members_do_not_create_a_new_version(tmp_path: Path) -> None:
    now = datetime(2026, 8, 18, 23, 50, tzinfo=UTC)
    store = UniverseStore(tmp_path, _policy())
    active = store.initialize(now - timedelta(days=3))
    (tmp_path / "control/formal-start.json").write_text("{}", encoding="ascii")

    decision = store.observe_and_plan(liquidity_snapshot(now), now=now)

    assert decision is None
    assert store.active == active
    assert not (tmp_path / "control/universe/pending.json").exists()


def test_disabled_automation_records_evaluation_and_discards_stale_pending(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 18, 23, 50, tzinfo=UTC)
    enabled = UniverseStore(tmp_path, _policy())
    active = enabled.initialize(now - timedelta(days=3))
    (tmp_path / "control/formal-start.json").write_text("{}", encoding="ascii")
    pending = enabled.observe_and_plan(
        liquidity_snapshot(now, inactive=active.boundary[0]), now=now
    )
    assert pending is not None

    disabled = UniverseStore(tmp_path, _policy(automation_enabled=False))
    disabled.initialize(now)
    assert disabled.observe_and_plan(liquidity_snapshot(now), now=now) is None

    evaluation_paths = list((tmp_path / "control/universe/evaluations").glob("*.json"))
    assert evaluation_paths
    assert not (tmp_path / "control/universe/pending.json").exists()
    assert disabled.apply_due(datetime(2026, 8, 19, tzinfo=UTC)) is None
    assert disabled.active == active


def test_core_and_candidate_changes_advance_separate_version_components() -> None:
    core, boundary, probe = formal_roles()
    active = UniverseDecision(
        core_generation=6,
        candidate_revision=1,
        decision_sequence=7,
        universe_version="6.1",
        created_at=datetime(2026, 8, 21, tzinfo=UTC),
        effective_at=datetime(2026, 8, 21, tzinfo=UTC),
        reason=UniverseDecisionReason.DAILY_CANDIDATE,
        core=core,
        boundary=boundary,
        probe=probe,
        universe_hash=universe_hash(core, boundary, probe),
    )

    assert _next_version(active, active.core) == (6, 2)
    changed_core = tuple(sorted((*active.core[1:], active.boundary[0])))
    assert _next_version(active, changed_core) == (7, 0)


def test_candidate_override_cli_leaves_version_allocation_to_edge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _core, boundary, probe = formal_roles()
    boundary_path = tmp_path / "boundary.txt"
    probe_path = tmp_path / "probe.txt"
    output_path = tmp_path / "candidate.override.json"
    boundary_path.write_text("\n".join(boundary), encoding="ascii")
    probe_path.write_text("\n".join(probe), encoding="ascii")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "miry-data-control",
            "--effective-at",
            "2099-01-01T00:00:00Z",
            "--boundary-file",
            str(boundary_path),
            "--probe-file",
            str(probe_path),
            "--output",
            str(output_path),
        ],
    )

    control_main()

    override = orjson.loads(output_path.read_bytes())
    assert "generation" not in override


def _policy(*, automation_enabled: bool = True) -> UniversePolicyConfig:
    core, boundary, probe = formal_roles()
    return UniversePolicyConfig(
        experiment_id="formal-test-60",
        bootstrap_evidence_sha256=EVIDENCE_HASH,
        core=core,
        boundary=boundary,
        probe=probe,
        automation_enabled=automation_enabled,
    )
