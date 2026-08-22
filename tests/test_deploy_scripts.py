from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from ft_shadow_data_plane.contracts.serde import universe_hash
from ft_shadow_data_plane.edge.config import load_edge_config
from ft_shadow_data_plane.edge.sources import StableWeightedSharder

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INSTALL_CAMPUS = PROJECT_ROOT / "deploy" / "campus-107" / "install.sh"
SUBMIT_DAY = PROJECT_ROOT / "deploy" / "campus-107" / "submit-day.sh"
RSYNC_GATEWAY = PROJECT_ROOT / "deploy" / "vultr" / "rsync_gateway.py"


def _load_rsync_gateway():
    spec = importlib.util.spec_from_file_location("rsync_gateway", RSYNC_GATEWAY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


RSYNC_GATEWAY_MODULE = _load_rsync_gateway()


def test_vultr_config_is_formal_sixty_and_memory_bounded() -> None:
    config = load_edge_config(PROJECT_ROOT / "deploy/vultr/edge.yaml.example")
    compose = (PROJECT_ROOT / "deploy/vultr/compose.yaml").read_text(encoding="ascii")
    service = (PROJECT_ROOT / "deploy/vultr/systemd/ft-shadow-data-plane.service").read_text(
        encoding="ascii"
    )

    role_sizes = (
        len(config.universe.core),
        len(config.universe.boundary),
        len(config.universe.probe),
    )
    assert role_sizes == (
        50,
        5,
        5,
    )
    assert len(config.universe.members) == 60
    assert (
        config.universe.core_generation,
        config.universe.candidate_revision,
        config.universe.decision_sequence,
    ) == (7, 0, 8)
    assert universe_hash(
        config.universe.core,
        config.universe.boundary,
        config.universe.probe,
    ) == "608e49c7199a3ab7cbc4c364c7126015ece7fbe4ccb9e9655de4204ede9ca22c"
    evidence = PROJECT_ROOT / "docs/formal-universe-7.0-evidence.json"
    assert hashlib.sha256(evidence.read_bytes()).hexdigest() == (
        config.universe.bootstrap_evidence_sha256
    )
    frozen = json.loads(evidence.read_bytes())
    assert (
        frozen["core_generation"],
        frozen["candidate_revision"],
        frozen["decision_sequence"],
        frozen["universe_version"],
    ) == (7, 0, 8, "7.0")
    assert tuple(frozen["core"]) == config.universe.core
    assert tuple(frozen["boundary"]) == config.universe.boundary
    assert tuple(frozen["probe"]) == config.universe.probe
    assert frozen["universe_hash"] == universe_hash(
        tuple(frozen["core"]),
        tuple(frozen["boundary"]),
        tuple(frozen["probe"]),
    )
    assert config.public_connection_shards == 4
    assert len(config.public_symbol_load_weights) == 60
    shards = StableWeightedSharder(
        config.public_connection_shards,
        config.public_symbol_load_weights,
    ).shards(config.universe.members)
    shard_sizes = sorted(len(shard) for shard in shards)
    shard_loads = [
        sum(config.public_symbol_load_weights[symbol] for symbol in shard)
        for shard in shards
    ]
    assert sum(shard_sizes) == 60
    assert max(shard_sizes) <= 18
    assert max(shard_loads) / min(shard_loads) < 1.05
    assert (max(shard_sizes) - 1) * config.snapshot_request_interval_seconds <= 17
    assert config.queue_max_bytes == 64 * 1024**2
    assert config.minimum_free_bytes == 2 * 1024**3
    assert config.websocket_max_queue == 16
    assert config.mark_price_liveness_seconds == 15
    assert config.subscription_audit_timeout_seconds == 20
    assert config.subscription_audit_failures_before_reconnect == 3
    assert config.refresh_failures_before_reconnect == 2
    assert config.writer_batch_bytes == 2 * 1024**2
    assert "mem_limit: 768m" in compose
    assert "cpus: 0.90" in compose
    assert "pids_limit: 256" in compose
    assert "--exit-code-from collector" in service


@pytest.mark.parametrize(
    ("command", "expected_arguments"),
    [
        (
            "rsync --server --sender -logDtpre.iLsfxCIvu . ready/",
            ("-ro", "/srv/ft-data-rsync/ready"),
        ),
        (
            "rsync --server -logDtpre.iLsfxCIvu . control/acks/",
            ("-wo", "-no-del", "/srv/ft-data-rsync/control/acks"),
        ),
    ],
)
def test_rsync_gateway_scopes_expected_transfers(
    command: str, expected_arguments: tuple[str, ...]
) -> None:
    restricted = RSYNC_GATEWAY_MODULE.restrict_command(command)

    assert restricted.original_command.endswith(" . .")
    assert restricted.rrsync_arguments == expected_arguments


@pytest.mark.parametrize(
    "command",
    [
        "bash",
        "rsync --server -logDtpre.iLsfxCIvu . ready/",
        "rsync --server --sender -logDtpre.iLsfxCIvu . control/acks/",
        "rsync --server --sender -logDtpre.iLsfxCIvu . ../ready/",
        "rsync --server --sender -logDtpre.iLsfxCIvu . ready/ control/",
    ],
)
def test_rsync_gateway_rejects_out_of_scope_commands(command: str) -> None:
    with pytest.raises(ValueError):
        RSYNC_GATEWAY_MODULE.restrict_command(command)


def test_campus_installer_uses_hash_named_release(tmp_path: Path) -> None:
    release = tmp_path / "downloaded.sif"
    release.write_bytes(b"immutable release")
    install_root = tmp_path / "persistent"
    fake_apptainer = tmp_path / "apptainer"
    _write_fake_apptainer(fake_apptainer)

    result = subprocess.run(
        [str(INSTALL_CAMPUS), str(release)],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "FT_APPTAINER": str(fake_apptainer),
            "FT_CAMPUS_ROOT": str(install_root),
        },
    )

    assert result.returncode == 0, result.stderr
    digest = hashlib.sha256(release.read_bytes()).hexdigest()
    versioned_release = install_root / f"ft-shadow-data-plane-{digest}.sif"
    active_release = install_root / "ft-shadow-data-plane.sif"
    assert versioned_release.read_bytes() == release.read_bytes()
    assert active_release.is_symlink()
    assert active_release.resolve() == versioned_release
    assert (install_root / "ft-shadow-data-plane.sandbox").is_symlink()
    assert os.access(install_root / "pull-once.sh", os.X_OK)
    assert (install_root / "status").is_dir()
    assert (install_root / "data/transfer-ledger").is_dir()
    assert (install_root / "central.yaml").is_file()
    assert (install_root / "deploy/campus-107/processing.env").is_file()


def test_submit_day_builds_dependency_chain(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    sbatch_log = tmp_path / "sbatch.log"
    _write_fake_sbatch(fake_bin / "sbatch")
    processing_env = _write_processing_env(tmp_path, concurrency=8)
    symbols = tmp_path / "symbols.txt"
    _write_symbols(symbols)

    result = subprocess.run(
        [str(SUBMIT_DAY), "2026-08-10", str(symbols)],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "FT_PROCESSING_ENV": str(processing_env),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "SBATCH_LOG": str(sbatch_log),
        },
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "normalize_job=101",
        "l2_job=102",
        "finalize_job=103",
    ]
    calls = sbatch_log.read_text(encoding="ascii").splitlines()
    assert calls[0].endswith("/slurm/normalize.sbatch")
    assert "--dependency=afterok:101" in calls[1]
    assert "--array=0-59%8" in calls[1]
    assert calls[1].endswith("/slurm/l2-array.sbatch")
    assert "--dependency=afterok:102" in calls[2]
    assert calls[2].endswith("/slurm/finalize.sbatch")


def test_submit_day_rejects_duplicate_symbols(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    sbatch_log = tmp_path / "sbatch.log"
    _write_fake_sbatch(fake_bin / "sbatch")
    processing_env = _write_processing_env(tmp_path, concurrency=8)
    symbols = tmp_path / "symbols.txt"
    _write_symbols(symbols, duplicate=True)

    result = subprocess.run(
        [str(SUBMIT_DAY), "2026-08-10", str(symbols)],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "FT_PROCESSING_ENV": str(processing_env),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "SBATCH_LOG": str(sbatch_log),
        },
    )

    assert result.returncode == 1
    assert "exactly 60 unique" in result.stderr
    assert not sbatch_log.exists()


def test_submit_day_rejects_out_of_order_checkpoint_processing(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    sbatch_log = tmp_path / "sbatch.log"
    _write_fake_sbatch(fake_bin / "sbatch")
    processing_env = _write_processing_env(tmp_path, concurrency=8)
    symbols = tmp_path / "symbols.txt"
    _write_symbols(symbols)
    previous_raw = tmp_path / "raw/collector=tokyo01/day-manifests/date=2026-08-09/SEALED.json"
    previous_raw.parent.mkdir(parents=True)
    previous_raw.write_text("{}", encoding="ascii")
    environment = {
        **os.environ,
        "FT_PROCESSING_ENV": str(processing_env),
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "SBATCH_LOG": str(sbatch_log),
    }

    rejected = subprocess.run(
        [str(SUBMIT_DAY), "2026-08-10", str(symbols)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert rejected.returncode == 1
    assert "previous UTC day must be processed first" in rejected.stderr
    assert not sbatch_log.exists()

    previous_processed = (
        tmp_path / "derived/quality/collector=tokyo01/date=2026-08-09/_PROCESSED.json"
    )
    previous_processed.parent.mkdir(parents=True)
    previous_processed.write_text("{}", encoding="ascii")
    accepted = subprocess.run(
        [str(SUBMIT_DAY), "2026-08-10", str(symbols)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert accepted.returncode == 0, accepted.stderr


def _write_processing_env(tmp_path: Path, *, concurrency: int) -> Path:
    path = tmp_path / "processing.env"
    path.write_text(
        "\n".join(
            (
                f"FT_APPTAINER={tmp_path / 'apptainer'}",
                f"FT_DATA_IMAGE={tmp_path / 'release.sandbox'}",
                f"FT_RAW_ROOT={tmp_path / 'raw'}",
                f"FT_DERIVED_ROOT={tmp_path / 'derived'}",
                "FT_COLLECTOR=tokyo01",
                f"FT_L2_CONCURRENCY={concurrency}",
                "",
            )
        ),
        encoding="ascii",
    )
    return path


def _write_symbols(path: Path, *, duplicate: bool = False) -> None:
    symbols = [f"S{index:03}USDT" for index in range(60)]
    if duplicate:
        symbols[-1] = symbols[0]
    path.write_text("\n".join((*symbols, "")), encoding="ascii")


def _write_fake_sbatch(path: Path) -> None:
    path.write_text(
        """#!/bin/sh
set -eu
printf '%s\\n' "$*" >> "$SBATCH_LOG"
case "$*" in
    *normalize.sbatch) echo '101;cluster' ;;
    *l2-array.sbatch) echo '102;cluster' ;;
    *finalize.sbatch) echo '103;cluster' ;;
    *) exit 1 ;;
esac
""",
        encoding="ascii",
    )
    path.chmod(0o755)


def _write_fake_apptainer(path: Path) -> None:
    path.write_text(
        """#!/bin/sh
set -eu
test "$1" = build
test "$2" = --sandbox
mkdir -p "$3"
""",
        encoding="ascii",
    )
    path.chmod(0o755)
