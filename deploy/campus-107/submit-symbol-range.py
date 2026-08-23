#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Submit per-symbol L2 jobs with same-symbol cross-day dependencies"
    )
    parser.add_argument("start", type=date.fromisoformat)
    parser.add_argument("end", type=date.fromisoformat)
    parser.add_argument("--derived-root", type=Path, required=True)
    parser.add_argument("--collector", required=True)
    parser.add_argument("--l2-script", type=Path, required=True)
    parser.add_argument("--finalize-script", type=Path, required=True)
    parser.add_argument("--log-root", type=Path, required=True)
    parser.add_argument("--output-plan", type=Path, required=True)
    parser.add_argument("--previous-plan", type=Path)
    parser.add_argument("--external-job", action="append", default=[])
    parser.add_argument("--gate", action="append", default=[])
    parser.add_argument("--account", required=True)
    parser.add_argument("--partition", required=True)
    parser.add_argument("--qos", required=True)
    args = parser.parse_args()

    if args.end < args.start:
        raise ValueError("end date is before start date")
    external_jobs = parse_assignments(args.external_job)
    gates = parse_assignments(args.gate)
    previous_jobs, previous_date = load_previous_plan(args.previous_plan)
    args.log_root.mkdir(parents=True, exist_ok=True)
    plan: dict[str, Any] = {"schema_version": 1, "dates": {}}

    current = args.start
    while current <= args.end:
        expected_previous = current - timedelta(days=1)
        if previous_jobs and previous_date != expected_previous:
            raise ValueError(
                f"previous job map is for {previous_date}, expected {expected_previous}"
            )
        symbols = load_symbols(args.derived_root, args.collector, current)
        previous_symbols = load_symbols(
            args.derived_root, args.collector, expected_previous, required=False
        )
        current_jobs: dict[str, str] = {}
        completed_symbols = []
        gate = gates.get(current.isoformat())

        for symbol in symbols:
            if checkpoint_exists(args.derived_root, args.collector, current, symbol):
                completed_symbols.append(symbol)
                continue
            dependencies = []
            if symbol in previous_symbols:
                if not checkpoint_exists(
                    args.derived_root, args.collector, expected_previous, symbol
                ):
                    previous_job = previous_jobs.get(symbol)
                    if previous_job is not None:
                        dependencies.append(previous_job)
                    else:
                        try:
                            dependencies.append(external_jobs[symbol])
                        except KeyError as exc:
                            raise ValueError(
                                f"missing prior checkpoint dependency for {symbol} on {current}"
                            ) from exc
            if gate is not None:
                dependencies.append(gate)
            current_jobs[symbol] = submit_symbol(
                args,
                utc_date=current,
                symbol=symbol,
                dependencies=dependencies,
            )

        finalize_job = (
            None
            if quality_result_exists(args.derived_root, args.collector, current)
            else submit_finalize(args, utc_date=current, dependencies=tuple(current_jobs.values()))
        )
        plan["dates"][current.isoformat()] = {
            "symbols": current_jobs,
            "completed_symbols": completed_symbols,
            "finalize_job": finalize_job,
        }
        atomic_json(args.output_plan, plan)
        print(
            f"date={current} symbol_jobs={len(current_jobs)} finalize={finalize_job}",
            flush=True,
        )
        previous_jobs = current_jobs
        previous_date = current
        current += timedelta(days=1)


def load_symbols(
    derived_root: Path, collector: str, utc_date: date, *, required: bool = True
) -> tuple[str, ...]:
    marker = (
        derived_root
        / "typed"
        / f"collector={collector}"
        / f"date={utc_date.isoformat()}"
        / "_NORMALIZED.json"
    )
    if not marker.is_file() and not required:
        return ()
    value = json.loads(marker.read_bytes())
    symbols = value.get("expected_symbols")
    if (
        not isinstance(symbols, list)
        or len(symbols) != 60
        or any(not isinstance(symbol, str) or not symbol for symbol in symbols)
        or len(set(symbols)) != 60
    ):
        raise ValueError(f"normalized marker has no authoritative 60-symbol universe: {marker}")
    return tuple(symbols)


def checkpoint_exists(derived_root: Path, collector: str, utc_date: date, symbol: str) -> bool:
    return (
        derived_root
        / "quality"
        / f"collector={collector}"
        / f"date={utc_date.isoformat()}"
        / f"symbol={symbol}"
        / "l2-checkpoint.json"
    ).is_file()


def quality_result_exists(derived_root: Path, collector: str, utc_date: date) -> bool:
    root = derived_root / "quality" / f"collector={collector}" / f"date={utc_date.isoformat()}"
    return (root / "_PROCESSED.json").is_file() or (root / "_QUALITY_REJECTED.json").is_file()


def submit_symbol(
    args: argparse.Namespace,
    *,
    utc_date: date,
    symbol: str,
    dependencies: list[str],
) -> str:
    command = sbatch_base(args)
    if dependencies:
        command.append(f"--dependency=afterok:{':'.join(dict.fromkeys(dependencies))}")
    command.extend(
        (
            f"--job-name=opt-l2-{utc_date:%Y%m%d}",
            f"--output={args.log_root}/symbol-l2-{utc_date}-{symbol}-%j.out",
            f"--error={args.log_root}/symbol-l2-{utc_date}-{symbol}-%j.err",
            str(args.l2_script),
        )
    )
    environment = {**os.environ, "MIRY_RANGE_DATE": utc_date.isoformat()}
    environment["MIRY_RANGE_SYMBOL"] = symbol
    return run_sbatch(command, environment=environment)


def submit_finalize(
    args: argparse.Namespace, *, utc_date: date, dependencies: tuple[str, ...]
) -> str:
    command = sbatch_base(args)
    if dependencies:
        command.append(f"--dependency=afterok:{':'.join(dependencies)}")
    command.extend(
        (
            f"--job-name=opt-fin-{utc_date:%Y%m%d}",
            f"--output={args.log_root}/symbol-fin-{utc_date}-%j.out",
            f"--error={args.log_root}/symbol-fin-{utc_date}-%j.err",
            str(args.finalize_script),
        )
    )
    environment = {**os.environ, "MIRY_RANGE_DATE": utc_date.isoformat()}
    environment.pop("MIRY_RANGE_SYMBOL", None)
    return run_sbatch(command, environment=environment)


def sbatch_base(args: argparse.Namespace) -> list[str]:
    return [
        "sbatch",
        "--parsable",
        f"--account={args.account}",
        f"--partition={args.partition}",
        f"--qos={args.qos}",
    ]


def run_sbatch(command: list[str], *, environment: dict[str, str]) -> str:
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    return result.stdout.strip().split(";", 1)[0]


def parse_assignments(values: list[str]) -> dict[str, str]:
    result = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key or not item:
            raise ValueError(f"invalid KEY=VALUE assignment: {value}")
        result[key] = item
    return result


def load_previous_plan(path: Path | None) -> tuple[dict[str, str], date | None]:
    if path is None:
        return {}, None
    value = json.loads(path.read_bytes())
    dates = value.get("dates")
    if not isinstance(dates, dict) or not dates:
        raise ValueError(f"invalid previous plan: {path}")
    latest = max(date.fromisoformat(value) for value in dates)
    latest_value = dates[latest.isoformat()]
    symbols = latest_value.get("symbols")
    completed = latest_value.get("completed_symbols", [])
    if (
        not isinstance(symbols, dict)
        or not isinstance(completed, list)
        or any(not isinstance(symbol, str) for symbol in completed)
        or any(
            not isinstance(key, str) or not isinstance(item, str) for key, item in symbols.items()
        )
    ):
        raise ValueError(f"invalid previous symbol jobs: {path}")
    return symbols, latest


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as destination:
            json.dump(value, destination, sort_keys=True, separators=(",", ":"))
            destination.write("\n")
            destination.flush()
            os.fsync(destination.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
