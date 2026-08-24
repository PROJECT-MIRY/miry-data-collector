#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class Job:
    name: str
    state: str
    cpus: int
    memory_gib: float


def main() -> None:
    parser = argparse.ArgumentParser(description="Show automated 107 processing progress")
    parser.add_argument("--base", type=Path, default=Path("/home/scc/pb24000367/Projects/bn"))
    parser.add_argument("--collector", default="tokyo01")
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args()
    jobs = load_jobs()
    running = [job for job in jobs if job.state == "RUNNING"]
    print(datetime.now().astimezone().strftime("%F %T %Z"))
    print(
        f"allocated CPU={sum(job.cpus for job in running)}/32 "
        f"memory={sum(job.memory_gib for job in running):.1f}/128GiB "
        f"running={len(running)}"
    )
    print("date        raw       normalize  inputs     L2          final       scheduler")

    for utc_date in processing_dates(args.base, args.collector)[-args.days :]:
        raw_day = (
            args.base
            / "data/raw"
            / f"collector={args.collector}"
            / "day-manifests"
            / f"date={utc_date}"
        )
        typed = (
            args.base
            / "data/derived/typed"
            / f"collector={args.collector}"
            / f"date={utc_date}"
        )
        inputs = (
            args.base
            / "data/derived/l2-inputs"
            / f"collector={args.collector}"
            / f"date={utc_date}"
        )
        quality = (
            args.base
            / "data/derived/quality"
            / f"collector={args.collector}"
            / f"date={utc_date}"
        )
        checkpoints = sum(1 for _ in quality.glob("symbol=*/l2-checkpoint.json"))
        active = [job for job in jobs if utc_date in job.name]
        l2_jobs = [job for job in active if job.name.startswith("miry-l2-")]
        l2_state = f"{checkpoints:02d}/60"
        if l2_jobs:
            l2_state += f" {aggregate_state(l2_jobs)}"
        final = "processed" if (quality / "_PROCESSED.json").is_file() else "waiting"
        if (quality / "_QUALITY_REJECTED.json").is_file():
            final = "rejected"
        elif any(job.name.startswith("miry-finalize-") for job in active):
            final = job_stage(active, "miry-finalize-")
        raw_state = "sealed" if (raw_day / "SEALED.json").is_file() else "waiting"
        normalized = "done" if (typed / "_NORMALIZED.json").is_file() else job_stage(
            active, "miry-norm-"
        )
        if (inputs / "_L2_INPUTS.json").is_file():
            input_state = "done"
        elif final in {"processed", "rejected"}:
            input_state = "cleaned"
        else:
            input_state = job_stage(active, "miry-inputs-")
        print(
            f"{utc_date}  {raw_state:9s} {normalized:9s}  {input_state:9s}  "
            f"{l2_state:11s} {final:11s} {scheduler_state(args.base, utc_date)}"
        )


def processing_dates(base: Path, collector: str) -> list[str]:
    roots = (
        base / "data/raw" / f"collector={collector}" / "day-manifests",
        base / "data/derived/quality" / f"collector={collector}",
        base / "runtime/status/processing/submissions",
    )
    values = {
        path.name.removeprefix("date=").removesuffix(".submitting")
        for root in roots
        if root.is_dir()
        for path in root.glob("date=*")
    }
    return sorted(value for value in values if len(value) == 10)


def scheduler_state(base: Path, utc_date: str) -> str:
    root = base / "runtime/status/processing/submissions"
    if (root / f"date={utc_date}").is_dir():
        return "submitted"
    if (root / f"date={utc_date}.submitting").is_dir():
        return "partial"
    return "waiting"


def load_jobs() -> list[Job]:
    user = subprocess.check_output(["id", "-un"], text=True).strip()
    result = subprocess.run(
        ["squeue", "-h", "-u", user, "-o", "%j|%T|%C|%m"],
        check=True,
        capture_output=True,
        text=True,
    )
    jobs = []
    for line in result.stdout.splitlines():
        name, state, cpus, memory = line.split("|", 3)
        jobs.append(
            Job(
                name=name.strip(),
                state=state.strip(),
                cpus=int(cpus),
                memory_gib=parse_memory(memory.strip()),
            )
        )
    return jobs


def job_stage(jobs: list[Job], prefix: str) -> str:
    selected = [job for job in jobs if job.name.startswith(prefix)]
    return aggregate_state(selected) if selected else "waiting"


def aggregate_state(jobs: list[Job]) -> str:
    states = {job.state for job in jobs}
    if "RUNNING" in states:
        return "running"
    if "PENDING" in states:
        return "pending"
    return "waiting"


def parse_memory(value: str) -> float:
    suffix = value[-1:].upper()
    number = float(value[:-1]) if suffix in {"K", "M", "G", "T"} else float(value)
    return number * {"K": 1 / 1024**2, "M": 1 / 1024, "G": 1, "T": 1024}.get(
        suffix, 1 / 1024**3
    )


if __name__ == "__main__":
    main()
