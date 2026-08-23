#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class Job:
    name: str
    state: str
    elapsed: int
    cpus: int
    memory_gib: float
    reason: str


def main() -> None:
    parser = argparse.ArgumentParser(description="Show optimized 107 processing progress")
    parser.add_argument("--base", type=Path, default=Path("/home/scc/pb24000367/Projects/bn"))
    args = parser.parse_args()
    jobs = load_jobs()
    running = [job for job in jobs if job.state == "RUNNING"]
    print(datetime.now().astimezone().strftime("%F %T %Z"))
    print(
        f"allocated: CPU {sum(job.cpus for job in running):2d}/32  "
        f"memory {sum(job.memory_gib for job in running):5.1f}/128 GiB  "
        f"running jobs {len(running)}"
    )
    print("date        track    normalize                 L2 progress      final       active/ETA")

    rows = [*(("legacy", day) for day in range(11, 22)), ("current", 22)]
    for track, day in rows:
        utc_date = f"2026-08-{day:02d}"
        compact = utc_date.replace("-", "")
        derived = args.base / "data/derived"
        if track == "legacy":
            derived /= "legacy"
        typed = derived / "typed" / "collector=tokyo01" / f"date={utc_date}"
        quality = derived / "quality" / "collector=tokyo01" / f"date={utc_date}"

        norm_jobs = named(jobs, f"opt-norm-{compact}")
        l2_jobs = named(jobs, f"opt-l2-{compact}")
        fin_jobs = named(jobs, f"opt-fin-{compact}")
        normalized = (typed / "_NORMALIZED.json").is_file()
        norm_text = "done"
        norm_eta = ""
        if not normalized:
            completed, total = normalize_bytes(
                args.base,
                utc_date=utc_date,
                typed_root=typed,
            )
            norm_text = byte_progress(completed, total)
            norm_eta = estimate(norm_jobs, completed, total)

        l2_done = count_files(quality, "symbol=*/l2-checkpoint.json")
        l2_text = l2_progress(l2_done, 60, l2_jobs)
        l2_eta = estimate(l2_jobs, l2_done, 60)
        if (quality / "_PROCESSED.json").is_file():
            final = "processed"
        elif (quality / "_QUALITY_REJECTED.json").is_file():
            final = "rejected"
        elif l2_done == 60 and any(
            path.stat().st_size == 0 for path in quality.glob("symbol=*/l2-validity.jsonl")
        ):
            final = "rejected(empty)"
        else:
            final = aggregate_state(fin_jobs)

        active = active_stage(norm_jobs, l2_jobs, fin_jobs)
        eta = l2_eta or norm_eta
        suffix = f"{active} {eta}".strip()
        print(f"{utc_date}  {track:7s}  {norm_text:24s}  {l2_text:15s}  {final:10s}  {suffix}")


def load_jobs() -> list[Job]:
    result = subprocess.run(
        [
            "squeue",
            "-h",
            "-u",
            subprocess.check_output(["id", "-un"], text=True).strip(),
            "-o",
            "%j|%T|%M|%C|%m|%R",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    jobs = []
    for line in result.stdout.splitlines():
        name, state, elapsed, cpus, memory, reason = line.split("|", 5)
        jobs.append(
            Job(
                name=name.strip(),
                state=state.strip(),
                elapsed=parse_elapsed(elapsed.strip()),
                cpus=int(cpus),
                memory_gib=parse_memory(memory.strip()),
                reason=reason.strip(),
            )
        )
    return jobs


def named(jobs: list[Job], name: str) -> list[Job]:
    return [job for job in jobs if job.name == name]


def count_files(root: Path, pattern: str) -> int:
    if not root.is_dir():
        return 0
    paths = root.glob(pattern) if "/" in pattern else root.rglob(pattern)
    return sum(1 for _ in paths)


def normalize_bytes(base: Path, *, utc_date: str, typed_root: Path) -> tuple[int, int]:
    manifest_path = (
        base / "data/raw/collector=tokyo01/day-manifests" / f"date={utc_date}" / "SEALED.json"
    )
    if not manifest_path.is_file():
        return 0, 0
    day = json.loads(manifest_path.read_bytes())
    sizes = {
        Path(chunk["data_path"]).stem: int(chunk["size_bytes"])
        for chunk in day["chunks"]
        if chunk["content_type"] == "application/vnd.apache.parquet"
    }
    completed = sum(
        sizes.get(path.name.removesuffix(".typed.parquet"), 0)
        for path in typed_root.glob("*.typed.parquet")
    )
    return completed, sum(sizes.values())


def progress(done: int, total: int) -> str:
    if total <= 0:
        return "waiting"
    width = 8
    filled = min(width, round(width * done / total))
    return f"[{'#' * filled}{'.' * (width - filled)}] {done:4d}/{total:<4d}"


def byte_progress(done: int, total: int) -> str:
    if total <= 0:
        return "waiting"
    width = 8
    filled = min(width, round(width * done / total))
    gib = 1024**3
    return f"[{'#' * filled}{'.' * (width - filled)}] {done / gib:4.1f}/{total / gib:4.1f}G"


def l2_progress(done: int, total: int, jobs: list[Job]) -> str:
    running = sum(job.state == "RUNNING" for job in jobs)
    waiting = max(0, total - done - running)
    return f"{done:02d}/{total:02d} +{running:02d}R {waiting:02d}W"


def estimate(jobs: list[Job], done: int, total: int) -> str:
    running = [job for job in jobs if job.state == "RUNNING"]
    if not running or done <= 0 or done >= total:
        return ""
    elapsed = max(job.elapsed for job in running)
    seconds = int(elapsed * (total - done) / done)
    return f"ETA {format_seconds(seconds)}"


def active_stage(*groups: list[Job]) -> str:
    for label, jobs in zip(("norm", "L2", "fin"), groups, strict=True):
        state = aggregate_state(jobs)
        if state in {"running", "pending"}:
            return f"{label}:{state}"
    return ""


def aggregate_state(jobs: list[Job]) -> str:
    states = {job.state for job in jobs}
    if "RUNNING" in states:
        return "running"
    if "PENDING" in states:
        return "pending"
    return "waiting"


def parse_elapsed(value: str) -> int:
    days = 0
    if "-" in value:
        raw_days, value = value.split("-", 1)
        days = int(raw_days)
    parts = [int(item) for item in value.split(":")]
    if len(parts) == 2:
        hours, minutes, seconds = 0, parts[0], parts[1]
    else:
        hours, minutes, seconds = parts
    return ((days * 24 + hours) * 60 + minutes) * 60 + seconds


def parse_memory(value: str) -> float:
    suffix = value[-1:].upper()
    number = float(value[:-1]) if suffix in {"K", "M", "G", "T"} else float(value)
    return number * {"K": 1 / 1024**2, "M": 1 / 1024, "G": 1, "T": 1024}.get(suffix, 1 / 1024**3)


def format_seconds(value: int) -> str:
    hours, remainder = divmod(max(0, value), 3600)
    minutes, _seconds = divmod(remainder, 60)
    return f"{hours:d}h{minutes:02d}m" if hours else f"{minutes:d}m"


if __name__ == "__main__":
    main()
