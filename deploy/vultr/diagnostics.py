#!/usr/bin/python3
from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

CONTAINER = "miry-data-collector-collector-1"
DATA_ROOT = Path("/srv/miry-data-rsync")
OUTPUT_ROOT = Path("/var/log/miry-data-collector/diagnostics")
PROBE_URL = "https://fstream.binance.com/"
RETENTION_DAYS = 14


def parse_ss(content: str) -> list[dict[str, object]]:
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    sockets: list[dict[str, object]] = []
    for index in range(0, len(lines), 2):
        fields = lines[index].split()
        if len(fields) < 5:
            continue
        metrics = lines[index + 1] if index + 1 < len(lines) else ""
        rtt = _metric(metrics, "rtt")
        rtt_parts = rtt.split("/", 1) if isinstance(rtt, str) else []
        retrans = _metric(metrics, "retrans")
        retrans_parts = retrans.split("/", 1) if isinstance(retrans, str) else []
        sockets.append(
            {
                "state": fields[0],
                "recv_q": int(fields[1]),
                "send_q": int(fields[2]),
                "local": fields[3],
                "peer": fields[4],
                "rto_ms": _float_metric(metrics, "rto"),
                "rtt_ms": float(rtt_parts[0]) if rtt_parts else None,
                "rtt_variance_ms": float(rtt_parts[1]) if len(rtt_parts) == 2 else None,
                "cwnd": _int_metric(metrics, "cwnd"),
                "bytes_received": _int_metric(metrics, "bytes_received"),
                "retransmits": int(retrans_parts[-1]) if retrans_parts else 0,
            }
        )
    return sockets


def append_sample(
    root: Path,
    sample: dict[str, Any],
    *,
    observed_at: datetime,
    retention_days: int = RETENTION_DAYS,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{observed_at.date().isoformat()}.jsonl"
    payload = {"observed_at": observed_at.isoformat(), **sample}
    with path.open("ab") as target:
        target.write(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii"))
        target.write(b"\n")
    cutoff = observed_at.date() - timedelta(days=retention_days - 1)
    for candidate in root.glob("????-??-??.jsonl"):
        try:
            candidate_date = date.fromisoformat(candidate.stem)
        except ValueError:
            continue
        if candidate_date < cutoff:
            candidate.unlink()
    return path


def collect() -> dict[str, Any]:
    errors: list[str] = []
    pid = _container_pid(errors)
    sample: dict[str, Any] = {
        "container": {"name": CONTAINER, "pid": pid},
        "errors": errors,
    }
    if pid is not None:
        sample["tcp"] = _netns_metrics(pid, errors)
        sample["cgroup"] = _cgroup_metrics(pid, errors)
    sample["pressure"] = {
        name: _pressure(Path(f"/proc/pressure/{name}"), errors)
        for name in ("cpu", "memory", "io")
    }
    sample["probe"] = _probe(errors)
    sample["transfer"] = _json_file(DATA_ROOT / "control/transfer-status.json", errors)
    open_root = DATA_ROOT / "control/open-gaps"
    sample["open_gap_count"] = (
        sum(path.is_file() for path in open_root.iterdir()) if open_root.exists() else 0
    )
    try:
        disk = os.statvfs(DATA_ROOT)
        sample["disk"] = {
            "free_bytes": disk.f_bavail * disk.f_frsize,
            "total_bytes": disk.f_blocks * disk.f_frsize,
        }
    except OSError as exc:
        errors.append(f"disk:{exc!r}")
    return sample


def main() -> int:
    observed_at = datetime.now(UTC)
    root = Path(os.environ.get("MIRY_DIAGNOSTICS_ROOT", OUTPUT_ROOT))
    append_sample(root, collect(), observed_at=observed_at)
    return 0


def _container_pid(errors: list[str]) -> int | None:
    output = _run(
        ("/usr/bin/docker", "inspect", "-f", "{{.State.Pid}}", CONTAINER), errors
    )
    try:
        pid = int(output.strip())
    except ValueError:
        errors.append("container:invalid pid")
        return None
    return pid if pid > 0 else None


def _netns_metrics(pid: int, errors: list[str]) -> dict[str, Any]:
    prefix = ("/usr/bin/nsenter", "-t", str(pid), "-n")
    nstat = _run((*prefix, "/usr/bin/nstat", "-asz"), errors)
    counters: dict[str, int] = {}
    for line in nstat.splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[0] in {
            "TcpRetransSegs",
            "TcpExtTCPTimeouts",
            "TcpExtTCPSynRetrans",
        }:
            counters[fields[0]] = int(fields[1])
    sockets = _run((*prefix, "/usr/bin/ss", "-tinH", "( dport = :443 )"), errors)
    return {"counters": counters, "sockets": parse_ss(sockets)}


def _cgroup_metrics(pid: int, errors: list[str]) -> dict[str, Any]:
    try:
        cgroup = next(
            line.split(":", 2)[2]
            for line in Path(f"/proc/{pid}/cgroup").read_text(encoding="ascii").splitlines()
            if line.startswith("0::")
        )
    except (OSError, StopIteration) as exc:
        errors.append(f"cgroup:path:{exc!r}")
        return {}
    root = Path("/sys/fs/cgroup") / cgroup.removeprefix("/")
    return {
        "path": cgroup,
        "cpu": _key_values(root / "cpu.stat", errors),
        "memory_current": _integer_file(root / "memory.current", errors),
        "memory_events": _key_values(root / "memory.events", errors),
    }


def _pressure(path: Path, errors: list[str]) -> dict[str, dict[str, float | int]]:
    result: dict[str, dict[str, float | int]] = {}
    try:
        lines = path.read_text(encoding="ascii").splitlines()
    except OSError as exc:
        errors.append(f"pressure:{path.name}:{exc!r}")
        return result
    for line in lines:
        fields = line.split()
        result[fields[0]] = {
            key: (int(value) if key == "total" else float(value))
            for key, value in (field.split("=", 1) for field in fields[1:])
        }
    return result


def _probe(errors: list[str]) -> dict[str, Any]:
    template = (
        '{"remote_ip":"%{remote_ip}","http_code":%{http_code},'
        '"dns_s":%{time_namelookup},"connect_s":%{time_connect},'
        '"tls_s":%{time_appconnect},"ttfb_s":%{time_starttransfer},'
        '"total_s":%{time_total}}'
    )
    output = _run(
        (
            "/usr/bin/curl",
            "-sS",
            "-o",
            "/dev/null",
            "--connect-timeout",
            "5",
            "--max-time",
            "10",
            "-w",
            template,
            PROBE_URL,
        ),
        errors,
    )
    try:
        value = json.loads(output)
    except json.JSONDecodeError as exc:
        errors.append(f"probe:json:{exc!r}")
        return {}
    if not isinstance(value, dict):
        errors.append("probe:json:not an object")
        return {}
    return value


def _run(command: tuple[str, ...], errors: list[str]) -> str:
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=12,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        errors.append(f"command:{command[0]}:{exc!r}")
        return ""
    if result.returncode != 0:
        errors.append(f"command:{command[0]}:exit={result.returncode}:{result.stderr[:200]}")
    return result.stdout


def _key_values(path: Path, errors: list[str]) -> dict[str, int]:
    try:
        return {
            key: int(value)
            for key, value in (
                line.split(None, 1)
                for line in path.read_text(encoding="ascii").splitlines()
                if line.strip()
            )
        }
    except (OSError, ValueError) as exc:
        errors.append(f"key-values:{path}:{exc!r}")
        return {}


def _integer_file(path: Path, errors: list[str]) -> int | None:
    try:
        return int(path.read_text(encoding="ascii").strip())
    except (OSError, ValueError) as exc:
        errors.append(f"integer:{path}:{exc!r}")
        return None


def _json_file(path: Path, errors: list[str]) -> Any:
    try:
        return json.loads(path.read_bytes())
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"json:{path}:{exc!r}")
        return None


def _metric(content: str, name: str) -> str | None:
    match = re.search(rf"(?:^|\s){re.escape(name)}:([^\s]+)", content)
    return match.group(1) if match else None


def _float_metric(content: str, name: str) -> float | None:
    value = _metric(content, name)
    return float(value) if value is not None else None


def _int_metric(content: str, name: str) -> int | None:
    value = _metric(content, name)
    return int(value) if value is not None else None


if __name__ == "__main__":
    raise SystemExit(main())
