from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DIAGNOSTICS = PROJECT_ROOT / "deploy" / "vultr" / "diagnostics.py"


def _load_diagnostics():
    spec = importlib.util.spec_from_file_location("miry_host_diagnostics", DIAGNOSTICS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_socket_diagnostics_parse_route_evidence() -> None:
    module = _load_diagnostics()
    sockets = module.parse_ss(
        """ESTAB 0 0 172.20.0.2:46984 18.177.33.103:443
 bbr wscale:8,10 rto:209 rtt:8.231/2.276 cwnd:44 bytes_received:1181010314 retrans:0/2
"""
    )

    assert sockets == [
        {
            "state": "ESTAB",
            "recv_q": 0,
            "send_q": 0,
            "local": "172.20.0.2:46984",
            "peer": "18.177.33.103:443",
            "rto_ms": 209.0,
            "rtt_ms": 8.231,
            "rtt_variance_ms": 2.276,
            "cwnd": 44,
            "bytes_received": 1_181_010_314,
            "retransmits": 2,
        }
    ]


def test_diagnostics_retention_keeps_fourteen_days(tmp_path: Path) -> None:
    module = _load_diagnostics()
    now = datetime(2026, 8, 23, 12, tzinfo=UTC)
    old = tmp_path / "2026-08-08.jsonl"
    recent = tmp_path / "2026-08-10.jsonl"
    old.write_text("old\n", encoding="ascii")
    recent.write_text("recent\n", encoding="ascii")

    path = module.append_sample(tmp_path, {"state": "ok"}, observed_at=now)

    assert path == tmp_path / "2026-08-23.jsonl"
    assert not old.exists()
    assert recent.exists()
    assert '"state":"ok"' in path.read_text(encoding="ascii")
    assert (now.date() - timedelta(days=13)).isoformat() == recent.stem
