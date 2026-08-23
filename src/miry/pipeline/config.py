from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field


class PullConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    host: str = Field(min_length=1)
    port: int = Field(default=22, ge=1, le=65535)
    username: str = Field(min_length=1)
    client_key: Path
    known_hosts: Path
    remote_ready_root: str = "ready"
    remote_ack_root: str = "control/acks"
    local_raw_root: Path
    local_staging_root: Path
    connect_timeout_seconds: int = Field(default=20, ge=1, le=120)
    io_timeout_seconds: int = Field(default=120, ge=30, le=900)
    rsync_binary: Path = Path("/usr/bin/rsync")
    ssh_binary: Path = Path("/usr/bin/ssh")


def load_pull_config(path: Path) -> PullConfig:
    with path.open("rb") as source:
        raw = yaml.safe_load(source)
    return PullConfig.model_validate(raw)
