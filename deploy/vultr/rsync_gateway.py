#!/usr/bin/python3
from __future__ import annotations

import os
import posixpath
import shlex
import sys
from dataclasses import dataclass

DATA_ROOT = "/srv/miry-data-rsync"
RRSYNC = "/usr/bin/rrsync"


@dataclass(frozen=True)
class RestrictedCommand:
    original_command: str
    rrsync_arguments: tuple[str, ...]


def restrict_command(command: str) -> RestrictedCommand:
    try:
        arguments = shlex.split(command)
    except ValueError as error:
        raise ValueError("invalid SSH command quoting") from error

    if arguments[:2] != ["rsync", "--server"]:
        raise ValueError("only the rsync server protocol is allowed")
    try:
        separator = arguments.index(".", 2)
    except ValueError as error:
        raise ValueError("invalid rsync server command") from error

    paths = arguments[separator + 1 :]
    if len(paths) != 1:
        raise ValueError("exactly one remote path is required")
    remote_path = _normalize_path(paths[0])
    sender = arguments[2:separator][:1] == ["--sender"]

    rewritten = [*arguments[: separator + 1], "."]
    if sender and remote_path == "ready":
        return RestrictedCommand(
            shlex.join(rewritten),
            ("-ro", f"{DATA_ROOT}/ready"),
        )
    if not sender and remote_path == "control/acks":
        return RestrictedCommand(
            shlex.join(rewritten),
            ("-wo", "-no-del", f"{DATA_ROOT}/control/acks"),
        )
    raise ValueError("remote path is not allowed")


def _normalize_path(value: str) -> str:
    if value.startswith("/"):
        raise ValueError("absolute remote paths are not allowed")
    normalized = posixpath.normpath(value)
    if normalized == ".." or normalized.startswith("../"):
        raise ValueError("remote path traversal is not allowed")
    return normalized


def main() -> int:
    try:
        restricted = restrict_command(os.environ.get("SSH_ORIGINAL_COMMAND", ""))
    except ValueError as error:
        print(f"restricted rsync rejected command: {error}", file=sys.stderr)
        return 1

    environment = {**os.environ, "SSH_ORIGINAL_COMMAND": restricted.original_command}
    os.execve(
        RRSYNC,
        (RRSYNC, *restricted.rrsync_arguments),
        environment,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
