from __future__ import annotations

import argparse
import logging
from pathlib import Path

from miry.pipeline.config import load_pull_config
from miry.pipeline.pull import run_pull


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull durable raw chunks from the collector")
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = load_pull_config(args.config)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if run_pull(config):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
