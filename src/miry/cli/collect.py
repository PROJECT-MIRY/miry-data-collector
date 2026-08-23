from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from pathlib import Path

from miry.collector.config import CollectorConfig, load_collector_config
from miry.collector.service import CollectorService


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Binance USD-M collector")
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = load_collector_config(args.config)
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_run(config))


async def _run(config: CollectorConfig) -> None:
    service = CollectorService(config)
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(name, service.request_stop)
    await service.run()


if __name__ == "__main__":
    main()
