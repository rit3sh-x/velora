"""Velora producer entry point.

Runs the Binance WS consumer and the Nitter tweet rotator concurrently
under one asyncio event loop. Both publish to Kafka.

Run with:
    cd producer && uv run python run.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import asyncio
import logging

from producer import binance_producer, tweet_producer
from producer.kafka_client import close_producer

log = logging.getLogger("velora.producer.run")


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


async def _main() -> None:
    log.info("starting velora producer")
    tasks = [
        asyncio.create_task(binance_producer.run(), name="binance"),
        asyncio.create_task(tweet_producer.run(), name="tweets"),
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        log.info("main cancelled; cancelling child tasks")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


def main() -> None:
    _setup_logging()
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        log.info("keyboard interrupt; shutting down")
    finally:
        close_producer()
        log.info("velora producer stopped")


if __name__ == "__main__":
    main()
