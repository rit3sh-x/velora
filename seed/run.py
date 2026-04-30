"""One-shot seed runner. Run AFTER consumer + producer are up.

    uv run python -m seed

Connects to Postgres for prices backfill (DB-direct).
Connects to Kafka for tweets backfill (publishes events; consumer ingests).
Both seeders are idempotent and skip-if-fresh.
"""
from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from consumer.db import init_pool, close_pool
from producer.kafka_client import close_producer
from seed.prices import backfill_prices
from seed.tweets import backfill_tweets

log = logging.getLogger("velora.seed")


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log.info("seed: prices first (DB direct), then tweets (Kafka)")

    pool = await init_pool()
    try:
        await backfill_prices(pool)
        try:
            await backfill_tweets(pool)
        finally:
            try:
                close_producer()
            except Exception:
                pass
    finally:
        await close_pool()

    log.info("seed: done")


if __name__ == "__main__":
    asyncio.run(main())
