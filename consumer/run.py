"""Velora consumer supervisor.

Boot order:
  1. apply schema (idempotent)
  2. init asyncpg pool
  3. spawn 5 long-running tasks: prices, tweets, vader, bert, aggregator
  4. host uvicorn for the FastAPI app

NO preseed. App starts cold; data fills in as workers run.
"""
from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import uvicorn

from consumer.api.main import app
from consumer.config import settings
from consumer.consumers import prices, tweets
from consumer.db import apply_schema, close_pool, init_pool
from consumer.workers import aggregator, bert, vader


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("velora")

    log.info("applying schema...")
    await apply_schema()

    log.info("initializing db pool...")
    pool = await init_pool()

    log.info("spawning workers + uvicorn on %s:%d", settings.api_host, settings.api_port)

    config = uvicorn.Config(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level="info",
    )
    server = uvicorn.Server(config)

    tasks = [
        asyncio.create_task(prices.run(pool), name="prices"),
        asyncio.create_task(tweets.run(pool), name="tweets"),
        asyncio.create_task(vader.run(pool), name="vader"),
        asyncio.create_task(bert.run(pool), name="bert"),
        asyncio.create_task(aggregator.run(pool), name="aggregator"),
        asyncio.create_task(server.serve(), name="api"),
    ]

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("shutting down...")
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await close_pool()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
