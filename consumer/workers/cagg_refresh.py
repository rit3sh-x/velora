"""Continuous-aggregate refresh worker.

Tight cron-cadence loop. Manually refreshes ``sentiment_hourly`` and
``prices_hourly`` every ``settings.cagg_refresh_interval_seconds``
seconds so live dashboards stay current. Replaces the slow 6h refresh
in ``maintenance.py`` (V18).

``CALL refresh_continuous_aggregate(...)`` cannot run inside a
transaction; asyncpg autocommit is used (see notes in maintenance.py).
"""
from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from consumer.config import settings

log = logging.getLogger("velora.worker.cagg_refresh")

_CAGGS = ("sentiment_hourly", "prices_hourly")


async def _refresh_one(conn: asyncpg.Connection, cagg: str) -> None:
    await conn.execute(f"CALL refresh_continuous_aggregate('{cagg}', NULL, NULL);")


async def _tick(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        for cagg in _CAGGS:
            try:
                await _refresh_one(conn, cagg)
                log.debug("cagg refresh ok: %s", cagg)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("cagg refresh failed: %s", cagg)
    log.info("cagg_refresh: refreshed %s", _CAGGS)


async def run(pool: asyncpg.Pool) -> None:
    """Long-running CAGG refresh loop. Cancel-safe."""
    interval = settings.cagg_refresh_interval_seconds
    log.info("cagg_refresh worker started (interval=%ss)", interval)
    try:
        await asyncio.sleep(5)
        while True:
            try:
                await _tick(pool)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("cagg_refresh tick failed")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        log.info("cagg_refresh worker cancelled")
        raise
