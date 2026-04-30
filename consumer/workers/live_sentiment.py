from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from consumer.config import settings
from shared.coins import COIN_NAMES

log = logging.getLogger("velora.worker.live_sentiment")

_SOURCES = ("twitter", "bluesky")


_RECENT_SQL = """
WITH recent AS (
    SELECT compound_vader, compound_bert, ts
    FROM sentiment_scored
    WHERE coin = $1
      AND source = $2
      AND compound_vader IS NOT NULL
    ORDER BY ts DESC
    LIMIT 100
)
SELECT
    COUNT(*)::INT                                                       AS n,
    AVG(compound_vader)::DOUBLE PRECISION                               AS avg_v,
    AVG(compound_bert)::DOUBLE PRECISION                                AS avg_b,
    EXTRACT(EPOCH FROM (NOW() - MIN(ts)))::INT                          AS oldest_age_sec
FROM recent;
"""

_UPSERT_SQL = """
INSERT INTO coin_live_sentiment (coin, source, bucket_ts, n_tweets, avg_vader, avg_bert, oldest_age_sec)
VALUES ($1, $2, date_trunc('minute', NOW()), $3, $4, $5, $6)
ON CONFLICT (coin, source, bucket_ts) DO UPDATE SET
    n_tweets       = EXCLUDED.n_tweets,
    avg_vader      = EXCLUDED.avg_vader,
    avg_bert       = EXCLUDED.avg_bert,
    oldest_age_sec = EXCLUDED.oldest_age_sec;
"""


async def _update(conn: asyncpg.Connection, coin: str, source: str) -> None:
    row = await conn.fetchrow(_RECENT_SQL, coin, source)
    n = int(row["n"]) if row and row["n"] is not None else 0
    if n == 0:
        await conn.execute(_UPSERT_SQL, coin, source, 0, None, None, None)
        return

    avg_v = float(row["avg_v"]) if row["avg_v"] is not None else None
    avg_b = float(row["avg_b"]) if row["avg_b"] is not None else None
    oldest_age_sec = int(row["oldest_age_sec"]) if row["oldest_age_sec"] is not None else None

    await conn.execute(_UPSERT_SQL, coin, source, n, avg_v, avg_b, oldest_age_sec)


async def _tick(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        for coin in COIN_NAMES:
            for source in _SOURCES:
                try:
                    await _update(conn, coin, source)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("live_sentiment: failed coin=%s source=%s", coin, source)
    log.info("live_sentiment: refreshed %d coins × %d sources", len(COIN_NAMES), len(_SOURCES))


async def run(pool: asyncpg.Pool) -> None:
    """Long-running live sentiment loop. Cancel-safe."""
    interval = settings.live_sentiment_interval_seconds
    log.info("live_sentiment worker started (interval=%ss)", interval)
    try:
        while True:
            try:
                await _tick(pool)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("live_sentiment tick failed")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        log.info("live_sentiment worker cancelled")
        raise
