"""Live sentiment rolling-window worker.

Every ``settings.live_sentiment_interval_seconds`` seconds, for each
coin in ``COIN_NAMES``, computes a rolling snapshot from the most recent
100 sentiment-scored rows (no time-window cutoff -- V21) and upserts a
per-minute bucket into ``coin_live_sentiment``.

``oldest_age_sec`` reports staleness so the dashboard can show a "stale"
badge when the rolling sample is old.

When no scored tweets exist at all, the bucket is still upserted with
``n_tweets = 0`` and NULL averages.
"""

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


_RECENT_SQL = """
WITH recent AS (
    SELECT compound_vader, compound_bert, ts
    FROM sentiment_scored
    WHERE coin = $1
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
INSERT INTO coin_live_sentiment (coin, bucket_ts, n_tweets, avg_vader, avg_bert, oldest_age_sec)
VALUES ($1, date_trunc('minute', NOW()), $2, $3, $4, $5)
ON CONFLICT (coin, bucket_ts) DO UPDATE SET
    n_tweets       = EXCLUDED.n_tweets,
    avg_vader      = EXCLUDED.avg_vader,
    avg_bert       = EXCLUDED.avg_bert,
    oldest_age_sec = EXCLUDED.oldest_age_sec;
"""


async def _update_coin(conn: asyncpg.Connection, coin: str) -> None:
    row = await conn.fetchrow(_RECENT_SQL, coin)

    n = int(row["n"]) if row and row["n"] is not None else 0
    if n == 0:
        await conn.execute(_UPSERT_SQL, coin, 0, None, None, None)
        log.debug("live_sent %s n=0 (stale)", coin)
        return

    avg_v = float(row["avg_v"]) if row["avg_v"] is not None else None
    avg_b = float(row["avg_b"]) if row["avg_b"] is not None else None
    oldest_age_sec = int(row["oldest_age_sec"]) if row["oldest_age_sec"] is not None else None

    await conn.execute(_UPSERT_SQL, coin, n, avg_v, avg_b, oldest_age_sec)

    log.debug(
        "live_sent %s n=%d avg_v=%s avg_b=%s oldest=%ss",
        coin,
        n,
        avg_v,
        avg_b,
        oldest_age_sec,
    )


async def _tick(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        for coin in COIN_NAMES:
            try:
                await _update_coin(conn, coin)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("live_sentiment: failed for coin=%s", coin)
    log.info("live_sentiment: refreshed %d coins", len(COIN_NAMES))


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
