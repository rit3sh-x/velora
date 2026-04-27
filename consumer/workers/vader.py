"""VADER sentiment worker.

Polls TimescaleDB for unscored tweets, runs VADER on each, and upserts
``sentiment_scored`` rows with the compound score and label. Wakes every
``settings.vader_interval_seconds`` seconds.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from consumer.config import settings
from consumer.workers.preprocess import preprocess

log = logging.getLogger("velora.worker.vader")

_FETCH_SQL = """
SELECT t.tweet_id, t.coin, t.ts, t.text
FROM raw_tweets t
LEFT JOIN sentiment_scored s
  ON s.tweet_id = t.tweet_id AND s.ts = t.ts
WHERE s.compound_vader IS NULL
ORDER BY t.scraped_at DESC
LIMIT 1000
"""

_UPSERT_SQL = """
INSERT INTO sentiment_scored
  (tweet_id, coin, ts, compound_vader, label_vader, scored_at_vader)
VALUES ($1, $2, $3, $4, $5, NOW())
ON CONFLICT (tweet_id, ts) DO UPDATE
SET compound_vader   = EXCLUDED.compound_vader,
    label_vader      = EXCLUDED.label_vader,
    scored_at_vader  = NOW()
"""


def _label(compound: float) -> str:
    if compound >= 0.05:
        return "positive"
    if compound <= -0.05:
        return "negative"
    return "neutral"


def _score_batch(
    rows: list[asyncpg.Record],
) -> list[tuple[str, str, object, float, str]]:
    """CPU-bound: preprocess + VADER score. Run in a thread."""
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

    analyzer = SentimentIntensityAnalyzer()
    out: list[tuple[str, str, object, float, str]] = []
    for row in rows:
        cleaned = preprocess(row["text"])
        if not cleaned:
            compound = 0.0
        else:
            compound = float(analyzer.polarity_scores(cleaned)["compound"])
        out.append(
            (
                row["tweet_id"],
                row["coin"],
                row["ts"],
                compound,
                _label(compound),
            )
        )
    return out


async def _tick(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        rows = await conn.fetch(_FETCH_SQL)

    if not rows:
        log.debug("vader: no unscored tweets")
        return 0

    scored = await asyncio.to_thread(_score_batch, list(rows))

    async with pool.acquire() as conn:
        await conn.executemany(_UPSERT_SQL, scored)

    log.info("vader: scored %d tweets", len(scored))
    return len(scored)


async def run(pool: asyncpg.Pool) -> None:
    """Long-running VADER scoring loop. Cancel-safe."""
    interval = settings.vader_interval_seconds
    log.info("vader worker started (interval=%ss)", interval)
    try:
        while True:
            try:
                await _tick(pool)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("vader tick failed")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        log.info("vader worker cancelled")
        raise
