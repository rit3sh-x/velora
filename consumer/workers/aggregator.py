"""Per-coin summary aggregator.

Every ``settings.aggregator_interval_seconds`` seconds, recomputes
``aggregates_summary`` for each coin in ``COIN_NAMES``: 24h post stats
(from the ``sentiment_hourly`` continuous aggregate), spot price,
24h/1h price change and volume, and lag1/lag2 correlation between
hourly sentiment and the next-hour price return.
"""

from __future__ import annotations

import asyncio
import logging
import math
import sys
from pathlib import Path

import asyncpg
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from consumer.config import settings
from shared.coins import COIN_NAMES

log = logging.getLogger("velora.worker.aggregator")


_POSTS_24H_SQL = """
SELECT
  COUNT(*)::INT                                                              AS total,
  COALESCE(AVG(compound_vader), 0)::DOUBLE PRECISION                          AS avg_v,
  COALESCE(AVG(compound_bert), 0)::DOUBLE PRECISION                           AS avg_b,
  COALESCE(AVG(CASE WHEN compound_vader >  0.05 THEN 1.0 ELSE 0.0 END) * 100, 0)::DOUBLE PRECISION AS pos,
  COALESCE(AVG(CASE WHEN compound_vader < -0.05 THEN 1.0 ELSE 0.0 END) * 100, 0)::DOUBLE PRECISION AS neg
FROM sentiment_scored
WHERE coin = $1 AND ts > NOW() - INTERVAL '24 hours' AND compound_vader IS NOT NULL
"""

_LAST_PRICE_SQL = (
    "SELECT close FROM prices_1m WHERE coin = $1 ORDER BY ts DESC LIMIT 1"
)
_PRICE_24H_AGO_SQL = (
    "SELECT close FROM prices_1m WHERE coin = $1 "
    "AND ts <= NOW() - INTERVAL '24 hours' ORDER BY ts DESC LIMIT 1"
)
_PRICE_1H_AGO_SQL = (
    "SELECT close FROM prices_1m WHERE coin = $1 "
    "AND ts <= NOW() - INTERVAL '1 hour' ORDER BY ts DESC LIMIT 1"
)
_VOLUME_24H_SQL = (
    "SELECT COALESCE(SUM(volume), 0) FROM prices_1m "
    "WHERE coin = $1 AND ts > NOW() - INTERVAL '24 hours'"
)
_VOLUME_1H_SQL = (
    "SELECT COALESCE(SUM(volume), 0) FROM prices_1m "
    "WHERE coin = $1 AND ts > NOW() - INTERVAL '1 hour'"
)

_LAG_BUCKET = "1 hour"

_LAG_SQL = """
WITH price_buckets AS (
    SELECT bucket, close AS price
    FROM prices_hourly
    WHERE coin = $1 AND bucket > NOW() - INTERVAL '7 days'
),
sent_buckets AS (
    SELECT bucket, avg_compound_vader, avg_compound_bert, post_count
    FROM sentiment_hourly
    WHERE coin = $1 AND bucket > NOW() - INTERVAL '7 days'
)
SELECT COALESCE(s.bucket, p.bucket) AS hour,
       s.avg_compound_vader,
       s.avg_compound_bert,
       s.post_count,
       p.price AS price
FROM sent_buckets s
FULL OUTER JOIN price_buckets p ON p.bucket = s.bucket
ORDER BY hour
"""

_UPSERT_SQL = """
INSERT INTO aggregates_summary (
    coin, total_posts_24h, avg_sentiment_24h, avg_sentiment_bert_24h,
    positive_pct_24h, negative_pct_24h,
    last_price, change_24h_pct, change_1h_pct, volume_24h, volume_1h,
    lag1_corr, lag2_corr, lag1_corr_bert, lag2_corr_bert,
    lag1_points, lag2_points,
    computed_at
) VALUES (
    $1, $2, $3, $4,
    $5, $6,
    $7, $8, $9, $10, $11,
    $12, $13, $14, $15,
    $16, $17,
    NOW()
)
ON CONFLICT (coin) DO UPDATE SET
    total_posts_24h        = EXCLUDED.total_posts_24h,
    avg_sentiment_24h      = EXCLUDED.avg_sentiment_24h,
    avg_sentiment_bert_24h = EXCLUDED.avg_sentiment_bert_24h,
    positive_pct_24h       = EXCLUDED.positive_pct_24h,
    negative_pct_24h       = EXCLUDED.negative_pct_24h,
    last_price             = EXCLUDED.last_price,
    change_24h_pct         = EXCLUDED.change_24h_pct,
    change_1h_pct          = EXCLUDED.change_1h_pct,
    volume_24h             = EXCLUDED.volume_24h,
    volume_1h              = EXCLUDED.volume_1h,
    lag1_corr              = EXCLUDED.lag1_corr,
    lag2_corr              = EXCLUDED.lag2_corr,
    lag1_corr_bert         = EXCLUDED.lag1_corr_bert,
    lag2_corr_bert         = EXCLUDED.lag2_corr_bert,
    lag1_points            = EXCLUDED.lag1_points,
    lag2_points            = EXCLUDED.lag2_points,
    computed_at            = NOW()
"""


def _pct_change(now: float | None, past: float | None) -> float | None:
    if now is None or past is None:
        return None
    if past == 0:
        return None
    return (now - past) / past * 100.0


def _safe_corr(value: float | None) -> float | None:
    """Convert a pandas correlation result to a JSON-friendly float or None."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return float(value)


def _compute_lag(rows: list[asyncpg.Record]) -> dict[str, object | None]:
    """Build lag1/lag2 correlation stats from hourly sentiment+price rows."""
    if not rows:
        return {
            "lag1_corr": None,
            "lag2_corr": None,
            "lag1_corr_bert": None,
            "lag2_corr_bert": None,
            "lag1_points": 0,
            "lag2_points": 0,
        }

    df = pd.DataFrame(
        [
            {
                "hour": r["hour"],
                "avg_compound_vader": r["avg_compound_vader"],
                "avg_compound_bert": r["avg_compound_bert"],
                "post_count": r["post_count"],
                "price": r["price"],
            }
            for r in rows
        ]
    )

    df["return_1h"] = df["price"].pct_change()
    df["sent_ff"] = df["avg_compound_vader"].ffill(limit=3)
    df["bert_ff"] = df["avg_compound_bert"].ffill(limit=3)
    df["sent_lag1"] = df["sent_ff"].shift(1)
    df["sent_lag2"] = df["sent_ff"].shift(2)
    df["bert_lag1"] = df["bert_ff"].shift(1)
    df["bert_lag2"] = df["bert_ff"].shift(2)

    lag1_points = int(df.dropna(subset=["sent_lag1", "return_1h"]).shape[0])
    lag2_points = int(df.dropna(subset=["sent_lag2", "return_1h"]).shape[0])
    bert_lag1_points = int(df.dropna(subset=["bert_lag1", "return_1h"]).shape[0])
    bert_lag2_points = int(df.dropna(subset=["bert_lag2", "return_1h"]).shape[0])

    lag1_corr = _safe_corr(df["sent_lag1"].corr(df["return_1h"])) if lag1_points >= 3 else None
    lag2_corr = _safe_corr(df["sent_lag2"].corr(df["return_1h"])) if lag2_points >= 3 else None
    lag1_corr_bert = _safe_corr(df["bert_lag1"].corr(df["return_1h"])) if bert_lag1_points >= 3 else None
    lag2_corr_bert = _safe_corr(df["bert_lag2"].corr(df["return_1h"])) if bert_lag2_points >= 3 else None

    return {
        "lag1_corr": lag1_corr,
        "lag2_corr": lag2_corr,
        "lag1_corr_bert": lag1_corr_bert,
        "lag2_corr_bert": lag2_corr_bert,
        "lag1_points": lag1_points,
        "lag2_points": lag2_points,
    }


async def _update_coin(conn: asyncpg.Connection, coin: str) -> None:
    posts = await conn.fetchrow(_POSTS_24H_SQL, coin)
    last_price = await conn.fetchval(_LAST_PRICE_SQL, coin)
    price_24h = await conn.fetchval(_PRICE_24H_AGO_SQL, coin)
    price_1h = await conn.fetchval(_PRICE_1H_AGO_SQL, coin)
    volume_24h = await conn.fetchval(_VOLUME_24H_SQL, coin)
    volume_1h = await conn.fetchval(_VOLUME_1H_SQL, coin)
    lag_rows = await conn.fetch(_LAG_SQL, coin)

    total = int(posts["total"]) if posts else 0
    avg_v = float(posts["avg_v"]) if posts else 0.0
    avg_b = float(posts["avg_b"]) if posts else 0.0
    pos_pct = float(posts["pos"]) if posts else 0.0
    neg_pct = float(posts["neg"]) if posts else 0.0

    last_price_f = float(last_price) if last_price is not None else None
    change_24h = _pct_change(last_price_f, float(price_24h) if price_24h is not None else None)
    change_1h = _pct_change(last_price_f, float(price_1h) if price_1h is not None else None)

    volume_24h_f = float(volume_24h) if volume_24h is not None else 0.0
    volume_1h_f = float(volume_1h) if volume_1h is not None else 0.0

    lag = _compute_lag(list(lag_rows))

    await conn.execute(
        _UPSERT_SQL,
        coin,
        total,
        avg_v,
        avg_b,
        pos_pct,
        neg_pct,
        last_price_f,
        change_24h,
        change_1h,
        volume_24h_f,
        volume_1h_f,
        lag["lag1_corr"],
        lag["lag2_corr"],
        lag["lag1_corr_bert"],
        lag["lag2_corr_bert"],
        lag["lag1_points"],
        lag["lag2_points"],
    )

    log.debug(
        "agg %s posts=%d price=%s ch24=%s ch1=%s lag1=%s",
        coin,
        total,
        last_price_f,
        change_24h,
        change_1h,
        lag["lag1_corr"],
    )


async def _tick(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        for coin in COIN_NAMES:
            try:
                await _update_coin(conn, coin)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("aggregator: failed for coin=%s", coin)
    log.info("aggregator: refreshed %d coins", len(COIN_NAMES))


async def run(pool: asyncpg.Pool) -> None:
    """Long-running aggregator loop. Cancel-safe."""
    interval = settings.aggregator_interval_seconds
    log.info("aggregator worker started (interval=%ss)", interval)
    try:
        while True:
            try:
                await _tick(pool)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("aggregator tick failed")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        log.info("aggregator worker cancelled")
        raise
