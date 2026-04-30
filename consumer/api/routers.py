from __future__ import annotations
from shared.coins import BY_COIN, COINS
from consumer.api.deps import db
from fastapi import APIRouter, Depends, HTTPException, Query
import pandas as pd
import numpy as np
import asyncpg

import asyncio
import logging
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


log = logging.getLogger("velora.api")

router = APIRouter()


def _iso_z(dt: datetime | None) -> str | None:
    """Format a tz-aware datetime as ISO8601 UTC with `Z` suffix."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    s = dt.isoformat(timespec="milliseconds")
    return s.replace("+00:00", "Z")


def _clean(value: Any) -> Any:
    """Normalize a single value: NaN -> None, numpy scalars -> python scalars,
    timestamps -> ISO Z strings."""
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (np.floating,)):
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        if isinstance(value, pd.Timestamp):
            value = value.to_pydatetime()
        return _iso_z(value)
    return value


def _df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a DataFrame to list[dict] with NaN→None and ts→ISO Z."""
    if df.empty:
        return []
    df = df.replace({np.nan: None})
    records = df.to_dict(orient="records")
    out: list[dict[str, Any]] = []
    for row in records:
        out.append({k: _clean(v) for k, v in row.items()})
    return out


def _resolve_coin(coin: str) -> str:
    """Lowercase + validate coin path param. Raises 404 on unknown."""
    key = coin.lower()
    if key not in BY_COIN:
        raise HTTPException(status_code=404, detail=f"unknown coin: {coin}")
    return key


def _add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Append ema12/ema26/sma20/sma50 columns."""
    close = df["close"]
    df["ema12"] = close.ewm(span=12, adjust=False).mean()
    df["ema26"] = close.ewm(span=26, adjust=False).mean()
    df["sma20"] = close.rolling(20, min_periods=1).mean()
    df["sma50"] = close.rolling(50, min_periods=1).mean()
    return df

import time as _time

_CACHE: dict[str, tuple[float, Any]] = {}
_CACHE_TTL_SEC = 30.0


def _cache_get(key: str) -> Any | None:
    entry = _CACHE.get(key)
    if entry is None:
        return None
    expires_at, value = entry
    if _time.monotonic() > expires_at:
        _CACHE.pop(key, None)
        return None
    return value


def _cache_set(key: str, value: Any) -> None:
    _CACHE[key] = (_time.monotonic() + _CACHE_TTL_SEC, value)


@router.get("/coins")
async def list_coins(pool: asyncpg.Pool = Depends(db)) -> list[dict[str, Any]]:
    """List all coins with current snapshot from aggregates_summary."""
    cached = _cache_get("coins")
    if cached is not None:
        return cached
    rows = await pool.fetch(
        """
        SELECT coin, last_price, change_24h_pct, volume_24h
        FROM aggregates_summary
        """
    )
    snap: dict[str, dict[str, Any]] = {r["coin"]: dict(r) for r in rows}

    out: list[dict[str, Any]] = []
    for spec in COINS:
        s = snap.get(spec.coin, {})
        out.append(
            {
                "coin": spec.coin,
                "symbol": spec.symbol,
                "display": spec.display,
                "last_price": _clean(s.get("last_price")),
                "change_24h_pct": _clean(s.get("change_24h_pct")),
                "volume_24h": _clean(s.get("volume_24h")) or 0.0,
            }
        )
    _cache_set("coins", out)
    return out


@router.get("/search")
async def search(
    q: str = Query(..., min_length=1),
) -> list[dict[str, str]]:
    """Substring filter on coin/symbol/display (case-insensitive)."""
    needle = q.lower()
    out = []
    for spec in COINS:
        if (
            needle in spec.coin.lower()
            or needle in spec.symbol.lower()
            or needle in spec.display.lower()
        ):
            out.append(
                {
                    "coin": spec.coin,
                    "symbol": spec.symbol,
                    "display": spec.display,
                }
            )
    return out


@router.get("/global")
async def global_stats(pool: asyncpg.Pool = Depends(db)) -> dict[str, Any]:
    """Aggregate sentiment across all coins. avg_sentiment is post-weighted."""
    cached = _cache_get("global")
    if cached is not None:
        return cached
    rows = await pool.fetch(
        """
        SELECT coin, total_posts_24h, avg_sentiment_24h,
               positive_pct_24h, negative_pct_24h
        FROM aggregates_summary
        """
    )

    total_posts = 0
    weighted_sum = 0.0
    pos_weighted = 0.0
    neg_weighted = 0.0
    for r in rows:
        n = int(r["total_posts_24h"] or 0)
        if n <= 0:
            continue
        total_posts += n
        weighted_sum += float(r["avg_sentiment_24h"] or 0.0) * n
        pos_weighted += float(r["positive_pct_24h"] or 0.0) * n
        neg_weighted += float(r["negative_pct_24h"] or 0.0) * n

    if total_posts > 0:
        avg_sentiment = weighted_sum / total_posts
        positive_pct = pos_weighted / total_posts
        negative_pct = neg_weighted / total_posts
    else:
        avg_sentiment = 0.0
        positive_pct = 0.0
        negative_pct = 0.0

    out = {
        "total_posts": int(total_posts),
        "avg_sentiment": round(avg_sentiment, 4),
        "positive_pct": round(positive_pct, 2),
        "negative_pct": round(negative_pct, 2),
    }
    _cache_set("global", out)
    return out


@router.get("/rankings")
async def rankings(pool: asyncpg.Pool = Depends(db)) -> list[dict[str, Any]]:
    """Coins ranked by post volume desc."""
    cached = _cache_get("rankings")
    if cached is not None:
        return cached
    rows = await pool.fetch(
        """
        SELECT coin, total_posts_24h, avg_sentiment_24h
        FROM aggregates_summary
        ORDER BY total_posts_24h DESC NULLS LAST
        """
    )
    out = [
        {
            "coin": r["coin"],
            "post_volume": int(r["total_posts_24h"] or 0),
            "avg_compound": _clean(r["avg_sentiment_24h"]) or 0.0,
        }
        for r in rows
    ]
    _cache_set("rankings", out)
    return out


@router.get("/trending")
async def trending(pool: asyncpg.Pool = Depends(db)) -> list[dict[str, Any]]:
    """Top movers in the last 1h, sorted by abs(change_1h_pct) desc."""
    cached = _cache_get("trending")
    if cached is not None:
        return cached
    rows = await pool.fetch(
        """
        SELECT coin, last_price, change_1h_pct, volume_1h
        FROM aggregates_summary
        WHERE change_1h_pct IS NOT NULL
        ORDER BY ABS(change_1h_pct) DESC
        """
    )
    out: list[dict[str, Any]] = []
    for r in rows:
        spec = BY_COIN.get(r["coin"])
        if spec is None:
            continue
        out.append(
            {
                "coin": r["coin"],
                "symbol": spec.symbol,
                "last_price": _clean(r["last_price"]) or 0.0,
                "change_1h_pct": _clean(r["change_1h_pct"]) or 0.0,
                "volume_1h": _clean(r["volume_1h"]) or 0.0,
            }
        )
    _cache_set("trending", out)
    return out


@router.get("/coin/{coin}/candles")
async def candles(
    coin: str,
    hours: int = Query(24, ge=1, le=24),
    pool: asyncpg.Pool = Depends(db),
) -> list[dict[str, Any]]:
    """OHLCV bars + indicators, 1m granularity, last `hours` hours, ascending."""
    coin = _resolve_coin(coin)
    rows = await pool.fetch(
        """
        SELECT ts, open, high, low, close, volume
        FROM prices_1m
        WHERE coin = $1
          AND ts >= NOW() - ($2::int * INTERVAL '1 hour')
        ORDER BY ts ASC
        """,
        coin,
        hours,
    )
    if not rows:
        return []

    def _build() -> list[dict[str, Any]]:
        df = pd.DataFrame(
            [dict(r) for r in rows],
            columns=["ts", "open", "high", "low", "close", "volume"],
        )
        df = df.sort_values("ts").reset_index(drop=True)
        df = _add_indicators(df)
        return _df_to_records(df)

    return await asyncio.to_thread(_build)


@router.get("/coin/{coin}/sentiment/hourly")
async def sentiment_hourly(
    coin: str,
    days: int = Query(7, ge=1, le=7),
    source: str | None = Query(None, pattern="^(twitter|bluesky)$"),
    pool: asyncpg.Pool = Depends(db),
) -> list[dict[str, Any]]:
    """Hourly sentiment buckets, last `days` days. Default = combined across sources."""
    coin = _resolve_coin(coin)
    if source is not None:
        rows = await pool.fetch(
            """
            SELECT
                bucket AS ts,
                avg_compound_vader::DOUBLE PRECISION AS avg_compound,
                avg_compound_bert::DOUBLE PRECISION  AS avg_compound_bert,
                post_count::INT                      AS post_count,
                ROUND(post_count * pos_ratio)::INT                    AS positive_count,
                ROUND(post_count * neg_ratio)::INT                    AS negative_count,
                ROUND(post_count * (1 - pos_ratio - neg_ratio))::INT  AS neutral_count,
                (pos_ratio * 100)::DOUBLE PRECISION                   AS positive_pct,
                (neg_ratio * 100)::DOUBLE PRECISION                   AS negative_pct,
                ((1 - pos_ratio - neg_ratio) * 100)::DOUBLE PRECISION AS neutral_pct
            FROM sentiment_hourly
            WHERE coin = $1 AND source = $2
              AND bucket >= NOW() - ($3::int * INTERVAL '1 day')
            ORDER BY bucket ASC
            """,
            coin, source, days,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT
                bucket AS ts,
                SUM(avg_compound_vader * post_count) / NULLIF(SUM(post_count), 0)::DOUBLE PRECISION AS avg_compound,
                SUM(avg_compound_bert  * post_count) / NULLIF(SUM(post_count), 0)::DOUBLE PRECISION AS avg_compound_bert,
                SUM(post_count)::INT                                                                AS post_count,
                ROUND(SUM(post_count * pos_ratio))::INT                                             AS positive_count,
                ROUND(SUM(post_count * neg_ratio))::INT                                             AS negative_count,
                ROUND(SUM(post_count * (1 - pos_ratio - neg_ratio)))::INT                           AS neutral_count,
                (SUM(pos_ratio * post_count) / NULLIF(SUM(post_count), 0) * 100)::DOUBLE PRECISION  AS positive_pct,
                (SUM(neg_ratio * post_count) / NULLIF(SUM(post_count), 0) * 100)::DOUBLE PRECISION  AS negative_pct,
                (SUM((1 - pos_ratio - neg_ratio) * post_count) / NULLIF(SUM(post_count), 0) * 100)::DOUBLE PRECISION AS neutral_pct
            FROM sentiment_hourly
            WHERE coin = $1
              AND bucket >= NOW() - ($2::int * INTERVAL '1 day')
            GROUP BY bucket
            ORDER BY bucket ASC
            """,
            coin, days,
        )
    return [
        {
            "ts": _iso_z(r["ts"]),
            "avg_compound": _clean(r["avg_compound"]) or 0.0,
            "avg_compound_bert": _clean(r["avg_compound_bert"]) or 0.0,
            "post_count": int(r["post_count"] or 0),
            "positive_count": int(r["positive_count"] or 0),
            "negative_count": int(r["negative_count"] or 0),
            "neutral_count": int(r["neutral_count"] or 0),
            "positive_pct": _clean(r["positive_pct"]) or 0.0,
            "negative_pct": _clean(r["negative_pct"]) or 0.0,
            "neutral_pct": _clean(r["neutral_pct"]) or 0.0,
        }
        for r in rows
    ]


@router.get("/coin/{coin}/price-sentiment")
async def price_sentiment(
    coin: str,
    days: int = Query(7, ge=1, le=7),
    source: str | None = Query(None, pattern="^(twitter|bluesky)$"),
    pool: asyncpg.Pool = Depends(db),
) -> list[dict[str, Any]]:
    """Hourly price merged w/ hourly sentiment + return_1h + sent lags.
    Default = combined across sources; `?source=` filter for split."""
    coin = _resolve_coin(coin)

    if source is not None:
        sent_rows = await pool.fetch(
            """
            SELECT
                bucket AS ts,
                avg_compound_vader::DOUBLE PRECISION AS avg_compound,
                avg_compound_bert::DOUBLE PRECISION  AS avg_compound_bert,
                post_count::INT                      AS post_count,
                (pos_ratio * 100)::DOUBLE PRECISION  AS positive_pct,
                (neg_ratio * 100)::DOUBLE PRECISION  AS negative_pct,
                ((1 - pos_ratio - neg_ratio) * 100)::DOUBLE PRECISION AS neutral_pct
            FROM sentiment_hourly
            WHERE coin = $1 AND source = $2
              AND bucket >= NOW() - ($3::int * INTERVAL '1 day')
            ORDER BY bucket ASC
            """,
            coin, source, days,
        )
    else:
        sent_rows = await pool.fetch(
            """
            SELECT
                bucket AS ts,
                SUM(avg_compound_vader * post_count) / NULLIF(SUM(post_count), 0)::DOUBLE PRECISION AS avg_compound,
                SUM(avg_compound_bert  * post_count) / NULLIF(SUM(post_count), 0)::DOUBLE PRECISION AS avg_compound_bert,
                SUM(post_count)::INT                                                                AS post_count,
                (SUM(pos_ratio * post_count) / NULLIF(SUM(post_count), 0) * 100)::DOUBLE PRECISION  AS positive_pct,
                (SUM(neg_ratio * post_count) / NULLIF(SUM(post_count), 0) * 100)::DOUBLE PRECISION  AS negative_pct,
                (SUM((1 - pos_ratio - neg_ratio) * post_count) / NULLIF(SUM(post_count), 0) * 100)::DOUBLE PRECISION AS neutral_pct
            FROM sentiment_hourly
            WHERE coin = $1
              AND bucket >= NOW() - ($2::int * INTERVAL '1 day')
            GROUP BY bucket
            ORDER BY bucket ASC
            """,
            coin, days,
        )
    price_rows = await pool.fetch(
        """
        SELECT bucket AS ts,
               close::DOUBLE PRECISION AS price
        FROM prices_hourly
        WHERE coin = $1
          AND bucket >= NOW() - ($2::int * INTERVAL '1 day')
        ORDER BY bucket ASC
        """,
        coin,
        days,
    )

    if not sent_rows and not price_rows:
        return []

    def _build() -> list[dict[str, Any]]:
        sent_df = pd.DataFrame(
            [dict(r) for r in sent_rows],
            columns=[
                "ts",
                "avg_compound",
                "avg_compound_bert",
                "post_count",
                "positive_pct",
                "negative_pct",
                "neutral_pct",
            ],
        )
        price_df = pd.DataFrame(
            [dict(r) for r in price_rows], columns=["ts", "price"]
        )

        if sent_df.empty and price_df.empty:
            return []

        if sent_df.empty:
            merged = price_df.copy()
            for col in ("avg_compound", "avg_compound_bert", "post_count",
                        "positive_pct", "negative_pct", "neutral_pct"):
                merged[col] = None
        elif price_df.empty:
            merged = sent_df.copy()
            merged["price"] = None
        else:
            merged = sent_df.merge(price_df, on="ts", how="outer")

        merged = merged.sort_values("ts").reset_index(drop=True)

        merged["return_1h"] = merged["price"].pct_change()
        merged["sentiment_lag1"] = merged["avg_compound"].shift(1)
        merged["sentiment_lag2"] = merged["avg_compound"].shift(2)
        merged["sentiment_bert_lag1"] = merged["avg_compound_bert"].shift(1)
        merged["sentiment_bert_lag2"] = merged["avg_compound_bert"].shift(2)

        return _df_to_records(merged)

    return await asyncio.to_thread(_build)


@router.get("/coin/{coin}/sentiment-live")
async def sentiment_live(
    coin: str,
    hours: int = Query(24, ge=1, le=168),
    pool: asyncpg.Pool = Depends(db),
) -> list[dict[str, Any]]:
    """1-minute-bucket VADER + BERT sentiment for the last N hours.

    Use for live overlays on price charts. Range capped to 7 days (168h)."""
    coin = _resolve_coin(coin)

    rows = await pool.fetch(
        """
        SELECT
            time_bucket('1 minute', ts) AS ts,
            AVG(compound_vader)::DOUBLE PRECISION AS avg_compound,
            AVG(compound_bert)::DOUBLE PRECISION  AS avg_compound_bert,
            COUNT(*)::INT                         AS post_count
        FROM sentiment_scored
        WHERE coin = $1
          AND ts >= NOW() - ($2::int * INTERVAL '1 hour')
          AND compound_vader IS NOT NULL
        GROUP BY 1
        ORDER BY 1 ASC
        """,
        coin,
        hours,
    )
    return [
        {
            "ts": _iso_z(r["ts"]),
            "avg_compound": _clean(r["avg_compound"]),
            "avg_compound_bert": _clean(r["avg_compound_bert"]),
            "post_count": int(r["post_count"] or 0),
        }
        for r in rows
    ]


@router.get("/coin/{coin}/sentiment-live-window")
async def sentiment_live_window(
    coin: str,
    hours: int = Query(24, ge=1, le=168),
    pool: asyncpg.Pool = Depends(db),
) -> list[dict[str, Any]]:
    """Precomputed 1-minute-bucket sentiment for the last N hours.

    Reads from `coin_live_sentiment`, which is upserted every 60s by a worker.
    Range capped to 7 days (168h)."""
    coin = _resolve_coin(coin)

    rows = await pool.fetch(
        """
        SELECT
            bucket_ts,
            SUM(avg_vader * n_tweets) / NULLIF(SUM(n_tweets), 0)::DOUBLE PRECISION AS avg_vader,
            SUM(avg_bert  * n_tweets) / NULLIF(SUM(n_tweets), 0)::DOUBLE PRECISION AS avg_bert,
            SUM(n_tweets)::INT                                                     AS n_tweets,
            MAX(oldest_age_sec)::INT                                               AS oldest_age_sec
        FROM coin_live_sentiment
        WHERE coin = $1
          AND bucket_ts >= NOW() - ($2::int * INTERVAL '1 hour')
        GROUP BY bucket_ts
        ORDER BY bucket_ts ASC
        """,
        coin,
        hours,
    )
    return [
        {
            "ts": _iso_z(r["bucket_ts"]),
            "avg_vader": _clean(r["avg_vader"]),
            "avg_bert": _clean(r["avg_bert"]),
            "n_tweets": int(r["n_tweets"] or 0),
            "oldest_age_sec": _clean(r["oldest_age_sec"]),
        }
        for r in rows
    ]


@router.get("/coin/{coin}/correlation")
async def correlation(
    coin: str,
    pool: asyncpg.Pool = Depends(db),
) -> dict[str, Any]:
    """Per-coin Pearson correlation between lagged sentiment and 1h returns."""
    coin = _resolve_coin(coin)

    summary = await pool.fetchrow(
        """
        SELECT lag1_corr, lag2_corr, lag1_corr_bert, lag2_corr_bert,
               lag1_points, lag2_points
        FROM aggregates_summary
        WHERE coin = $1
        """,
        coin,
    )

    latest_price_row = await pool.fetchrow(
        """
        SELECT ts, close
        FROM prices_1m
        WHERE coin = $1
        ORDER BY ts DESC
        LIMIT 1
        """,
        coin,
    )

    latest_sent_row = await pool.fetchrow(
        """
        SELECT time_bucket('1 hour', ts) AS hour,
               AVG(compound_vader)::DOUBLE PRECISION AS avg_compound_vader
        FROM sentiment_scored
        WHERE coin = $1 AND compound_vader IS NOT NULL
        GROUP BY hour
        ORDER BY hour DESC
        LIMIT 1
        """,
        coin,
    )

    last_two_hourly = await pool.fetch(
        """
        SELECT time_bucket('1 hour', ts) AS hour,
               last(close, ts) AS close
        FROM prices_1m
        WHERE coin = $1
          AND ts > NOW() - INTERVAL '4 hours'
        GROUP BY hour
        ORDER BY hour DESC
        LIMIT 2
        """,
        coin,
    )
    latest_return_1h: float | None = None
    if len(last_two_hourly) == 2:
        newer = last_two_hourly[0]["close"]
        older = last_two_hourly[1]["close"]
        if newer is not None and older not in (None, 0):
            latest_return_1h = (float(newer) - float(older)) / float(older)

    latest_ts = latest_price_row["ts"] if latest_price_row else None
    latest_price = (
        float(latest_price_row["close"])
        if latest_price_row and latest_price_row["close"] is not None
        else 0.0
    )
    latest_sentiment = (
        float(latest_sent_row["avg_compound_vader"])
        if latest_sent_row and latest_sent_row["avg_compound_vader"] is not None
        else 0.0
    )

    return {
        "coin": coin,
        "lag1_points": int(summary["lag1_points"]) if summary else 0,
        "lag2_points": int(summary["lag2_points"]) if summary else 0,
        "lag1_corr": _clean(summary["lag1_corr"]) if summary else None,
        "lag2_corr": _clean(summary["lag2_corr"]) if summary else None,
        "lag1_corr_bert": _clean(summary["lag1_corr_bert"]) if summary else None,
        "lag2_corr_bert": _clean(summary["lag2_corr_bert"]) if summary else None,
        "latest_timestamp": _iso_z(latest_ts) or _iso_z(datetime.now(timezone.utc)),
        "latest_sentiment": latest_sentiment,
        "latest_price": latest_price,
        "latest_return_1h": _clean(latest_return_1h),
    }


@router.get("/coin/{coin}/tweets")
async def tweets(
    coin: str,
    sentiment: str = Query("all", pattern="^(positive|negative|all)$"),
    source: str | None = Query(None, pattern="^(twitter|bluesky)$"),
    limit: int = Query(5, ge=1, le=200),
    since_ms: int | None = Query(
        None, description="Epoch ms lower bound (Grafana ${__from})"),
    until_ms: int | None = Query(
        None, description="Epoch ms upper bound (Grafana ${__to})"),
    pool: asyncpg.Pool = Depends(db),
) -> list[dict[str, Any]]:
    """Top tweets per V39: bronze in Mongo, sentiment in Timescale silver.

    sentiment=all     → Mongo find latest, app-side enrich via silver
    sentiment=±       → silver picks top-N tweet_ids, Mongo enriches text
    """
    from consumer.db_mongo import get_tweets_collection

    coin = _resolve_coin(coin)
    coll = get_tweets_collection()

    ts_filter: dict[str, datetime] = {}
    if since_ms is not None:
        ts_filter["$gte"] = datetime.fromtimestamp(
            since_ms / 1000.0, tz=timezone.utc)
    if until_ms is not None:
        ts_filter["$lte"] = datetime.fromtimestamp(
            until_ms / 1000.0, tz=timezone.utc)

    base_filter: dict[str, Any] = {"coin": coin}
    if ts_filter:
        base_filter["ts"] = ts_filter
    if source is not None:
        base_filter["source"] = source

    projection = {
        "_id": 0, "tweet_id": 1, "ts": 1, "text": 1,
        "username": 1, "url": 1, "source": 1,
    }

    if sentiment == "all":
        cursor = coll.find(base_filter, projection).sort("ts", -1).limit(limit)
        docs = await cursor.to_list(length=limit)
        if not docs:
            return []

        keys = [(d["tweet_id"], d["ts"], d["source"]) for d in docs]
        compounds: dict[tuple[str, datetime, str], float | None] = {}
        if keys:
            rows = await pool.fetch(
                "SELECT tweet_id, ts, source, compound_vader "
                "FROM sentiment_scored "
                "WHERE (tweet_id, ts, source) IN ("
                + ",".join(f"(${i*3+1}, ${i*3+2}, ${i*3+3})" for i in range(len(keys)))
                + ")",
                *[v for k in keys for v in k],
            )
            for r in rows:
                compounds[(r["tweet_id"], r["ts"], r["source"])
                          ] = r["compound_vader"]

        return [
            {
                "ts": _iso_z(d["ts"]),
                "text": d.get("text"),
                "username": d.get("username"),
                "url": d.get("url"),
                "source": d.get("source"),
                "compound": _clean(compounds.get((d["tweet_id"], d["ts"], d["source"]))) or 0.0,
            }
            for d in docs
        ]

    where_extra = " AND coin = $1"
    params: list[Any] = [coin]
    if since_ms is not None:
        params.append(datetime.fromtimestamp(
            since_ms / 1000.0, tz=timezone.utc))
        where_extra += f" AND ts >= ${len(params)}"
    if until_ms is not None:
        params.append(datetime.fromtimestamp(
            until_ms / 1000.0, tz=timezone.utc))
        where_extra += f" AND ts <= ${len(params)}"
    if source is not None:
        params.append(source)
        where_extra += f" AND source = ${len(params)}"
    params.append(limit)
    limit_idx = len(params)

    direction = "DESC" if sentiment == "positive" else "ASC"
    sign_filter = "compound_vader > 0" if sentiment == "positive" else "compound_vader < 0"

    sql = f"""
        SELECT tweet_id, ts, source, compound_vader AS compound
        FROM sentiment_scored
        WHERE compound_vader IS NOT NULL AND {sign_filter}
          {where_extra}
        ORDER BY compound_vader {direction}
        LIMIT ${limit_idx}
    """
    rows = await pool.fetch(sql, *params)
    if not rows:
        return []

    keys = [(r["tweet_id"], r["ts"], r["source"]) for r in rows]
    docs = await coll.find(
        {"$or": [{"tweet_id": tid, "source": src} for tid, _ts, src in keys]},
        {"_id": 0, "tweet_id": 1, "source": 1, "text": 1, "username": 1, "url": 1},
    ).to_list(length=len(keys))
    by_key = {(d["tweet_id"], d["source"]): d for d in docs}

    return [
        {
            "ts": _iso_z(r["ts"]),
            "text": (by_key.get((r["tweet_id"], r["source"])) or {}).get("text"),
            "username": (by_key.get((r["tweet_id"], r["source"])) or {}).get("username"),
            "url": (by_key.get((r["tweet_id"], r["source"])) or {}).get("url"),
            "source": r["source"],
            "compound": _clean(r["compound"]),
        }
        for r in rows
    ]


@router.get("/coin/{coin}/metrics")
async def metrics(
    coin: str,
    pool: asyncpg.Pool = Depends(db),
) -> dict[str, Any]:
    """Per-coin summary card."""
    coin = _resolve_coin(coin)
    spec = BY_COIN[coin]

    row = await pool.fetchrow(
        """
        SELECT total_posts_24h, avg_sentiment_24h, avg_sentiment_bert_24h,
               positive_pct_24h, negative_pct_24h,
               last_price, change_24h_pct
        FROM aggregates_summary
        WHERE coin = $1
        """,
        coin,
    )

    if row is None:
        return {
            "coin": spec.coin,
            "display": spec.display,
            "symbol": spec.symbol,
            "total_posts": 0,
            "avg_sentiment": 0.0,
            "avg_sentiment_bert": 0.0,
            "positive_pct": 0.0,
            "negative_pct": 0.0,
            "last_price": None,
            "change_24h_pct": None,
        }

    return {
        "coin": spec.coin,
        "display": spec.display,
        "symbol": spec.symbol,
        "total_posts": int(row["total_posts_24h"] or 0),
        "avg_sentiment": _clean(row["avg_sentiment_24h"]) or 0.0,
        "avg_sentiment_bert": _clean(row["avg_sentiment_bert_24h"]) or 0.0,
        "positive_pct": _clean(row["positive_pct_24h"]) or 0.0,
        "negative_pct": _clean(row["negative_pct_24h"]) or 0.0,
        "last_price": _clean(row["last_price"]),
        "change_24h_pct": _clean(row["change_24h_pct"]),
    }


@router.get("/ticker/{coin}")
async def ticker(
    coin: str,
    pool: asyncpg.Pool = Depends(db),
) -> dict[str, Any]:
    """Lightweight live tick. 503 if no data yet."""
    coin = _resolve_coin(coin)

    price_row = await pool.fetchrow(
        """
        SELECT close
        FROM prices_1m
        WHERE coin = $1
        ORDER BY ts DESC
        LIMIT 1
        """,
        coin,
    )
    if price_row is None or price_row["close"] is None:
        raise HTTPException(status_code=503, detail="no data")

    summary = await pool.fetchrow(
        "SELECT change_24h_pct FROM aggregates_summary WHERE coin = $1",
        coin,
    )
    change_24h = _clean(summary["change_24h_pct"]) if summary else None

    return {
        "coin": coin,
        "ts": _iso_z(datetime.now(timezone.utc)),
        "price": float(price_row["close"]),
        "change_24h_pct": change_24h if change_24h is not None else 0.0,
    }


@router.get("/coin/{coin}/sentiment-by-source")
async def sentiment_by_source(
    coin: str,
    days: int = Query(7, ge=1, le=7),
    pool: asyncpg.Pool = Depends(db),
) -> list[dict[str, Any]]:
    """Hourly sentiment per (coin, source). One row per (ts, source)."""
    coin = _resolve_coin(coin)
    rows = await pool.fetch(
        """
        SELECT
            bucket AS ts,
            source,
            avg_compound_vader::DOUBLE PRECISION AS avg_compound,
            avg_compound_bert::DOUBLE PRECISION  AS avg_compound_bert,
            post_count::INT                      AS post_count,
            (pos_ratio * 100)::DOUBLE PRECISION  AS positive_pct,
            (neg_ratio * 100)::DOUBLE PRECISION  AS negative_pct
        FROM sentiment_hourly
        WHERE coin = $1
          AND bucket >= NOW() - ($2::int * INTERVAL '1 day')
        ORDER BY bucket ASC, source
        """,
        coin, days,
    )
    return [
        {
            "ts": _iso_z(r["ts"]),
            "source": r["source"],
            "avg_compound": _clean(r["avg_compound"]) or 0.0,
            "avg_compound_bert": _clean(r["avg_compound_bert"]) or 0.0,
            "post_count": int(r["post_count"] or 0),
            "positive_pct": _clean(r["positive_pct"]) or 0.0,
            "negative_pct": _clean(r["negative_pct"]) or 0.0,
        }
        for r in rows
    ]


@router.get("/coin/{coin}/divergence")
async def divergence(
    coin: str,
    days: int = Query(7, ge=1, le=7),
    pool: asyncpg.Pool = Depends(db),
) -> list[dict[str, Any]]:
    """Hourly |vader_twitter - vader_bluesky| series. V34."""
    coin = _resolve_coin(coin)
    rows = await pool.fetch(
        """
        SELECT bucket AS ts,
               MAX(CASE WHEN source='twitter' THEN avg_compound_vader END) AS v_tw,
               MAX(CASE WHEN source='bluesky' THEN avg_compound_vader END) AS v_bs,
               MAX(CASE WHEN source='twitter' THEN post_count END) AS n_tw,
               MAX(CASE WHEN source='bluesky' THEN post_count END) AS n_bs
        FROM sentiment_hourly
        WHERE coin = $1
          AND bucket >= NOW() - ($2::int * INTERVAL '1 day')
        GROUP BY bucket
        ORDER BY bucket ASC
        """,
        coin, days,
    )
    out: list[dict[str, Any]] = []
    for r in rows:
        v_tw = r["v_tw"]
        v_bs = r["v_bs"]
        delta = abs(float(v_tw) - float(v_bs)
                    ) if v_tw is not None and v_bs is not None else None
        out.append({
            "ts": _iso_z(r["ts"]),
            "v_twitter": _clean(v_tw),
            "v_bluesky": _clean(v_bs),
            "divergence": _clean(delta),
            "n_twitter": int(r["n_tw"] or 0),
            "n_bluesky": int(r["n_bs"] or 0),
            "flagged": bool(delta is not None and delta >= 0.4),
        })
    return out


@router.get("/coin/{coin}/posts-by-source")
async def posts_by_source(
    coin: str,
    days: int = Query(7, ge=1, le=7),
    pool: asyncpg.Pool = Depends(db),
) -> list[dict[str, Any]]:
    """Hourly post counts split by source. V47 descriptive."""
    coin = _resolve_coin(coin)
    rows = await pool.fetch(
        """
        SELECT bucket AS ts, source, post_count
        FROM sentiment_hourly
        WHERE coin = $1
          AND bucket >= NOW() - ($2::int * INTERVAL '1 day')
        ORDER BY bucket ASC, source
        """,
        coin, days,
    )
    return [
        {
            "ts": _iso_z(r["ts"]),
            "source": r["source"],
            "post_count": int(r["post_count"] or 0),
        }
        for r in rows
    ]


@router.get("/coin/{coin}/predict")
async def predict_latest(
    coin: str,
    horizon: int | None = Query(
        None, description="Horizon minutes filter (15/60/240). Default = all horizons."),
    pool: asyncpg.Pool = Depends(db),
) -> list[dict[str, Any]]:
    """Latest prediction(s) per horizon. V32."""
    coin = _resolve_coin(coin)
    if horizon is not None:
        rows = await pool.fetch(
            """
            SELECT ts, horizon_minutes, predicted_return_pct, confidence, model_version
            FROM predictions
            WHERE coin = $1 AND horizon_minutes = $2
            ORDER BY ts DESC LIMIT 1
            """,
            coin, horizon,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT DISTINCT ON (horizon_minutes)
                   ts, horizon_minutes, predicted_return_pct, confidence, model_version
            FROM predictions
            WHERE coin = $1
            ORDER BY horizon_minutes ASC, ts DESC
            """,
            coin,
        )
    return [
        {
            "coin": coin,
            "ts": _iso_z(r["ts"]),
            "horizon_minutes": int(r["horizon_minutes"]),
            "predicted_return_pct": _clean(r["predicted_return_pct"]),
            "confidence": _clean(r["confidence"]),
            "model_version": r["model_version"],
        }
        for r in rows
    ]


@router.get("/coin/{coin}/predict-history")
async def predict_history(
    coin: str,
    horizon: int = Query(60, description="Horizon minutes (15/60/240)"),
    days: int = Query(7, ge=1, le=7),
    pool: asyncpg.Pool = Depends(db),
) -> list[dict[str, Any]]:
    """Predictions for one horizon over last `days`. Drives backtest panel. V47."""
    coin = _resolve_coin(coin)
    rows = await pool.fetch(
        """
        SELECT ts, predicted_return_pct, confidence, model_version
        FROM predictions
        WHERE coin = $1 AND horizon_minutes = $2
          AND ts >= NOW() - ($3::int * INTERVAL '1 day')
        ORDER BY ts ASC
        """,
        coin, horizon, days,
    )
    return [
        {
            "ts": _iso_z(r["ts"]),
            "predicted_return_pct": _clean(r["predicted_return_pct"]),
            "confidence": _clean(r["confidence"]),
            "model_version": r["model_version"],
        }
        for r in rows
    ]


@router.get("/coin/{coin}/recommend")
async def recommend_latest(
    coin: str,
    pool: asyncpg.Pool = Depends(db),
) -> dict[str, Any]:
    """Latest signal + score + reasons. V33 + V51."""
    coin = _resolve_coin(coin)
    row = await pool.fetchrow(
        """
        SELECT ts, signal, score, reasons
        FROM recommendations
        WHERE coin = $1
        ORDER BY ts DESC LIMIT 1
        """,
        coin,
    )
    if row is None:
        return {"coin": coin, "ts": None, "signal": "hold", "score": 0.0, "reasons": []}

    import json as _json
    raw = row["reasons"]
    if isinstance(raw, str):
        reasons = _json.loads(raw)
    elif isinstance(raw, list):
        reasons = raw
    else:
        reasons = []

    return {
        "coin": coin,
        "ts": _iso_z(row["ts"]),
        "signal": row["signal"],
        "score": _clean(row["score"]) or 0.0,
        "reasons": reasons,
    }


@router.get("/coin/{coin}/recommend-history")
async def recommend_history(
    coin: str,
    days: int = Query(7, ge=1, le=7),
    pool: asyncpg.Pool = Depends(db),
) -> list[dict[str, Any]]:
    """Signal timeline over last `days`. V33 + V47."""
    coin = _resolve_coin(coin)
    rows = await pool.fetch(
        """
        SELECT ts, signal, score
        FROM recommendations
        WHERE coin = $1
          AND ts >= NOW() - ($2::int * INTERVAL '1 day')
        ORDER BY ts ASC
        """,
        coin, days,
    )
    return [
        {
            "ts": _iso_z(r["ts"]),
            "signal": r["signal"],
            "score": _clean(r["score"]) or 0.0,
        }
        for r in rows
    ]
