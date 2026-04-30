"""Historical tweet backfill via Nitter date-bounded search.

Run via the `seed` package entrypoint AFTER the producer + consumer are up.
Walks N days back per coin, scraping all paginated tweets per window via
Playwright. Publishes each tweet to Kafka as a TweetEvent so the existing
consumer ingests it normally. Idempotent — safe to re-run; cheap when data
is fresh (per-coin skip on healthy 24h tweet count).
"""
from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from producer.config import settings
from producer.kafka_client import publish
from producer.playwright_scraper import scrape_coin
from producer.tweet_producer import _to_event
from shared.coins import COINS, CoinSpec
from shared.topics import TWEETS

log = logging.getLogger("velora.seed.tweets")


async def needs_backfill(
    pool: asyncpg.Pool,
    coin: str,
    min_recent_tweets: int = 200,
    window_hours: int = 24,
) -> bool:
    """True if we should backfill — i.e. fewer than `min_recent_tweets` in last `window_hours`."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    row = await pool.fetchrow(
        "SELECT COUNT(*) AS n, MIN(ts) AS oldest FROM raw_tweets "
        "WHERE coin = $1 AND ts >= $2",
        coin,
        cutoff,
    )
    if not row:
        return True
    return (row["n"] or 0) < min_recent_tweets


def _date_windows(days: int) -> list[tuple[str, str]]:
    """Build [(since, until), ...] from `days` ago through tomorrow.

    Each window is one calendar day in YYYY-MM-DD; `until` is exclusive on
    Twitter's API, so we hand it tomorrow to capture all of today.
    """
    today = datetime.now(timezone.utc).date()
    windows: list[tuple[str, str]] = []
    for offset in range(days, -1, -1):
        d = today - timedelta(days=offset)
        windows.append((d.isoformat(), (d + timedelta(days=1)).isoformat()))
    return windows


async def _backfill_coin(spec: CoinSpec) -> int:
    windows = _date_windows(settings.tweet_backfill_days)
    log.info(
        "[%s] backfill %d windows from %s to %s",
        spec.coin,
        len(windows),
        windows[0][0],
        windows[-1][1],
    )

    published = 0
    for since, until in windows:
        try:
            raw_tweets = await scrape_coin(
                coin=spec.coin,
                query=spec.nitter_query,
                instance=settings.nitter_url,
                limit=settings.tweet_backfill_max_per_day,
                timeout_ms=settings.playwright_timeout_ms,
                max_pages=settings.tweet_backfill_pages_per_day,
                since=since,
                until=until,
                min_faves=settings.tweet_backfill_min_faves,
            )
        except Exception:
            log.exception("[%s] window %s..%s scrape failed", spec.coin, since, until)
            continue

        scraped_at = datetime.now(timezone.utc)
        for raw in raw_tweets:
            try:
                event = _to_event(spec.coin, raw, scraped_at)
            except Exception as exc:
                log.warning("[%s] event build failed: %s", spec.coin, exc)
                continue
            publish(TWEETS, key=spec.coin, event=event)
            published += 1

        log.info(
            "[%s] window %s..%s -> %d tweets (cumulative %d)",
            spec.coin,
            since,
            until,
            len(raw_tweets),
            published,
        )

    log.info("[%s] backfill done: %d tweets published", spec.coin, published)
    return published


async def backfill_tweets(pool: asyncpg.Pool | None = None) -> None:
    """Parallel per-coin historical backfill (skip-if-fresh per coin). Idempotent.

    `pool` is optional; when provided, each coin is checked against `raw_tweets`
    and skipped if it already has enough recent data. When not provided, all
    coins are backfilled (legacy behavior).
    """
    if not settings.tweet_backfill_enabled:
        log.info("tweet backfill disabled by config")
        return

    log.info(
        "tweet backfill starting: %d coins, %d days back, min_faves=%d",
        len(COINS),
        settings.tweet_backfill_days,
        settings.tweet_backfill_min_faves,
    )

    targets: list[CoinSpec] = []
    for spec in COINS:
        if pool is not None and not await needs_backfill(pool, spec.coin):
            row = await pool.fetchrow(
                "SELECT COUNT(*) AS n, MIN(ts) AS oldest FROM raw_tweets "
                "WHERE coin = $1 AND ts >= $2",
                spec.coin,
                datetime.now(timezone.utc) - timedelta(hours=24),
            )
            n = row["n"] if row else 0
            oldest = row["oldest"] if row else None
            log.info("[%s] skipping backfill: have %d rows since %s", spec.coin, n, oldest)
            continue
        targets.append(spec)

    if not targets:
        log.info("tweet backfill: nothing to do (all coins fresh)")
        return

    counts = await asyncio.gather(
        *(_backfill_coin(spec) for spec in targets),
        return_exceptions=True,
    )
    total = sum(c for c in counts if isinstance(c, int))
    log.info("tweet backfill complete: %d total tweets published", total)
