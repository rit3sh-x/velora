"""Historical tweet backfill via Nitter date-bounded search.

Walks N days back per coin, one day-window per Nitter search, scraping
all paginated tweets ('Load more' cursor) up to per-day caps. Publishes
each tweet to Kafka as a TweetEvent so the existing consumer ingests it
into raw_tweets normally. Idempotent (PK conflict in raw_tweets).

Runs once at producer startup before the live rotator begins.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from producer.config import settings
from producer.kafka_client import publish
from producer.playwright_scraper import scrape_coin
from producer.tweet_producer import _to_event
from shared.coins import COINS, CoinSpec
from shared.topics import TWEETS

log = logging.getLogger("velora.producer.backfill")


def _date_windows(days: int) -> list[tuple[str, str]]:
    """Build [(since, until), ...] from `days` ago through tomorrow.

    Each window is one calendar day in YYYY-MM-DD; until is exclusive on
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


async def backfill_tweets() -> None:
    """Parallel per-coin historical backfill. Idempotent."""
    if not settings.tweet_backfill_enabled:
        log.info("tweet backfill disabled by config")
        return

    log.info(
        "tweet backfill starting: %d coins, %d days back, min_faves=%d",
        len(COINS),
        settings.tweet_backfill_days,
        settings.tweet_backfill_min_faves,
    )
    counts = await asyncio.gather(
        *(_backfill_coin(spec) for spec in COINS),
        return_exceptions=True,
    )
    total = sum(c for c in counts if isinstance(c, int))
    log.info("tweet backfill complete: %d total tweets published", total)
