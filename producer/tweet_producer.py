"""Tweet producer.

Round-robins through the 6 coins, scraping one per `tweet_poll_interval_seconds`.
Publishes each tweet as a `TweetEvent` to the `velora.tweets` topic.
"""
from __future__ import annotations

import producer

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from producer.config import settings
from producer.kafka_client import publish
from producer.playwright_scraper import scrape_coin
from shared.coins import BY_COIN, COIN_NAMES
from shared.schemas import TweetEvent
from shared.topics import TWEETS

log = logging.getLogger("velora.producer.tweets")

_DATE_FORMATS: tuple[str, ...] = (
    "%b %d, %Y · %I:%M %p UTC",
    "%b %d, %Y · %H:%M UTC",
    "%b %d, %Y · %I:%M %p",
)


def _parse_tweet_date(raw: str | None) -> datetime:
    if not raw:
        raise ValueError("missing tweet date")
    cleaned = raw.strip()
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"unparseable tweet date: {raw!r}")


def _to_event(coin: str, raw: dict[str, Any], scraped_at: datetime) -> TweetEvent:
    return TweetEvent(
        coin=coin,
        tweet_id=raw["tweet_id"],
        ts=_parse_tweet_date(raw.get("date")),
        scraped_at=scraped_at,
        text=raw.get("text") or "",
        username=raw.get("username"),
        likes=int(raw.get("likes", 0) or 0),
        retweets=int(raw.get("retweets", 0) or 0),
        replies=int(raw.get("replies", 0) or 0),
        url=raw.get("url"),
    )


async def _scrape_and_publish(coin: str) -> int:
    spec = BY_COIN[coin]
    raw_tweets = await scrape_coin(
        coin=coin,
        query=spec.nitter_query,
        instance=settings.nitter_url,
        limit=settings.tweets_per_scrape,
        timeout_ms=settings.playwright_timeout_ms,
        max_pages=settings.tweet_max_pages,
    )

    scraped_at = datetime.now(timezone.utc)
    published = 0
    for raw in raw_tweets:
        try:
            event = _to_event(coin, raw, scraped_at)
        except Exception as exc:
            log.warning("[%s] could not build TweetEvent: %s", coin, exc)
            continue
        publish(TWEETS, key=coin, event=event)
        published += 1
    return published


async def run() -> None:
    """Long-running loop: concurrent scrape of all coins each cycle, indefinitely."""
    interval = settings.tweet_poll_interval_seconds
    log.info(
        "tweet producer running: %d coins concurrent, interval=%ss, limit=%d",
        len(COIN_NAMES),
        interval,
        settings.tweets_per_scrape,
    )

    while True:
        try:
            results = await asyncio.gather(
                *[_scrape_and_publish(c) for c in COIN_NAMES],
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            log.info("tweet producer cancelled")
            raise

        for coin, result in zip(COIN_NAMES, results):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, BaseException):
                log.warning("[%s] scrape cycle failed: %s", coin, result)
            else:
                log.info("[%s] published %d tweets", coin, result)

        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            log.info("tweet producer cancelled during sleep")
            raise
