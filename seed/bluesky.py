from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from producer.config import settings
from producer.kafka_client import publish
from shared.coins import COINS, CoinSpec
from shared.queries import KEYWORDS, search_query
from shared.schemas import TweetEvent
from shared.topics import TWEETS

log = logging.getLogger("velora.seed.bluesky")

_SEARCH_PATH = "/xrpc/app.bsky.feed.searchPosts"
_MAX_PAGES_PER_COIN = 25


def _parse_record_ts(raw: str) -> datetime:
    """ISO8601 → UTC datetime. ⊥ fallback (V9)."""
    cleaned = raw.replace("Z", "+00:00")
    dt = datetime.fromisoformat(cleaned)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _post_to_event(coin: str, post: dict[str, Any], scraped_at: datetime) -> TweetEvent | None:
    """Bluesky post envelope → TweetEvent. Returns None on parse failure."""
    record = post.get("record") or {}
    text = record.get("text") or ""
    if not text.strip():
        return None

    uri = post.get("uri") or ""
    rkey = uri.rsplit("/", 1)[-1] if uri else None
    if not rkey:
        return None

    author = post.get("author") or {}
    did = author.get("did") or ""
    handle = author.get("handle") or did

    try:
        ts = _parse_record_ts(record.get("createdAt") or post.get("indexedAt") or "")
    except (ValueError, TypeError):
        return None

    return TweetEvent(
        coin=coin,
        tweet_id=rkey,
        ts=ts,
        scraped_at=scraped_at,
        text=text,
        source="bluesky",
        username=handle,
        url=f"https://bsky.app/profile/{did}/post/{rkey}" if did else None,
        likes=int(post.get("likeCount") or 0),
        retweets=int(post.get("repostCount") or 0),
        replies=int(post.get("replyCount") or 0),
    )


async def _seed_coin(client: httpx.AsyncClient, spec: CoinSpec) -> int:
    """One unauth searchPosts call for one coin. Returns count published.

    Bluesky public XRPC searchPosts (https://github.com/bluesky-social/atproto/issues/2940):
      - `cursor` and `since` BOTH require auth. Unauth = 403.
      - Single first page is the only reliable unauth call (~100 posts max).
      - Bluesky also temp-disables public search under load → 403 expected.

    Live data path (Jetstream WS firehose) covers ongoing feed.
    """
    keyword = search_query(spec.coin)
    params: dict[str, Any] = {
        "q": keyword,
        "limit": min(settings.bluesky_seed_posts_per_query, 100),
        "sort": "latest",
    }

    try:
        resp = await client.get(_SEARCH_PATH, params=params, timeout=20.0)
    except httpx.HTTPError as exc:
        log.warning("[%s] bluesky seed network error: %s", spec.coin, exc)
        return 0

    if resp.status_code == 403:
        log.info("[%s] bluesky public search returns 403 (rate-limited or auth-only); "
                 "skipping seed — live Jetstream firehose will populate", spec.coin)
        return 0
    if resp.status_code != 200:
        log.warning("[%s] bluesky seed HTTP %d", spec.coin, resp.status_code)
        return 0

    data = resp.json()
    posts = data.get("posts") or []
    if not posts:
        log.info("[%s] bluesky seed: 0 posts returned", spec.coin)
        return 0

    scraped_at = datetime.now(timezone.utc)
    published = 0
    for post in posts:
        event = _post_to_event(spec.coin, post, scraped_at)
        if event is None:
            continue
        try:
            publish(TWEETS, key=spec.coin, event=event)
            published += 1
        except Exception as exc:
            log.warning("[%s] publish failed: %s", spec.coin, exc)

    log.info("[%s] bluesky seed: %d posts published (single page; live firehose covers rest)",
             spec.coin, published)
    return published


async def backfill_bluesky() -> int:
    """One unauth searchPosts call per coin. Idempotent via Mongo unique idx.

    Bluesky public search is unreliable (often 403) — graceful degrade to 0.
    Live Jetstream WS firehose (producer/bluesky_producer.py) is the primary
    bluesky data path; this seed just primes the first ~100 posts per coin.
    """
    log.info("bluesky seed starting: %d coins, base=%s (unauth single-page)",
             len(COINS), settings.bluesky_public_api_url)

    async with httpx.AsyncClient(base_url=settings.bluesky_public_api_url) as client:
        counts = await asyncio.gather(
            *(_seed_coin(client, spec) for spec in COINS),
            return_exceptions=True,
        )

    total = 0
    for c, spec in zip(counts, COINS):
        if isinstance(c, int):
            total += c
        else:
            log.warning("[%s] bluesky seed exception: %r", spec.coin, c)

    log.info("bluesky seed complete: %d total posts published", total)
    return total
