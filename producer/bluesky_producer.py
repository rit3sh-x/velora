from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from producer.config import settings
from producer.kafka_client import publish
from shared.queries import match_coins
from shared.schemas import TweetEvent
from shared.topics import TWEETS

log = logging.getLogger("velora.producer.bluesky")


def _parse_record_ts(raw: str | None) -> datetime:
    """Bluesky record.created_at = ISO8601 (e.g. '2026-04-30T12:34:56.789Z').

    Drop tweets w/ unparseable ts per V9.
    """
    if not raw:
        raise ValueError("missing record.created_at")
    cleaned = raw.replace("Z", "+00:00")
    dt = datetime.fromisoformat(cleaned)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_event(
    coin: str,
    text: str,
    rkey: str,
    did: str,
    record_ts: datetime,
    scraped_at: datetime,
) -> TweetEvent:
    """Build TweetEvent from Jetstream commit.

    `tweet_id` = AT-uri rkey (post-specific TID). Stable per V8.
    `url` = `https://bsky.app/profile/{did}/post/{rkey}`.
    """
    return TweetEvent(
        coin=coin,
        tweet_id=rkey,
        ts=record_ts,
        scraped_at=scraped_at,
        text=text,
        source="bluesky",
        username=did,
        url=f"https://bsky.app/profile/{did}/post/{rkey}",
        likes=0,
        retweets=0,
        replies=0,
    )


def _extract_post(msg: dict[str, Any]) -> tuple[str, str, str, datetime] | None:
    """Pull (text, rkey, did, record_ts) from Jetstream commit envelope.

    Returns None for non-post events or malformed records.
    """
    if msg.get("kind") != "commit":
        return None
    commit = msg.get("commit") or {}
    if commit.get("operation") != "create":
        return None
    record = commit.get("record") or {}
    if record.get("$type") != "app.bsky.feed.post":
        return None
    text = record.get("text") or ""
    if not text.strip():
        return None
    rkey = commit.get("rkey")
    did = msg.get("did")
    if not rkey or not did:
        return None
    try:
        record_ts = _parse_record_ts(record.get("createdAt"))
    except ValueError as exc:
        log.debug("drop bluesky post: %s", exc)
        return None
    return text, rkey, did, record_ts


async def _consume_once() -> None:
    """One WS connection lifetime. Returns on graceful close; raises on error."""
    url = settings.bluesky_jetstream_url
    log.info("connecting bluesky jetstream %s", url)

    async with websockets.connect(url, max_size=2**20, ping_interval=20, ping_timeout=20) as ws:
        log.info("bluesky jetstream connected")
        async for raw_msg in ws:
            try:
                msg = json.loads(raw_msg)
            except json.JSONDecodeError:
                continue

            extracted = _extract_post(msg)
            if extracted is None:
                continue
            text, rkey, did, record_ts = extracted

            matched = match_coins(text)
            if not matched:
                continue

            scraped_at = datetime.now(timezone.utc)
            for coin in matched:
                try:
                    event = _to_event(coin, text, rkey, did, record_ts, scraped_at)
                    publish(TWEETS, key=coin, event=event)
                except Exception as exc:
                    log.warning("[%s] could not publish bluesky post: %s", coin, exc)


async def run() -> None:
    """Long-running loop: connect, consume, reconnect on drop. Cancel-safe.

    Exponential backoff per V56. Cap at `bluesky_reconnect_backoff_max`.
    """
    backoff = settings.bluesky_reconnect_backoff_initial
    cap = settings.bluesky_reconnect_backoff_max

    log.info(
        "bluesky producer starting: jetstream=%s coins=%d",
        settings.bluesky_jetstream_url,
        len(match_coins("")),
    )

    while True:
        try:
            await _consume_once()
            backoff = settings.bluesky_reconnect_backoff_initial
            log.info("bluesky jetstream closed cleanly; reconnecting")
        except asyncio.CancelledError:
            log.info("bluesky producer cancelled")
            raise
        except (ConnectionClosed, WebSocketException, OSError) as exc:
            log.warning("bluesky jetstream connection lost: %s; backoff=%.1fs", exc, backoff)
        except Exception:
            log.exception("bluesky jetstream unexpected error; backoff=%.1fs", backoff)

        try:
            await asyncio.sleep(backoff)
        except asyncio.CancelledError:
            log.info("bluesky producer cancelled during backoff")
            raise

        backoff = min(backoff * 2.0, cap)
