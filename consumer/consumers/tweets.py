"""Kafka -> TimescaleDB consumer for velora.tweets.

Long-running asyncio task. Polls KafkaConsumer (sync, kafka-python) inside
asyncio.to_thread(), parses each record into a TweetEvent, and inserts into
the raw_tweets hypertable with ON CONFLICT DO NOTHING.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path

import asyncpg
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from consumer.config import settings
from shared.schemas import TweetEvent
from shared.topics import TWEETS

log = logging.getLogger("velora.consumer.tweets")

GROUP_ID = "velora-consumer-tweets"
POLL_TIMEOUT_MS = 1000
RECONNECT_BACKOFF_SECONDS = 5.0

INSERT_SQL = """
INSERT INTO raw_tweets (tweet_id, coin, ts, scraped_at, text, username, url, likes, retweets, replies)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (tweet_id, ts) DO NOTHING
"""


def _build_consumer() -> KafkaConsumer:
    """Construct a KafkaConsumer subscribed to the tweets topic."""
    return KafkaConsumer(
        TWEETS,
        bootstrap_servers=settings.kafka_broker,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=-1,
    )


async def _handle_record(conn: asyncpg.Connection, raw_value: object) -> None:
    """Parse one Kafka record value and insert into raw_tweets."""
    try:
        event = TweetEvent.model_validate(raw_value)
    except ValidationError as exc:
        log.warning("tweet parse error: %s | payload=%r", exc, raw_value)
        return

    status = await conn.execute(
        INSERT_SQL,
        event.tweet_id,
        event.coin,
        event.ts,
        event.scraped_at,
        event.text,
        event.username,
        event.url,
        event.likes,
        event.retweets,
        event.replies,
    )
    if status.endswith(" 0"):
        log.debug("skipped duplicate tweet_id=%s coin=%s ts=%s",
                  event.tweet_id, event.coin, event.ts)
    else:
        log.debug("inserted tweet_id=%s coin=%s ts=%s",
                  event.tweet_id, event.coin, event.ts)


async def run(pool: asyncpg.Pool) -> None:
    """Long-running consumer loop. Designed for asyncio.gather()."""
    log.info("starting tweets consumer (broker=%s topic=%s group=%s)",
             settings.kafka_broker, TWEETS, GROUP_ID)

    consumer: KafkaConsumer | None = None
    try:
        while True:
            if consumer is None:
                try:
                    consumer = await asyncio.to_thread(_build_consumer)
                    log.info("tweets consumer connected")
                except KafkaError as exc:
                    log.warning("kafka connect failed: %s; retrying in %.1fs",
                                exc, RECONNECT_BACKOFF_SECONDS)
                    await asyncio.sleep(RECONNECT_BACKOFF_SECONDS)
                    continue

            try:
                batch = await asyncio.to_thread(consumer.poll, POLL_TIMEOUT_MS)
            except KafkaError as exc:
                log.warning("kafka poll failed: %s; reconnecting", exc)
                await asyncio.to_thread(consumer.close)
                consumer = None
                await asyncio.sleep(RECONNECT_BACKOFF_SECONDS)
                continue

            if not batch:
                await asyncio.sleep(0)
                continue

            async with pool.acquire() as conn:
                for _tp, records in batch.items():
                    for record in records:
                        await _handle_record(conn, record.value)

    except asyncio.CancelledError:
        log.info("tweets consumer cancelled; shutting down")
        raise
    finally:
        if consumer is not None:
            try:
                await asyncio.to_thread(consumer.close)
            except Exception as exc:
                log.warning("error closing kafka consumer: %s", exc)
        log.info("tweets consumer stopped")
