from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pydantic import ValidationError
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from consumer.config import settings
from consumer.db_mongo import get_tweets_collection
from shared.schemas import TweetEvent
from shared.topics import TWEETS

log = logging.getLogger("velora.consumer.tweets")

GROUP_ID = "velora.bronze.tweets"
POLL_TIMEOUT_MS = 1000
RECONNECT_BACKOFF_SECONDS = 5.0


def _build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TWEETS,
        bootstrap_servers=settings.kafka_broker,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=-1,
    )


def _event_to_doc(event: TweetEvent) -> dict[str, object]:
    """TweetEvent → bronze Mongo doc. V71 shape."""
    return {
        "tweet_id":   event.tweet_id,
        "coin":       event.coin,
        "ts":         event.ts,
        "scraped_at": event.scraped_at,
        "text":       event.text,
        "source":     event.source,
        "username":   event.username,
        "url":        event.url,
        "likes":      event.likes,
        "retweets":   event.retweets,
        "replies":    event.replies,
    }


async def _handle_record(raw_value: object) -> None:
    """Parse one Kafka record value and upsert into Mongo bronze."""
    try:
        event = TweetEvent.model_validate(raw_value)
    except ValidationError as exc:
        log.warning("tweet parse error: %s | payload=%r", exc, raw_value)
        return

    coll = get_tweets_collection()
    doc = _event_to_doc(event)

    try:
        result = await coll.update_one(
            {"tweet_id": event.tweet_id, "source": event.source},
            {"$setOnInsert": doc},
            upsert=True,
        )
    except PyMongoError as exc:
        log.warning("mongo upsert error: %s | tweet_id=%s source=%s",
                    exc, event.tweet_id, event.source)
        return

    if result.upserted_id is not None:
        log.debug("inserted tweet_id=%s source=%s coin=%s ts=%s",
                  event.tweet_id, event.source, event.coin, event.ts)
    else:
        log.debug("dup tweet_id=%s source=%s", event.tweet_id, event.source)


async def run() -> None:
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

            for _tp, records in batch.items():
                for record in records:
                    await _handle_record(record.value)

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
