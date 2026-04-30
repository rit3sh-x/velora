from __future__ import annotations
from shared.schemas import SentimentEvent
from consumer.config import settings

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


log = logging.getLogger("velora.consumer.sentiment")

GROUP_ID = "velora.silver.sentiment"
TOPIC = "velora.sentiment"
POLL_TIMEOUT_MS = 1000
RECONNECT_BACKOFF_SECONDS = 5.0

UPSERT_SQL = """
INSERT INTO sentiment_scored
    (tweet_id, ts, source, coin,
     compound_vader, label_vader, scored_at_vader,
     compound_bert,  label_bert,  scored_at_bert)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (tweet_id, ts, source) DO UPDATE SET
    compound_vader  = COALESCE(EXCLUDED.compound_vader,  sentiment_scored.compound_vader),
    label_vader     = COALESCE(EXCLUDED.label_vader,     sentiment_scored.label_vader),
    scored_at_vader = COALESCE(EXCLUDED.scored_at_vader, sentiment_scored.scored_at_vader),
    compound_bert   = COALESCE(EXCLUDED.compound_bert,   sentiment_scored.compound_bert),
    label_bert      = COALESCE(EXCLUDED.label_bert,      sentiment_scored.label_bert),
    scored_at_bert  = COALESCE(EXCLUDED.scored_at_bert,  sentiment_scored.scored_at_bert)
"""


def _build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=settings.kafka_broker,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=-1,
    )


async def _handle_record(conn: asyncpg.Connection, raw_value: object) -> None:
    try:
        event = SentimentEvent.model_validate(raw_value)
    except ValidationError as exc:
        log.warning("sentiment parse error: %s | payload=%r", exc, raw_value)
        return

    await conn.execute(
        UPSERT_SQL,
        event.tweet_id,
        event.ts,
        event.source,
        event.coin,
        event.compound_vader,
        event.label_vader,
        event.scored_at_vader,
        event.compound_bert,
        event.label_bert,
        event.scored_at_bert,
    )

    if event.compound_vader is not None:
        log.debug("vader %s/%s coin=%s c=%.3f", event.tweet_id, event.source,
                  event.coin, event.compound_vader)
    if event.compound_bert is not None:
        log.debug("bert %s/%s coin=%s c=%.3f", event.tweet_id, event.source,
                  event.coin, event.compound_bert)


async def run(pool: asyncpg.Pool) -> None:
    log.info("starting sentiment consumer (broker=%s topic=%s group=%s)",
             settings.kafka_broker, TOPIC, GROUP_ID)

    consumer: KafkaConsumer | None = None
    try:
        while True:
            if consumer is None:
                try:
                    consumer = await asyncio.to_thread(_build_consumer)
                    log.info("sentiment consumer connected")
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
        log.info("sentiment consumer cancelled; shutting down")
        raise
    finally:
        if consumer is not None:
            try:
                await asyncio.to_thread(consumer.close)
            except Exception as exc:
                log.warning("error closing kafka consumer: %s", exc)
        log.info("sentiment consumer stopped")
