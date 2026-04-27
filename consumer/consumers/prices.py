"""Kafka -> TimescaleDB consumer for velora.prices.

Long-running asyncio task. Polls KafkaConsumer (sync, kafka-python) inside
asyncio.to_thread(), parses each record into a PriceEvent, and upserts into
the prices_1m hypertable.
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
from shared.schemas import PriceEvent
from shared.topics import PRICES

log = logging.getLogger("velora.consumer.prices")

GROUP_ID = "velora-consumer-prices"
POLL_TIMEOUT_MS = 1000
RECONNECT_BACKOFF_SECONDS = 5.0

UPSERT_SQL = """
INSERT INTO prices_1m (coin, ts, open, high, low, close, volume)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (coin, ts) DO UPDATE SET
  open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
  close=EXCLUDED.close, volume=EXCLUDED.volume
"""


def _build_consumer() -> KafkaConsumer:
    """Construct a KafkaConsumer subscribed to the prices topic."""
    return KafkaConsumer(
        PRICES,
        bootstrap_servers=settings.kafka_broker,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=-1,
    )


async def _handle_record(conn: asyncpg.Connection, raw_value: object) -> None:
    """Parse one Kafka record value and upsert into prices_1m."""
    try:
        event = PriceEvent.model_validate(raw_value)
    except ValidationError as exc:
        log.warning("price parse error: %s | payload=%r", exc, raw_value)
        return

    await conn.execute(
        UPSERT_SQL,
        event.coin,
        event.ts,
        event.open,
        event.high,
        event.low,
        event.close,
        event.volume,
    )
    log.debug("upserted price coin=%s ts=%s close=%s", event.coin, event.ts, event.close)


async def run(pool: asyncpg.Pool) -> None:
    """Long-running consumer loop. Designed for asyncio.gather()."""
    log.info("starting prices consumer (broker=%s topic=%s group=%s)",
             settings.kafka_broker, PRICES, GROUP_ID)

    consumer: KafkaConsumer | None = None
    try:
        while True:
            if consumer is None:
                try:
                    consumer = await asyncio.to_thread(_build_consumer)
                    log.info("prices consumer connected")
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
        log.info("prices consumer cancelled; shutting down")
        raise
    finally:
        if consumer is not None:
            try:
                await asyncio.to_thread(consumer.close)
            except Exception as exc:
                log.warning("error closing kafka consumer: %s", exc)
        log.info("prices consumer stopped")
