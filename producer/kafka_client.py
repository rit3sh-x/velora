"""Kafka producer wrapper.

Builds a single idempotent `KafkaProducer` instance configured with a
JSON value serializer (`shared.schemas.serialize`) and utf-8 key
serializer. Exposes a module-level `publish(topic, key, event)` helper.
"""
from __future__ import annotations

import producer

import logging
from threading import Lock
from typing import Optional

from kafka import KafkaProducer
from pydantic import BaseModel

from producer.config import settings
from shared.schemas import serialize

log = logging.getLogger("velora.producer.kafka")

_producer: Optional[KafkaProducer] = None
_lock = Lock()


def _value_serializer(model: BaseModel) -> bytes:
    return serialize(model)


def _key_serializer(key: str | None) -> bytes | None:
    if key is None:
        return None
    return key.encode("utf-8")


def get_producer() -> KafkaProducer:
    """Return a process-wide singleton KafkaProducer."""
    global _producer
    if _producer is not None:
        return _producer

    with _lock:
        if _producer is None:
            log.info("connecting kafka producer to %s", settings.kafka_broker)
            _producer = KafkaProducer(
                bootstrap_servers=settings.kafka_broker,
                value_serializer=_value_serializer,
                key_serializer=_key_serializer,
                acks="all",
                linger_ms=50,
                retries=5,
            )
    return _producer


def publish(topic: str, key: str, event: BaseModel) -> None:
    """Publish a pydantic event to a topic with a string key."""
    producer_ = get_producer()
    producer_.send(topic, key=key, value=event)


def close_producer() -> None:
    """Flush and close the singleton producer (for clean shutdown)."""
    global _producer
    with _lock:
        if _producer is None:
            return
        try:
            _producer.flush(timeout=10)
        except Exception as exc:
            log.warning("kafka flush error during shutdown: %s", exc)
        try:
            _producer.close(timeout=10)
        except Exception as exc:
            log.warning("kafka close error during shutdown: %s", exc)
        _producer = None
