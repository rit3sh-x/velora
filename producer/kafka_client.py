"""Kafka producer wrapper.

Builds a single idempotent `KafkaProducer` instance configured with a
JSON value serializer (`shared.schemas.serialize`) and utf-8 key
serializer. Exposes a module-level `publish(topic, key, event)` helper.

On ctor failure (broker unreachable), shares the failure window across
concurrent callers via a memoized `_last_fail_at` timestamp — prevents
N concurrent KafkaProducer ctor attempts each blocking ~2s.
"""
from __future__ import annotations

import producer

import logging
import time
from threading import Lock
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from pydantic import BaseModel

from producer.config import settings
from shared.schemas import serialize

log = logging.getLogger("velora.producer.kafka")

_producer: Optional[KafkaProducer] = None
_lock = Lock()
_last_fail_at: float = 0.0
_FAIL_BACKOFF_SECONDS = 5.0


def _value_serializer(model: BaseModel) -> bytes:
    return serialize(model)


def _key_serializer(key: str | None) -> bytes | None:
    if key is None:
        return None
    return key.encode("utf-8")


def get_producer() -> KafkaProducer:
    """Return a process-wide singleton KafkaProducer.

    Within `_FAIL_BACKOFF_SECONDS` of last ctor failure, raises immediately
    instead of attempting a fresh connect. Avoids N concurrent callers
    each spending ~2s on the same failed bootstrap.
    """
    global _producer, _last_fail_at
    if _producer is not None:
        return _producer

    with _lock:
        if _producer is not None:
            return _producer
        now = time.monotonic()
        if now - _last_fail_at < _FAIL_BACKOFF_SECONDS:
            raise NoBrokersAvailable(
                f"kafka producer fail backoff ({_FAIL_BACKOFF_SECONDS - (now - _last_fail_at):.1f}s remain)"
            )
        log.info("connecting kafka producer to %s", settings.kafka_broker)
        try:
            _producer = KafkaProducer(
                bootstrap_servers=settings.kafka_broker,
                value_serializer=_value_serializer,
                key_serializer=_key_serializer,
                acks="all",
                linger_ms=50,
                retries=5,
            )
        except Exception:
            _last_fail_at = time.monotonic()
            raise
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
