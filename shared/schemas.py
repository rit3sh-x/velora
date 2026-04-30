"""Shared event schemas for Kafka payloads + bronze Mongo docs.

§V cites: V7 (JSON serialize), V23 (source ∈ {twitter, bluesky}),
V25 (sentiment event), V36 (bronze doc shape).
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

Source = Literal["twitter", "bluesky"]


class PriceEvent(BaseModel):
    """Closed 1m kline pushed to velora.prices."""
    coin: str
    symbol: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class TweetEvent(BaseModel):
    """One scraped/streamed tweet pushed to velora.tweets.

    `source` ∈ {twitter, bluesky} per V23. Stamped @ producer.
    Bronze Mongo doc = this shape verbatim (V36, V71).
    """
    coin: str
    tweet_id: str
    ts: datetime
    scraped_at: datetime
    text: str
    source: Source = "twitter"
    username: str | None = None
    likes: int = 0
    retweets: int = 0
    replies: int = 0
    url: str | None = None


class SentimentEvent(BaseModel):
    """Sentiment scoring event pushed to velora.sentiment by Spark.

    Per V25 + V46: Spark Structured Streaming output. Two writeStream queries
    (VADER 30s, BERT 5min) emit partial events; consumer UPSERTs by tweet_id+ts+source.
    `cleaned_text` optional (debug, ⊥ persisted to silver per V46).
    """
    tweet_id: str
    coin: str
    ts: datetime
    source: Source

    compound_vader: float | None = None
    label_vader: str | None = None
    scored_at_vader: datetime | None = None

    compound_bert: float | None = None
    label_bert: str | None = None
    scored_at_bert: datetime | None = None

    cleaned_text: str | None = None


def serialize(model: BaseModel) -> bytes:
    """Kafka producer value serializer. Datetimes → iso8601 Z."""
    return model.model_dump_json().encode("utf-8")
