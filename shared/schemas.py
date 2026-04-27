from datetime import datetime
from pydantic import BaseModel, Field


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
    """One scraped tweet pushed to velora.tweets."""
    coin: str
    tweet_id: str
    ts: datetime
    scraped_at: datetime
    text: str
    username: str | None = None
    likes: int = 0
    retweets: int = 0
    replies: int = 0
    url: str | None = None


def serialize(model: BaseModel) -> bytes:
    """Kafka producer value serializer. Datetimes → iso8601 Z."""
    return model.model_dump_json().encode("utf-8")
