from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ROOT_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    kafka_broker: str = "localhost:9092"

    nitter_url: str = "https://nitter.tiekoetter.com"
    tweet_poll_interval_seconds: int = 120
    tweets_per_scrape: int = 100
    tweet_max_pages: int = 12
    playwright_timeout_ms: int = 45000

    tweet_backfill_enabled: bool = True
    tweet_backfill_days: int = 2
    tweet_backfill_min_faves: int = 0
    tweet_backfill_pages_per_day: int = 30
    tweet_backfill_max_per_day: int = 600

    bluesky_jetstream_url: str = (
        "wss://jetstream1.us-east.bsky.network/subscribe"
        "?wantedCollections=app.bsky.feed.post"
    )
    bluesky_public_api_url: str = "https://public.api.bsky.app"
    bluesky_reconnect_backoff_initial: float = 2.0
    bluesky_reconnect_backoff_max: float = 60.0
    bluesky_seed_posts_per_query: int = 100


settings = Settings()
