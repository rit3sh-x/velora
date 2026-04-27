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
    tweets_per_scrape: int = 30
    playwright_timeout_ms: int = 30000


settings = Settings()
