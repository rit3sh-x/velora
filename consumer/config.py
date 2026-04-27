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

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "velora"
    postgres_user: str = "velora"
    postgres_password: str = "velora"

    vader_interval_seconds: int = 30
    bert_interval_seconds: int = 300
    aggregator_interval_seconds: int = 60

    bert_model: str = "distilbert-base-uncased-finetuned-sst-2-english"
    bert_batch_size: int = 64
    bert_enabled: bool = True

    api_host: str = "0.0.0.0"
    api_port: int = 8000

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()
