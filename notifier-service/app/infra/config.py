import socket
from functools import lru_cache
from typing import Literal

from pydantic import Field, MongoDsn, RedisDsn, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.domain.dead_letter import dlq_stream_for

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    mongo_url: MongoDsn = Field(alias="MONGO_URL")
    mongo_database: str = Field(alias="MONGO_DB", min_length=1)
    mongo_collection: str = Field(default="notifications", alias="MONGO_COLLECTION", min_length=1)
    redis_url: RedisDsn = Field(alias="REDIS_URL")
    service_name: str = Field(alias="SERVICE_NAME", min_length=1)
    log_level: LogLevel = Field(alias="LOG_LEVEL")
    mongo_server_selection_timeout_ms: int = Field(
        default=5000, alias="MONGO_SERVER_SELECTION_TIMEOUT_MS", gt=0
    )

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalize_log_level(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value

    @property
    def mongo_dsn(self) -> str:
        return str(self.mongo_url)

    @property
    def redis_dsn(self) -> str:
        return str(self.redis_url)


class ConsumerSettings(Settings):
    stream: str = Field(default="orders.completed", alias="CONSUMER_STREAM", min_length=1)
    group: str = Field(default="notifier", alias="CONSUMER_GROUP", min_length=1)
    consumer_name: str = Field(default="", alias="CONSUMER_NAME")
    block_ms: int = Field(default=5000, alias="CONSUMER_BLOCK_MS", gt=0)
    batch_size: int = Field(default=10, alias="CONSUMER_BATCH_SIZE", gt=0)
    error_pause_seconds: float = Field(default=1.0, alias="CONSUMER_ERROR_PAUSE_SECONDS", gt=0)
    max_attempts: int = Field(default=3, alias="CONSUMER_MAX_ATTEMPTS", gt=0)
    backoff_base_seconds: float = Field(default=1.0, alias="CONSUMER_BACKOFF_BASE_SECONDS", gt=0)
    backoff_max_seconds: float = Field(default=30.0, alias="CONSUMER_BACKOFF_MAX_SECONDS", gt=0)
    backoff_jitter_min: float = Field(default=0.8, alias="CONSUMER_BACKOFF_JITTER_MIN", gt=0)
    backoff_jitter_max: float = Field(default=1.2, alias="CONSUMER_BACKOFF_JITTER_MAX", gt=0)
    dlq_stream: str = Field(default="", alias="CONSUMER_DLQ_STREAM")
    janitor_interval_seconds: float = Field(default=15.0, alias="JANITOR_INTERVAL_SECONDS", gt=0)
    janitor_min_idle_ms: int = Field(default=60_000, alias="JANITOR_MIN_IDLE_MS", gt=0)
    janitor_batch_size: int = Field(default=50, alias="JANITOR_BATCH_SIZE", gt=0)
    janitor_max_deliveries: int = Field(default=5, alias="JANITOR_MAX_DELIVERIES", gt=0)

    @model_validator(mode="after")
    def _check_jitter_range(self) -> "ConsumerSettings":
        if self.backoff_jitter_max < self.backoff_jitter_min:
            message = "CONSUMER_BACKOFF_JITTER_MAX no puede ser menor que el MIN"
            raise ValueError(message)
        return self

    @property
    def resolved_consumer_name(self) -> str:
        return self.consumer_name or f"notifier-{socket.gethostname()}"

    @property
    def resolved_dlq_stream(self) -> str:
        return self.dlq_stream or dlq_stream_for(self.stream)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


@lru_cache(maxsize=1)
def get_consumer_settings() -> ConsumerSettings:
    return ConsumerSettings()
