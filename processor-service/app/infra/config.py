import socket
from functools import lru_cache
from typing import Literal

from pydantic import Field, PostgresDsn, RedisDsn, field_validator, model_validator
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

    database_url: PostgresDsn = Field(alias="DATABASE_URL")
    redis_url: RedisDsn = Field(alias="REDIS_URL")
    service_name: str = Field(alias="SERVICE_NAME", min_length=1)
    log_level: LogLevel = Field(alias="LOG_LEVEL")
    health_probe_timeout_seconds: float = Field(
        default=2.0, alias="HEALTH_PROBE_TIMEOUT_SECONDS", gt=0
    )
    lock_timeout_ms: int = Field(default=3000, alias="DB_LOCK_TIMEOUT_MS", ge=0)
    idle_in_transaction_timeout_ms: int = Field(
        default=10_000, alias="DB_IDLE_IN_TRANSACTION_TIMEOUT_MS", ge=0
    )

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalize_log_level(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value

    @property
    def sqlalchemy_dsn(self) -> str:
        return str(self.database_url)

    @property
    def redis_dsn(self) -> str:
        return str(self.redis_url)


class ConsumerSettings(Settings):
    stream: str = Field(default="orders.created", alias="CONSUMER_STREAM", min_length=1)
    group: str = Field(default="processor", alias="CONSUMER_GROUP", min_length=1)
    consumer_name: str = Field(default="", alias="CONSUMER_NAME")
    block_ms: int = Field(default=5000, alias="CONSUMER_BLOCK_MS", gt=0)
    batch_size: int = Field(default=1, alias="CONSUMER_BATCH_SIZE", gt=0)
    error_pause_seconds: float = Field(default=1.0, alias="CONSUMER_ERROR_PAUSE_SECONDS", gt=0)
    prep_min_seconds: float = Field(default=2.0, alias="PREP_MIN_SECONDS", ge=0)
    prep_max_seconds: float = Field(default=5.0, alias="PREP_MAX_SECONDS", ge=0)
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
    http_host: str = Field(default="0.0.0.0", alias="HTTP_HOST", min_length=1)
    http_port: int = Field(default=8000, alias="HTTP_PORT", gt=0, le=65535)

    @model_validator(mode="after")
    def _check_prep_range(self) -> "ConsumerSettings":
        if self.prep_max_seconds < self.prep_min_seconds:
            message = "PREP_MAX_SECONDS no puede ser menor que PREP_MIN_SECONDS"
            raise ValueError(message)
        return self

    @model_validator(mode="after")
    def _check_jitter_range(self) -> "ConsumerSettings":
        if self.backoff_jitter_max < self.backoff_jitter_min:
            message = "CONSUMER_BACKOFF_JITTER_MAX no puede ser menor que el MIN"
            raise ValueError(message)
        return self

    @property
    def resolved_consumer_name(self) -> str:
        return self.consumer_name or f"processor-{socket.gethostname()}"

    @property
    def resolved_dlq_stream(self) -> str:
        return self.dlq_stream or dlq_stream_for(self.stream)


class PublisherSettings(Settings):
    poll_interval_ms: int = Field(default=200, alias="OUTBOX_POLL_INTERVAL_MS", gt=0)
    batch_size: int = Field(default=100, alias="OUTBOX_BATCH_SIZE", gt=0)
    max_attempts: int = Field(default=10, alias="OUTBOX_MAX_ATTEMPTS", gt=0)
    backoff_base_seconds: float = Field(default=1.0, alias="OUTBOX_BACKOFF_BASE_SECONDS", gt=0)
    backoff_max_seconds: float = Field(default=300.0, alias="OUTBOX_BACKOFF_MAX_SECONDS", gt=0)
    stream_maxlen: int = Field(default=10_000, alias="OUTBOX_STREAM_MAXLEN", gt=0)
    purge_every_cycles: int = Field(default=1500, alias="OUTBOX_PURGE_EVERY_CYCLES", gt=0)
    purge_batch_size: int = Field(default=1000, alias="OUTBOX_PURGE_BATCH_SIZE", gt=0)
    outbox_retention_hours: int = Field(default=24, alias="OUTBOX_RETENTION_HOURS", gt=0)

    @property
    def poll_interval_seconds(self) -> float:
        return self.poll_interval_ms / 1000


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


@lru_cache(maxsize=1)
def get_consumer_settings() -> ConsumerSettings:
    return ConsumerSettings()


@lru_cache(maxsize=1)
def get_publisher_settings() -> PublisherSettings:
    return PublisherSettings()
