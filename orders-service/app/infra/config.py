from functools import lru_cache
from typing import Literal

from pydantic import Field, PostgresDsn, RedisDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

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
    idempotency_grace_hours: int = Field(default=1, alias="IDEMPOTENCY_PURGE_GRACE_HOURS", ge=0)

    @property
    def poll_interval_seconds(self) -> float:
        return self.poll_interval_ms / 1000


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


@lru_cache(maxsize=1)
def get_publisher_settings() -> PublisherSettings:
    return PublisherSettings()
