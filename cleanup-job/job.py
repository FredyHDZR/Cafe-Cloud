import argparse
import json
import logging
import signal
import sys
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import FrameType
from typing import Any, Literal
from uuid import uuid4

from apscheduler.schedulers.blocking import BlockingScheduler
from pydantic import Field, MongoDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
Document = dict[str, Any]

LOG_CONTEXT_FIELD = "context"
CLEANUP_JOB_ID = "notifications-cleanup"
RETENTION_FIELD = "created_at"

logger = logging.getLogger("cleanup_job")


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
    service_name: str = Field(alias="SERVICE_NAME", min_length=1)
    log_level: LogLevel = Field(alias="LOG_LEVEL")
    interval_seconds: float = Field(default=60.0, alias="CLEANUP_INTERVAL_SECONDS", gt=0)
    retention_hours: float = Field(default=24.0, alias="RETENTION_HOURS", gt=0)
    run_once: bool = Field(default=False, alias="CLEANUP_RUN_ONCE")
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
    def retention(self) -> timedelta:
        return timedelta(hours=self.retention_hours)


_trace_id: ContextVar[str | None] = ContextVar("trace_id", default=None)


def get_trace_id() -> str | None:
    return _trace_id.get()


def new_trace_id() -> str:
    return str(uuid4())


@contextmanager
def trace_id_scope(trace_id: str) -> Iterator[None]:
    token: Token[str | None] = _trace_id.set(trace_id)
    try:
        yield
    finally:
        _trace_id.reset(token)


class JsonFormatter(logging.Formatter):
    def __init__(self, service_name: str) -> None:
        super().__init__()
        self._service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "service": self._service_name,
            "trace_id": get_trace_id(),
            "message": record.getMessage(),
            "logger": record.name,
        }
        context = getattr(record, LOG_CONTEXT_FIELD, None)
        if isinstance(context, dict):
            entry[LOG_CONTEXT_FIELD] = context
        if record.exc_info is not None:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False, default=str)


def configure_logging(service_name: str, log_level: LogLevel) -> None:
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter(service_name))

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(log_level)


def log_context(**fields: Any) -> dict[str, dict[str, Any]]:
    return {LOG_CONTEXT_FIELD: fields}


@dataclass(frozen=True, slots=True)
class CleanupResult:
    deleted_count: int
    threshold: datetime
    duration_ms: float


class NotificationCleaner:
    def __init__(self, collection: Collection[Document], retention: timedelta) -> None:
        self._collection = collection
        self._retention = retention

    def run_once(self) -> CleanupResult:
        threshold = datetime.now(tz=UTC) - self._retention
        started_at = time.perf_counter()
        # ADR-005: el borrado es explicito y no un indice TTL, para que el job haga trabajo real.
        outcome = self._collection.delete_many({RETENTION_FIELD: {"$lt": threshold}})
        return CleanupResult(
            deleted_count=outcome.deleted_count,
            threshold=threshold,
            duration_ms=round((time.perf_counter() - started_at) * 1000, 3),
        )


def run_cleanup_pass(cleaner: NotificationCleaner) -> bool:
    with trace_id_scope(new_trace_id()):
        try:
            result = cleaner.run_once()
        except PyMongoError:
            logger.exception("cleanup_pass_failed")
            return False
        logger.info(
            "cleanup_pass_completed",
            extra=log_context(
                deleted_count=result.deleted_count,
                older_than=result.threshold.isoformat(timespec="milliseconds"),
                duration_ms=result.duration_ms,
            ),
        )
        return True


def build_scheduler(cleaner: NotificationCleaner, settings: Settings) -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone=UTC)
    scheduler.add_job(
        run_cleanup_pass,
        trigger="interval",
        seconds=settings.interval_seconds,
        args=(cleaner,),
        id=CLEANUP_JOB_ID,
        next_run_time=datetime.now(tz=UTC),
        max_instances=1,
        coalesce=True,
        # La pasada es idempotente y barata: llegar tarde nunca justifica saltarsela.
        misfire_grace_time=None,
    )
    return scheduler


def install_stop_handlers(scheduler: BlockingScheduler) -> None:
    def _shutdown(received: int, _frame: FrameType | None) -> None:
        logger.info("shutdown_requested", extra=log_context(signal=signal.Signals(received).name))
        if scheduler.running:
            scheduler.shutdown(wait=True)

    for received in (signal.SIGTERM, signal.SIGINT):
        signal.signal(received, _shutdown)


def build_collection(settings: Settings, client: MongoClient[Document]) -> Collection[Document]:
    return client[settings.mongo_database][settings.mongo_collection]


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="cleanup-job")
    parser.add_argument(
        "--once",
        action="store_true",
        help="ejecuta una sola pasada de limpieza y termina",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    settings = Settings()
    configure_logging(settings.service_name, settings.log_level)

    client: MongoClient[Document] = MongoClient(
        settings.mongo_dsn,
        tz_aware=True,
        serverSelectionTimeoutMS=settings.mongo_server_selection_timeout_ms,
    )
    single_shot = bool(args.once) or settings.run_once
    logger.info(
        "cleanup_job_started",
        extra=log_context(
            database=settings.mongo_database,
            collection=settings.mongo_collection,
            retention_hours=settings.retention_hours,
            interval_seconds=settings.interval_seconds,
            run_once=single_shot,
        ),
    )
    try:
        cleaner = NotificationCleaner(build_collection(settings, client), settings.retention)
        if single_shot:
            return 0 if run_cleanup_pass(cleaner) else 1
        scheduler = build_scheduler(cleaner, settings)
        install_stop_handlers(scheduler)
        scheduler.start()
        return 0
    finally:
        client.close()
        logger.info("cleanup_job_stopped")


if __name__ == "__main__":
    sys.exit(main())
