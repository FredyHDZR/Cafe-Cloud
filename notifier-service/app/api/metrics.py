import logging
from collections.abc import Coroutine
from typing import Any

from app.domain.health import describe
from app.infra.logging import log_context
from app.infra.metrics import (
    DLQ_STREAM_LENGTH,
    METRICS_COLLECTION_ERRORS,
    NOTIFICATIONS_OLDEST_AGE,
    NOTIFICATIONS_STORED,
    STREAM_PENDING_ENTRIES,
)
from app.infra.streams import RedisStreamConsumer
from app.repositories.notifications import NotificationRepository

logger = logging.getLogger(__name__)


class MetricsCollector:
    async def collect(self) -> None:
        raise NotImplementedError


class ApiMetricsCollector(MetricsCollector):
    def __init__(self, *, notifications: NotificationRepository) -> None:
        self._notifications = notifications

    async def collect(self) -> None:
        await guarded("mongo", self._collect_notifications())

    async def _collect_notifications(self) -> None:
        stats = await self._notifications.stats()
        NOTIFICATIONS_STORED.set(stats.stored)
        NOTIFICATIONS_OLDEST_AGE.set(stats.oldest_age_seconds)


class ConsumerMetricsCollector(MetricsCollector):
    def __init__(self, *, stream: RedisStreamConsumer) -> None:
        self._stream = stream

    async def collect(self) -> None:
        await guarded("redis", self._collect_streams())

    async def _collect_streams(self) -> None:
        STREAM_PENDING_ENTRIES.labels(stream=self._stream.stream, group=self._stream.group).set(
            await self._stream.pending_count()
        )
        DLQ_STREAM_LENGTH.labels(stream=self._stream.dlq_stream).set(
            await self._stream.dlq_length()
        )


async def guarded(source: str, work: Coroutine[Any, Any, None]) -> None:
    try:
        await work
    except Exception as error:
        METRICS_COLLECTION_ERRORS.labels(source=source).inc()
        logger.warning(
            "metrics_collection_failed",
            extra=log_context(source=source, error=describe(error)),
        )
