import logging
from collections.abc import Coroutine
from typing import Any

from app.domain.health import describe
from app.infra.database import Database
from app.infra.logging import log_context
from app.infra.metrics import (
    DLQ_STREAM_LENGTH,
    METRICS_COLLECTION_ERRORS,
    OUTBOX_OLDEST_PENDING_AGE,
    OUTBOX_ROWS,
    STREAM_PENDING_ENTRIES,
)
from app.infra.streams import RedisStreamConsumer
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)


class MetricsCollector:
    def __init__(self, *, database: Database, stream: RedisStreamConsumer) -> None:
        self._database = database
        self._stream = stream

    async def collect(self) -> None:
        await guarded("outbox", self._collect_outbox())
        await guarded("redis", self._collect_streams())

    async def _collect_outbox(self) -> None:
        async with self._database.session() as session:
            stats = await OutboxRepository(session).stats()
        OUTBOX_ROWS.labels(state="pending").set(stats.pending)
        OUTBOX_ROWS.labels(state="published").set(stats.published)
        OUTBOX_ROWS.labels(state="failed").set(stats.failed)
        OUTBOX_OLDEST_PENDING_AGE.set(stats.oldest_pending_age_seconds)

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
