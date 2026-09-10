import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from app.domain.events import PendingEvent, envelope_fields
from app.infra.logging import log_context
from app.infra.tracing import trace_id_scope

logger = logging.getLogger(__name__)


class OutboxStore(Protocol):
    async def claim_pending(self, limit: int) -> list[PendingEvent]: ...

    async def mark_published(self, row_ids: Sequence[int]) -> None: ...

    async def reschedule(self, row_id: int, *, delay_seconds: float, error: str) -> None: ...

    async def mark_failed(self, row_id: int, *, error: str) -> None: ...

    async def delete_published_before(self, *, hours: int, limit: int) -> int: ...


class EventStream(Protocol):
    async def publish(self, stream: str, fields: Mapping[str, str]) -> str: ...


class UnitOfWork(Protocol):
    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PublisherPolicy:
    batch_size: int
    max_attempts: int
    backoff_base_seconds: float
    backoff_max_seconds: float


@dataclass(frozen=True, slots=True)
class PurgePolicy:
    outbox_retention_hours: int
    batch_size: int


@dataclass(frozen=True, slots=True)
class PublishOutcome:
    published: int = 0
    rescheduled: int = 0
    failed: int = 0


@dataclass(frozen=True, slots=True)
class PurgeOutcome:
    outbox_rows: int


def backoff_delay(attempts: int, *, base_seconds: float, max_seconds: float) -> float:
    return min(base_seconds * 2.0**attempts, max_seconds)


class PublishOutboxService:
    def __init__(
        self,
        *,
        outbox: OutboxStore,
        stream: EventStream,
        unit_of_work: UnitOfWork,
        policy: PublisherPolicy,
    ) -> None:
        self._outbox = outbox
        self._stream = stream
        self._unit_of_work = unit_of_work
        self._policy = policy

    async def run_once(self) -> PublishOutcome:
        pending = await self._outbox.claim_pending(self._policy.batch_size)
        if not pending:
            await self._unit_of_work.rollback()
            return PublishOutcome()

        published: list[int] = []
        rescheduled = 0
        failed = 0
        for event in pending:
            try:
                fields = envelope_fields(event)
            except (TypeError, ValueError) as error:
                await self._fail(event, error, reason="invalid_envelope")
                failed += 1
                continue
            try:
                entry_id = await self._stream.publish(event.event_type, fields)
            except Exception as error:
                if event.attempts + 1 >= self._policy.max_attempts:
                    await self._fail(event, error, reason="max_attempts_exhausted")
                    failed += 1
                else:
                    await self._reschedule(event, error)
                    rescheduled += 1
                # El orden global solo se conserva si la cola no se adelanta a la fila que falla.
                break
            published.append(event.row_id)
            self._log_published(event, entry_id)

        if published:
            await self._outbox.mark_published(published)
        await self._unit_of_work.commit()
        return PublishOutcome(published=len(published), rescheduled=rescheduled, failed=failed)

    async def _reschedule(self, event: PendingEvent, error: Exception) -> None:
        delay = backoff_delay(
            event.attempts,
            base_seconds=self._policy.backoff_base_seconds,
            max_seconds=self._policy.backoff_max_seconds,
        )
        await self._outbox.reschedule(event.row_id, delay_seconds=delay, error=_describe(error))
        with trace_id_scope(str(event.trace_id)):
            logger.warning(
                "outbox_publish_retry",
                extra=log_context(
                    event_id=str(event.event_id),
                    event_type=event.event_type,
                    outbox_id=event.row_id,
                    attempt=event.attempts + 1,
                    max_attempts=self._policy.max_attempts,
                    retry_in_seconds=delay,
                    error=_describe(error),
                ),
            )

    async def _fail(self, event: PendingEvent, error: Exception, *, reason: str) -> None:
        await self._outbox.mark_failed(event.row_id, error=_describe(error))
        with trace_id_scope(str(event.trace_id)):
            logger.error(
                "outbox_publish_failed",
                extra=log_context(
                    event_id=str(event.event_id),
                    event_type=event.event_type,
                    outbox_id=event.row_id,
                    attempts=event.attempts + 1,
                    reason=reason,
                    error=_describe(error),
                ),
            )

    def _log_published(self, event: PendingEvent, entry_id: str) -> None:
        with trace_id_scope(str(event.trace_id)):
            logger.info(
                "outbox_event_published",
                extra=log_context(
                    event_id=str(event.event_id),
                    event_type=event.event_type,
                    outbox_id=event.row_id,
                    stream_entry_id=entry_id,
                    attempts=event.attempts,
                ),
            )


class PurgeOutboxService:
    def __init__(
        self,
        *,
        outbox: OutboxStore,
        unit_of_work: UnitOfWork,
        policy: PurgePolicy,
    ) -> None:
        self._outbox = outbox
        self._unit_of_work = unit_of_work
        self._policy = policy

    async def run(self) -> PurgeOutcome:
        outbox_rows = await self._outbox.delete_published_before(
            hours=self._policy.outbox_retention_hours, limit=self._policy.batch_size
        )
        await self._unit_of_work.commit()
        outcome = PurgeOutcome(outbox_rows=outbox_rows)
        _log_purge(outcome)
        return outcome


def _log_purge(outcome: PurgeOutcome) -> None:
    context = log_context(outbox_rows=outcome.outbox_rows)
    if outcome.outbox_rows:
        logger.info("outbox_purge_done", extra=context)
    else:
        logger.debug("outbox_purge_done", extra=context)


def _describe(error: Exception) -> str:
    return f"{type(error).__name__}: {error}"
