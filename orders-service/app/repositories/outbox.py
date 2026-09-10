from collections.abc import Sequence
from uuid import UUID, uuid4

from sqlalchemy import delete, func, insert, select, update

from app.domain.events import EventDraft, OutboxStats, PendingEvent
from app.infra.models import OutboxEvent as OutboxRow
from app.repositories.base import SQLAlchemyRepository
from app.repositories.intervals import interval

MAX_ERROR_CHARS = 500


class OutboxRepository(SQLAlchemyRepository):
    async def append(self, event: EventDraft) -> UUID:
        # El event_id nace aqui, no al publicar: es la clave de deduplicacion (ADR-002).
        event_id = uuid4()
        await self._session.execute(
            insert(OutboxRow).values(
                event_id=event_id,
                event_type=event.event_type,
                event_version=event.event_version,
                aggregate_id=event.aggregate_id,
                trace_id=event.trace_id,
                occurred_at=event.occurred_at,
                payload=event.payload,
            )
        )
        return event_id

    async def stats(self) -> OutboxStats:
        pending = OutboxRow.published_at.is_(None) & OutboxRow.failed_at.is_(None)
        row = (
            await self._session.execute(
                select(
                    func.count().filter(pending).label("pending"),
                    func.count().filter(OutboxRow.published_at.is_not(None)).label("published"),
                    func.count().filter(OutboxRow.failed_at.is_not(None)).label("failed"),
                    func.extract(
                        "epoch", func.now() - func.min(OutboxRow.occurred_at).filter(pending)
                    ).label("oldest_pending_age_seconds"),
                )
            )
        ).one()
        return OutboxStats(
            pending=row.pending,
            published=row.published,
            failed=row.failed,
            oldest_pending_age_seconds=float(row.oldest_pending_age_seconds or 0.0),
        )

    async def claim_pending(self, limit: int) -> list[PendingEvent]:
        rows = (
            await self._session.execute(
                select(
                    OutboxRow.id,
                    OutboxRow.event_id,
                    OutboxRow.event_type,
                    OutboxRow.event_version,
                    OutboxRow.trace_id,
                    OutboxRow.occurred_at,
                    OutboxRow.payload,
                    OutboxRow.attempts,
                )
                .where(
                    OutboxRow.published_at.is_(None),
                    OutboxRow.failed_at.is_(None),
                    OutboxRow.next_attempt_at <= func.now(),
                )
                .order_by(OutboxRow.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        ).all()
        return [
            PendingEvent(
                row_id=row.id,
                event_id=row.event_id,
                event_type=row.event_type,
                event_version=row.event_version,
                trace_id=row.trace_id,
                occurred_at=row.occurred_at,
                payload=row.payload,
                attempts=row.attempts,
            )
            for row in rows
        ]

    async def mark_published(self, row_ids: Sequence[int]) -> None:
        await self._session.execute(
            update(OutboxRow)
            .where(OutboxRow.id.in_(row_ids))
            .values(published_at=func.now(), last_error=None)
        )

    async def reschedule(self, row_id: int, *, delay_seconds: float, error: str) -> None:
        await self._session.execute(
            update(OutboxRow)
            .where(OutboxRow.id == row_id)
            .values(
                attempts=OutboxRow.attempts + 1,
                next_attempt_at=func.now() + interval(seconds=delay_seconds),
                last_error=error[:MAX_ERROR_CHARS],
            )
        )

    async def mark_failed(self, row_id: int, *, error: str) -> None:
        await self._session.execute(
            update(OutboxRow)
            .where(OutboxRow.id == row_id)
            .values(
                attempts=OutboxRow.attempts + 1,
                failed_at=func.now(),
                last_error=error[:MAX_ERROR_CHARS],
            )
        )

    async def delete_published_before(self, *, hours: int, limit: int) -> int:
        victims = (
            select(OutboxRow.id)
            .where(OutboxRow.published_at < func.now() - interval(hours=hours))
            .order_by(OutboxRow.id)
            .limit(limit)
        )
        deleted = await self._session.execute(
            delete(OutboxRow).where(OutboxRow.id.in_(victims)).returning(OutboxRow.id)
        )
        return len(deleted.all())
