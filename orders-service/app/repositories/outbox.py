from uuid import UUID, uuid4

from sqlalchemy import insert

from app.domain.events import EventDraft
from app.infra.models import OutboxEvent as OutboxRow
from app.repositories.base import SQLAlchemyRepository


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
