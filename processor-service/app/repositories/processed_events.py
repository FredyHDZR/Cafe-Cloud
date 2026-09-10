from uuid import UUID

from sqlalchemy.dialects.postgresql import insert

from app.infra.models import ProcessedEvent
from app.repositories.base import SQLAlchemyRepository


class ProcessedEventRepository(SQLAlchemyRepository):
    async def reserve(self, *, event_id: UUID, event_type: str, consumer_group: str) -> bool:
        reserved = await self._session.execute(
            insert(ProcessedEvent)
            .values(event_id=event_id, event_type=event_type, consumer_group=consumer_group)
            .on_conflict_do_nothing(index_elements=[ProcessedEvent.event_id])
            .returning(ProcessedEvent.event_id)
        )
        return reserved.scalar_one_or_none() is not None
