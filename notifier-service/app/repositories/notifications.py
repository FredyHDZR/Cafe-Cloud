import logging
from datetime import UTC, datetime
from uuid import UUID

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError

from app.domain.notification import Notification, NotificationPage, NotificationStats
from app.domain.timestamps import utc_now
from app.infra.logging import log_context
from app.infra.mongo import Document

logger = logging.getLogger(__name__)

# Mongo asigna _id aunque to_document no lo escriba: se excluye en origen, no por omision.
PUBLIC_PROJECTION: Document = {"_id": 0}


def to_document(notification: Notification) -> Document:
    return {
        "event_id": str(notification.event_id),
        "order_id": str(notification.order_id),
        "customer_id": notification.customer_id,
        "status": notification.status,
        "message": notification.message,
        "trace_id": str(notification.trace_id),
        "created_at": notification.created_at,
    }


def as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def from_document(document: Document) -> Notification:
    created_at: datetime = document["created_at"]
    return Notification(
        event_id=UUID(document["event_id"]),
        order_id=UUID(document["order_id"]),
        customer_id=document["customer_id"],
        status=document["status"],
        message=document["message"],
        trace_id=UUID(document["trace_id"]),
        created_at=as_utc(created_at),
    )


class NotificationRepository:
    def __init__(self, collection: AsyncIOMotorCollection[Document]) -> None:
        self._collection = collection

    async def insert(self, notification: Notification) -> bool:
        try:
            await self._collection.insert_one(to_document(notification))
        except DuplicateKeyError:
            # El indice unico sobre event_id ES la deduplicacion del consumidor (ADR-004): el
            # evento ya estaba aplicado, asi que esto es exito, no fallo.
            logger.debug(
                "duplicate_key_on_insert",
                extra=log_context(event_id=str(notification.event_id)),
            )
            return False
        return True

    async def list_by_customer(
        self, customer_id: str, *, limit: int, offset: int
    ) -> NotificationPage:
        criteria: Document = {"customer_id": customer_id}
        cursor = (
            self._collection.find(criteria, projection=PUBLIC_PROJECTION)
            .sort("created_at", DESCENDING)
            .skip(offset)
            .limit(limit)
        )
        documents = await cursor.to_list(length=limit)
        total = await self._collection.count_documents(criteria)
        return NotificationPage(
            items=tuple(from_document(document) for document in documents),
            total=total,
            limit=limit,
            offset=offset,
        )

    async def stats(self) -> NotificationStats:
        stored = await self._collection.count_documents({})
        oldest = await self._collection.find_one(
            projection={"_id": 0, "created_at": 1}, sort=[("created_at", ASCENDING)]
        )
        if oldest is None:
            return NotificationStats(stored=stored, oldest_age_seconds=0.0)
        created_at = as_utc(oldest["created_at"])
        return NotificationStats(
            stored=stored,
            oldest_age_seconds=(utc_now() - created_at).total_seconds(),
        )
