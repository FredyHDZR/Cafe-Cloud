from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_serializer

from app.domain.notification import Notification, NotificationPage
from app.domain.timestamps import to_rfc3339


class NotificationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    order_id: UUID
    customer_id: str
    status: str
    message: str
    trace_id: UUID
    created_at: datetime

    @field_serializer("created_at")
    def _serialize_created_at(self, value: datetime) -> str:
        return to_rfc3339(value)

    @classmethod
    def from_domain(cls, notification: Notification) -> "NotificationResponse":
        return cls(
            event_id=notification.event_id,
            order_id=notification.order_id,
            customer_id=notification.customer_id,
            status=notification.status,
            message=notification.message,
            trace_id=notification.trace_id,
            created_at=notification.created_at,
        )


class NotificationPageResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[NotificationResponse]
    total: int = Field(ge=0)
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)

    @classmethod
    def from_domain(cls, page: NotificationPage) -> "NotificationPageResponse":
        return cls(
            items=[NotificationResponse.from_domain(item) for item in page.items],
            total=page.total,
            limit=page.limit,
            offset=page.offset,
        )
