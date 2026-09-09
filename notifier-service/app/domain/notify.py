import logging
from dataclasses import dataclass
from typing import Protocol

from app.domain.envelope import OrderCompletedEnvelope
from app.domain.notification import Notification, notification_from_event
from app.infra.logging import log_context

logger = logging.getLogger(__name__)


class NotificationStore(Protocol):
    async def insert(self, notification: Notification) -> bool: ...


@dataclass(frozen=True, slots=True)
class NotifyOutcome:
    notification: Notification
    duplicate: bool


class NotifyCustomerService:
    def __init__(self, *, notifications: NotificationStore) -> None:
        self._notifications = notifications

    async def run(self, event: OrderCompletedEnvelope) -> NotifyOutcome:
        notification = notification_from_event(event)
        inserted = await self._notifications.insert(notification)
        if not inserted:
            logger.info(
                "notification_duplicate_ignored",
                extra=log_context(
                    event_id=str(notification.event_id),
                    order_id=str(notification.order_id),
                    customer_id=notification.customer_id,
                    duplicate=True,
                ),
            )
            return NotifyOutcome(notification=notification, duplicate=True)

        logger.info(
            "notification_created",
            extra=log_context(
                event_id=str(notification.event_id),
                order_id=str(notification.order_id),
                customer_id=notification.customer_id,
                status=notification.status,
            ),
        )
        return NotifyOutcome(notification=notification, duplicate=False)
