from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from app.domain.envelope import OrderCompletedEnvelope
from app.domain.timestamps import utc_now

# Unica cadena del repositorio que lee un cliente final, y por eso lleva tildes.
MESSAGE_TEMPLATE = "Tu pedido {order_id} está listo"

DEFAULT_PAGE_LIMIT = 50
MAX_PAGE_LIMIT = 200


@dataclass(frozen=True, slots=True)
class Notification:
    event_id: UUID
    order_id: UUID
    customer_id: str
    status: str
    message: str
    trace_id: UUID
    created_at: datetime


def notification_message(order_id: UUID) -> str:
    return MESSAGE_TEMPLATE.format(order_id=order_id)


def notification_from_event(event: OrderCompletedEnvelope) -> Notification:
    order = event.payload
    return Notification(
        event_id=event.event_id,
        order_id=order.order_id,
        customer_id=order.customer_id,
        status=order.status,
        message=notification_message(order.order_id),
        trace_id=event.trace_id,
        # created_at es el reloj de retencion de la notificacion, no el del pedido: un evento
        # recuperado tarde tiene que ser visible 24 h desde que se notifica (ADR-005).
        created_at=utc_now(),
    )


@dataclass(frozen=True, slots=True)
class NotificationPage:
    items: tuple[Notification, ...]
    total: int
    limit: int
    offset: int
