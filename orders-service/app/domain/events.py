import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from app.domain.order import Order
from app.domain.timestamps import to_rfc3339

ORDER_CREATED_TYPE = "orders.created"
ORDER_CREATED_VERSION = 1


@dataclass(frozen=True, slots=True)
class EventDraft:
    event_type: str
    event_version: int
    aggregate_id: UUID
    trace_id: UUID
    occurred_at: datetime
    payload: dict[str, Any]


def order_created_payload(order: Order) -> dict[str, Any]:
    return {
        "order_id": str(order.id),
        "customer_id": order.customer_id,
        "status": order.status.value,
        "items": [{"name": item.name, "qty": item.qty} for item in order.items],
        "created_at": to_rfc3339(order.created_at),
    }


def order_created_event(order: Order, trace_id: UUID) -> EventDraft:
    return EventDraft(
        event_type=ORDER_CREATED_TYPE,
        event_version=ORDER_CREATED_VERSION,
        aggregate_id=order.id,
        trace_id=trace_id,
        occurred_at=order.created_at,
        payload=order_created_payload(order),
    )


@dataclass(frozen=True, slots=True)
class PendingEvent:
    row_id: int
    event_id: UUID
    event_type: str
    event_version: int
    trace_id: UUID
    occurred_at: datetime
    payload: dict[str, Any]
    attempts: int


def envelope_fields(event: PendingEvent) -> dict[str, str]:
    return {
        "event_id": str(event.event_id),
        "event_type": event.event_type,
        "event_version": str(event.event_version),
        # La columna guarda microsegundos y el contrato exige milisegundos con sufijo Z.
        "occurred_at": to_rfc3339(event.occurred_at),
        "trace_id": str(event.trace_id),
        "payload": json.dumps(event.payload, ensure_ascii=False, separators=(",", ":")),
    }
