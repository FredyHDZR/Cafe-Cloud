import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from app.domain.envelope import OrderCreatedEnvelope
from app.domain.order import OrderStatus
from app.domain.timestamps import to_rfc3339

ORDER_COMPLETED_TYPE = "orders.completed"
ORDER_COMPLETED_VERSION = 1


@dataclass(frozen=True, slots=True)
class OutboxStats:
    pending: int
    published: int
    failed: int
    oldest_pending_age_seconds: float


@dataclass(frozen=True, slots=True)
class EventDraft:
    event_type: str
    event_version: int
    aggregate_id: UUID
    trace_id: UUID
    occurred_at: datetime
    payload: dict[str, Any]


def order_completed_payload(
    source: OrderCreatedEnvelope, *, completed_at: datetime, processing_ms: int
) -> dict[str, Any]:
    order = source.payload
    return {
        "order_id": str(order.order_id),
        "customer_id": order.customer_id,
        "status": OrderStatus.COMPLETED.value,
        # Los items se copian del evento de entrada: processor_rw no ve orders.order_items.
        "items": [{"name": item.name, "qty": item.qty} for item in order.items],
        "created_at": to_rfc3339(order.created_at),
        "completed_at": to_rfc3339(completed_at),
        "processing_ms": processing_ms,
    }


def order_completed_event(
    source: OrderCreatedEnvelope, *, completed_at: datetime, processing_ms: int
) -> EventDraft:
    return EventDraft(
        event_type=ORDER_COMPLETED_TYPE,
        event_version=ORDER_COMPLETED_VERSION,
        aggregate_id=source.payload.order_id,
        trace_id=source.trace_id,
        occurred_at=completed_at,
        payload=order_completed_payload(
            source, completed_at=completed_at, processing_ms=processing_ms
        ),
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
