import logging
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from app.domain.envelope import OrderCreatedEnvelope
from app.domain.errors import (
    NonRetryableError,
    OrderAlreadyCompletedError,
    OrderNotCompletedError,
)
from app.domain.events import EventDraft, order_completed_event
from app.domain.order import OrderStatus, OrderTransition
from app.infra.logging import log_context

logger = logging.getLogger(__name__)


class ProcessedEventStore(Protocol):
    async def reserve(self, *, event_id: UUID, event_type: str, consumer_group: str) -> bool: ...


class OrderStore(Protocol):
    async def complete(self, order_id: UUID) -> OrderTransition: ...


class OutboxStore(Protocol):
    async def append(self, event: EventDraft) -> UUID: ...


class UnitOfWork(Protocol):
    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


@dataclass(frozen=True, slots=True)
class CompleteOutcome:
    duplicate: bool
    transitioned: bool = False
    outgoing_event_id: UUID | None = None


class CompleteOrderService:
    def __init__(
        self,
        *,
        processed_events: ProcessedEventStore,
        orders: OrderStore,
        outbox: OutboxStore,
        unit_of_work: UnitOfWork,
        consumer_group: str,
    ) -> None:
        self._processed_events = processed_events
        self._orders = orders
        self._outbox = outbox
        self._unit_of_work = unit_of_work
        self._consumer_group = consumer_group

    async def run(self, event: OrderCreatedEnvelope, *, processing_ms: int) -> CompleteOutcome:
        reserved = await self._processed_events.reserve(
            event_id=event.event_id,
            event_type=event.event_type,
            consumer_group=self._consumer_group,
        )
        if not reserved:
            await self._unit_of_work.rollback()
            logger.debug(
                "event_already_processed",
                extra=log_context(
                    event_id=str(event.event_id),
                    event_type=event.event_type,
                    order_id=str(event.payload.order_id),
                ),
            )
            return CompleteOutcome(duplicate=True)

        transition = await self._orders.complete(event.payload.order_id)
        # ADR-006 y ADR-007: solo emite quien de verdad llevo la fila de PENDING a COMPLETED.
        if not transition.transitioned:
            raise _rejection(event.payload.order_id, transition.status)
        completed_at = transition.completed_at
        if completed_at is None:
            message = f"el pedido {event.payload.order_id} transiciono sin completed_at"
            raise OrderNotCompletedError(message)
        outgoing = order_completed_event(
            event, completed_at=completed_at, processing_ms=processing_ms
        )
        outgoing_event_id = await self._outbox.append(outgoing)
        await self._unit_of_work.commit()

        logger.info(
            "order_completed",
            extra=log_context(
                event_id=str(event.event_id),
                order_id=str(event.payload.order_id),
                transitioned=transition.transitioned,
                outgoing_event_id=str(outgoing_event_id),
                processing_ms=processing_ms,
            ),
        )
        return CompleteOutcome(
            duplicate=False,
            transitioned=transition.transitioned,
            outgoing_event_id=outgoing_event_id,
        )


def _rejection(order_id: UUID, status: str | None) -> NonRetryableError:
    if status == OrderStatus.COMPLETED.value:
        message = f"el pedido {order_id} ya estaba COMPLETED antes de este evento"
        return OrderAlreadyCompletedError(message)
    message = f"el pedido {order_id} no quedo COMPLETED: status={status!r}"
    return OrderNotCompletedError(message)
