import logging
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from app.domain.envelope import OrderCreatedEnvelope
from app.domain.errors import OrderNotCompletedError
from app.domain.events import EventDraft, order_completed_event
from app.domain.order import OrderStatus, OrderTransition
from app.domain.timestamps import utc_now
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
        # ADR-006: sin fila en COMPLETED el evento seria una promesa, no un hecho.
        if not transition.transitioned and transition.status != OrderStatus.COMPLETED.value:
            message = (
                f"el pedido {event.payload.order_id} no quedo COMPLETED: "
                f"status={transition.status!r}"
            )
            raise OrderNotCompletedError(message)
        completed_at = transition.completed_at or utc_now()
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
