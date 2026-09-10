import asyncio
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from app.domain.complete_order import CompleteOrderService, CompleteOutcome
from app.domain.envelope import OrderCreatedEnvelope, parse_order_created
from app.domain.errors import OrderAlreadyCompletedError, OrderNotCompletedError
from app.domain.events import EventDraft
from app.domain.order import OrderTransition

EVENT_ID = UUID("8f14e45f-ea0d-4b6c-9a1e-2c3b5d7f9012")
TRACE_ID = UUID("1f2a3b4c-5d6e-4f70-8123-9a0b1c2d3e4f")
ORDER_ID = UUID("7c9e6679-7425-40de-944b-e07fc1f90ae7")
COMPLETED_AT = datetime(2026, 9, 8, 20, 0, 3, 412000, tzinfo=UTC)
CONSUMER_GROUP = "processor"


def incoming() -> OrderCreatedEnvelope:
    payload = {
        "order_id": str(ORDER_ID),
        "customer_id": "abc123",
        "status": "PENDING",
        "items": [{"name": "latte", "qty": 1}, {"name": "muffin", "qty": 2}],
        "created_at": "2026-09-08T20:00:00.000Z",
    }
    return parse_order_created(
        {
            "event_id": str(EVENT_ID),
            "event_type": "orders.created",
            "event_version": "1",
            "occurred_at": "2026-09-08T20:00:00.000Z",
            "trace_id": str(TRACE_ID),
            "payload": json.dumps(payload),
        }
    )


class FakeProcessedEvents:
    def __init__(self, *, already_seen: bool = False) -> None:
        self._already_seen = already_seen
        self.reserved: list[tuple[UUID, str, str]] = []

    async def reserve(self, *, event_id: UUID, event_type: str, consumer_group: str) -> bool:
        self.reserved.append((event_id, event_type, consumer_group))
        return not self._already_seen


class FakeOrders:
    def __init__(self, transition: OrderTransition) -> None:
        self._transition = transition
        self.completed: list[UUID] = []

    async def complete(self, order_id: UUID) -> OrderTransition:
        self.completed.append(order_id)
        return self._transition


class FakeOutbox:
    def __init__(self) -> None:
        self.appended: list[EventDraft] = []

    async def append(self, event: EventDraft) -> UUID:
        self.appended.append(event)
        return uuid4()


class FakeUnitOfWork:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


def run(
    *,
    processed_events: FakeProcessedEvents,
    orders: FakeOrders,
    outbox: FakeOutbox,
    unit_of_work: FakeUnitOfWork,
    processing_ms: int = 3412,
) -> CompleteOutcome:
    service = CompleteOrderService(
        processed_events=processed_events,
        orders=orders,
        outbox=outbox,
        unit_of_work=unit_of_work,
        consumer_group=CONSUMER_GROUP,
    )
    return asyncio.run(service.run(incoming(), processing_ms=processing_ms))


def test_the_happy_path_updates_the_order_and_queues_orders_completed() -> None:
    orders = FakeOrders(
        OrderTransition(transitioned=True, completed_at=COMPLETED_AT, status="COMPLETED")
    )
    outbox = FakeOutbox()
    unit_of_work = FakeUnitOfWork()

    outcome = run(
        processed_events=FakeProcessedEvents(),
        orders=orders,
        outbox=outbox,
        unit_of_work=unit_of_work,
    )

    assert outcome.duplicate is False
    assert outcome.transitioned is True
    assert orders.completed == [ORDER_ID]
    assert unit_of_work.commits == 1
    draft = outbox.appended[0]
    assert draft.event_type == "orders.completed"
    assert draft.event_version == 1
    assert draft.occurred_at == COMPLETED_AT


def test_the_trace_id_of_the_incoming_event_is_copied_unchanged() -> None:
    outbox = FakeOutbox()

    run(
        processed_events=FakeProcessedEvents(),
        orders=FakeOrders(
            OrderTransition(transitioned=True, completed_at=COMPLETED_AT, status="COMPLETED")
        ),
        outbox=outbox,
        unit_of_work=FakeUnitOfWork(),
    )

    assert outbox.appended[0].trace_id == TRACE_ID


def test_the_payload_copies_customer_and_items_from_the_incoming_event() -> None:
    outbox = FakeOutbox()

    run(
        processed_events=FakeProcessedEvents(),
        orders=FakeOrders(
            OrderTransition(transitioned=True, completed_at=COMPLETED_AT, status="COMPLETED")
        ),
        outbox=outbox,
        unit_of_work=FakeUnitOfWork(),
        processing_ms=3412,
    )

    assert outbox.appended[0].payload == {
        "order_id": str(ORDER_ID),
        "customer_id": "abc123",
        "status": "COMPLETED",
        "items": [{"name": "latte", "qty": 1}, {"name": "muffin", "qty": 2}],
        "created_at": "2026-09-08T20:00:00.000Z",
        "completed_at": "2026-09-08T20:00:03.412Z",
        "processing_ms": 3412,
    }


def test_a_duplicate_neither_touches_the_order_nor_queues_a_second_event() -> None:
    orders = FakeOrders(
        OrderTransition(transitioned=True, completed_at=COMPLETED_AT, status="COMPLETED")
    )
    outbox = FakeOutbox()
    unit_of_work = FakeUnitOfWork()

    outcome = run(
        processed_events=FakeProcessedEvents(already_seen=True),
        orders=orders,
        outbox=outbox,
        unit_of_work=unit_of_work,
    )

    assert outcome.duplicate is True
    assert orders.completed == []
    assert outbox.appended == []
    assert unit_of_work.commits == 0
    assert unit_of_work.rollbacks == 1


def test_an_order_already_completed_is_rejected_without_touching_the_outbox() -> None:
    # ADR-007: la transicion, y no la deduplicacion, es lo que hace que orders.completed se emita
    # como mucho una vez por pedido.
    orders = FakeOrders(
        OrderTransition(transitioned=False, completed_at=COMPLETED_AT, status="COMPLETED")
    )
    outbox = FakeOutbox()
    unit_of_work = FakeUnitOfWork()

    with pytest.raises(OrderAlreadyCompletedError) as raised:
        run(
            processed_events=FakeProcessedEvents(),
            orders=orders,
            outbox=outbox,
            unit_of_work=unit_of_work,
        )

    assert raised.value.reason == "order_already_completed"
    assert orders.completed == [ORDER_ID]
    assert outbox.appended == []
    assert unit_of_work.commits == 0


def test_a_transition_without_completed_at_never_invents_one() -> None:
    # ADR-007: despues de la guarda, transitioned implica completed_at; el camino queda cerrado
    # de forma explicita para que ningun proceso vuelva a fabricar una marca de tiempo.
    outbox = FakeOutbox()

    with pytest.raises(OrderNotCompletedError):
        run(
            processed_events=FakeProcessedEvents(),
            orders=FakeOrders(
                OrderTransition(transitioned=True, completed_at=None, status="COMPLETED")
            ),
            outbox=outbox,
            unit_of_work=FakeUnitOfWork(),
        )

    assert outbox.appended == []


def test_the_dedup_reservation_carries_the_consumer_group() -> None:
    processed_events = FakeProcessedEvents()

    run(
        processed_events=processed_events,
        orders=FakeOrders(
            OrderTransition(transitioned=True, completed_at=COMPLETED_AT, status="COMPLETED")
        ),
        outbox=FakeOutbox(),
        unit_of_work=FakeUnitOfWork(),
    )

    assert processed_events.reserved == [(EVENT_ID, "orders.created", CONSUMER_GROUP)]


def test_an_order_that_is_not_completed_is_rejected_without_emitting_the_event() -> None:
    outbox = FakeOutbox()
    unit_of_work = FakeUnitOfWork()

    with pytest.raises(OrderNotCompletedError) as raised:
        run(
            processed_events=FakeProcessedEvents(),
            orders=FakeOrders(OrderTransition(transitioned=False, completed_at=None, status=None)),
            outbox=outbox,
            unit_of_work=unit_of_work,
        )

    assert raised.value.reason == "order_not_completed"
    assert outbox.appended == []
    assert unit_of_work.commits == 0


def test_an_order_in_an_unexpected_status_is_rejected_too() -> None:
    outbox = FakeOutbox()

    with pytest.raises(OrderNotCompletedError):
        run(
            processed_events=FakeProcessedEvents(),
            orders=FakeOrders(
                OrderTransition(transitioned=False, completed_at=None, status="CANCELLED")
            ),
            outbox=outbox,
            unit_of_work=FakeUnitOfWork(),
        )

    assert outbox.appended == []
