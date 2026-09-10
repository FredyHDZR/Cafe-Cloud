import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from app.api.schemas.orders import render_order
from app.domain.create_order import CreateOrderCommand, CreateOrderResult, CreateOrderService
from app.domain.errors import IdempotencyKeyInProgressError, IdempotencyKeyReuseError
from app.domain.events import EventDraft
from app.domain.idempotency import IdempotencyRecord, IdempotencyState, new_order_fingerprint
from app.domain.order import NewOrder, Order, OrderItem, OrderStatus

ORDER_ID = UUID("7c9e6679-7425-40de-944b-e07fc1f90ae7")
EVENT_ID = UUID("8f14e45f-ea0d-4b6c-9a1e-2c3b5d7f9012")
TRACE_ID = UUID("1f2a3b4c-5d6e-4f70-8123-9a0b1c2d3e4f")
CREATED_AT = datetime(2026, 9, 9, 20, 0, 0, tzinfo=UTC)
IDEMPOTENCY_KEY = "k-001"

NEW_ORDER = NewOrder(
    customer_id="abc123",
    items=(OrderItem(name="latte", qty=1), OrderItem(name="muffin", qty=2)),
)


class FakeOrderStore:
    def __init__(self) -> None:
        self.added: list[NewOrder] = []

    async def add(self, draft: NewOrder) -> Order:
        self.added.append(draft)
        return Order(
            id=ORDER_ID,
            customer_id=draft.customer_id,
            status=OrderStatus.PENDING,
            items=draft.items,
            created_at=CREATED_AT,
        )


class FakeOutboxStore:
    def __init__(self) -> None:
        self.events: list[EventDraft] = []

    async def append(self, event: EventDraft) -> UUID:
        self.events.append(event)
        return EVENT_ID


class FakeIdempotencyStore:
    def __init__(self, *, stored: IdempotencyRecord | None = None, visible: bool = True) -> None:
        self._stored = stored
        self._visible = visible
        self.reserved = False
        self.completed: tuple[UUID, int, dict[str, Any]] | None = None

    async def reserve(self, endpoint: str, key: str, fingerprint: str) -> bool:
        if self._stored is not None:
            return False
        self._stored = IdempotencyRecord(
            request_hash=fingerprint,
            state=IdempotencyState.IN_PROGRESS,
            order_id=None,
            response_status=None,
            response_body=None,
        )
        self.reserved = True
        return True

    async def find(self, endpoint: str, key: str) -> IdempotencyRecord | None:
        return self._stored if self._visible else None

    async def complete(
        self,
        endpoint: str,
        key: str,
        *,
        order_id: UUID,
        response_status: int,
        response_body: dict[str, Any],
    ) -> None:
        self.completed = (order_id, response_status, response_body)


class FakeUnitOfWork:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


def _build(
    idempotency: FakeIdempotencyStore,
) -> tuple[CreateOrderService, FakeOrderStore, FakeOutboxStore, FakeUnitOfWork]:
    orders = FakeOrderStore()
    outbox = FakeOutboxStore()
    unit_of_work = FakeUnitOfWork()
    service = CreateOrderService(
        orders=orders,
        outbox=outbox,
        idempotency=idempotency,
        unit_of_work=unit_of_work,
        render=render_order,
    )
    return service, orders, outbox, unit_of_work


def _execute(service: CreateOrderService) -> CreateOrderResult:
    return asyncio.run(
        service.execute(
            CreateOrderCommand(idempotency_key=IDEMPOTENCY_KEY, trace_id=TRACE_ID, order=NEW_ORDER)
        )
    )


def _completed_record(*, request_hash: str | None = None) -> IdempotencyRecord:
    return IdempotencyRecord(
        request_hash=request_hash or new_order_fingerprint(NEW_ORDER),
        state=IdempotencyState.COMPLETED,
        order_id=ORDER_ID,
        response_status=201,
        response_body={"order_id": str(ORDER_ID), "status": "PENDING"},
    )


def test_first_request_creates_the_order_and_commits_once() -> None:
    idempotency = FakeIdempotencyStore()
    service, orders, _, unit_of_work = _build(idempotency)

    result = _execute(service)

    assert result.status_code == 201
    assert result.replayed is False
    assert result.body["order_id"] == str(ORDER_ID)
    assert result.body["status"] == "PENDING"
    assert result.body["created_at"] == "2026-09-09T20:00:00.000Z"
    assert orders.added == [NEW_ORDER]
    assert unit_of_work.commits == 1
    assert unit_of_work.rollbacks == 0


def test_first_request_writes_the_orders_created_envelope_to_the_outbox() -> None:
    idempotency = FakeIdempotencyStore()
    service, _, outbox, _ = _build(idempotency)

    _execute(service)
    event = outbox.events[0]

    assert event.event_type == "orders.created"
    assert event.event_version == 1
    assert event.aggregate_id == ORDER_ID
    assert event.trace_id == TRACE_ID
    assert event.occurred_at == CREATED_AT
    assert event.payload == {
        "order_id": str(ORDER_ID),
        "customer_id": "abc123",
        "status": "PENDING",
        "items": [{"name": "latte", "qty": 1}, {"name": "muffin", "qty": 2}],
        "created_at": "2026-09-09T20:00:00.000Z",
    }


def test_first_request_stores_the_response_before_committing() -> None:
    idempotency = FakeIdempotencyStore()
    service, _, _, _ = _build(idempotency)

    result = _execute(service)

    assert idempotency.completed == (ORDER_ID, 201, result.body)


def test_same_key_and_body_replays_the_stored_response() -> None:
    idempotency = FakeIdempotencyStore(stored=_completed_record())
    service, orders, outbox, unit_of_work = _build(idempotency)

    result = _execute(service)

    assert result.replayed is True
    assert result.status_code == 201
    assert result.body == {"order_id": str(ORDER_ID), "status": "PENDING"}
    assert orders.added == []
    assert outbox.events == []
    assert unit_of_work.commits == 0
    assert unit_of_work.rollbacks == 1


def test_same_key_with_a_different_body_is_a_conflict() -> None:
    idempotency = FakeIdempotencyStore(stored=_completed_record(request_hash="0" * 64))
    service, orders, outbox, _ = _build(idempotency)

    with pytest.raises(IdempotencyKeyReuseError) as excinfo:
        _execute(service)

    assert excinfo.value.code == "idempotency_key_reuse"
    assert int(excinfo.value.status) == 409
    assert orders.added == []
    assert outbox.events == []


def test_same_key_still_in_progress_is_rejected_without_waiting() -> None:
    in_progress = IdempotencyRecord(
        request_hash=new_order_fingerprint(NEW_ORDER),
        state=IdempotencyState.IN_PROGRESS,
        order_id=None,
        response_status=None,
        response_body=None,
    )
    idempotency = FakeIdempotencyStore(stored=in_progress)
    service, orders, _, unit_of_work = _build(idempotency)

    with pytest.raises(IdempotencyKeyInProgressError) as excinfo:
        _execute(service)

    assert excinfo.value.code == "idempotency_key_in_progress"
    assert int(excinfo.value.status) == 409
    assert excinfo.value.headers["Retry-After"] == "1"
    assert orders.added == []
    assert unit_of_work.rollbacks == 1


def test_conflict_against_a_vanished_row_is_treated_as_in_progress() -> None:
    idempotency = FakeIdempotencyStore(stored=_completed_record(), visible=False)
    service, orders, _, _ = _build(idempotency)

    with pytest.raises(IdempotencyKeyInProgressError):
        _execute(service)

    assert orders.added == []
