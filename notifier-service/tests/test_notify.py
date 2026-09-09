import asyncio
import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from app.domain.envelope import OrderCompletedEnvelope, parse_order_completed
from app.domain.notification import Notification, notification_from_event
from app.domain.notify import NotifyCustomerService
from app.repositories.notifications import to_document

ORDER_ID = UUID("7c9e6679-7425-40de-944b-e07fc1f90ae7")


def event(**overrides: Any) -> OrderCompletedEnvelope:
    payload = {
        "order_id": str(ORDER_ID),
        "customer_id": "abc123",
        "status": "COMPLETED",
        "items": [{"name": "latte", "qty": 1}, {"name": "muffin", "qty": 2}],
        "created_at": "2026-09-09T20:00:00.000Z",
        "completed_at": "2026-09-09T20:00:03.412Z",
        "processing_ms": 3412,
    }
    fields: dict[str, str] = {
        "event_id": str(uuid4()),
        "event_type": "orders.completed",
        "event_version": "1",
        "occurred_at": "2026-09-09T20:00:03.412Z",
        "trace_id": str(uuid4()),
        "payload": json.dumps(payload),
    }
    fields.update(overrides)
    return parse_order_completed(fields)


class FakeStore:
    def __init__(self) -> None:
        self.documents: dict[str, Notification] = {}

    async def insert(self, notification: Notification) -> bool:
        key = str(notification.event_id)
        if key in self.documents:
            return False
        self.documents[key] = notification
        return True


def test_message_names_the_order() -> None:
    notification = notification_from_event(event())

    assert notification.message == f"Tu pedido {ORDER_ID} está listo"


def test_notification_copies_the_identity_of_the_event() -> None:
    source = event()

    notification = notification_from_event(source)

    assert notification.event_id == source.event_id
    assert notification.trace_id == source.trace_id
    assert notification.order_id == ORDER_ID
    assert notification.customer_id == "abc123"
    assert notification.status == "COMPLETED"


def test_created_at_is_the_moment_of_the_notification() -> None:
    before = datetime.now(UTC)

    notification = notification_from_event(event())

    assert before <= notification.created_at <= datetime.now(UTC)


def test_document_has_exactly_the_fields_of_adr_005() -> None:
    document = to_document(notification_from_event(event()))

    assert set(document) == {
        "event_id",
        "order_id",
        "customer_id",
        "status",
        "message",
        "trace_id",
        "created_at",
    }
    assert isinstance(document["event_id"], str)
    assert isinstance(document["created_at"], datetime)


def test_first_delivery_writes_the_notification() -> None:
    store = FakeStore()

    outcome = asyncio.run(NotifyCustomerService(notifications=store).run(event()))

    assert outcome.duplicate is False
    assert len(store.documents) == 1


def test_redelivery_of_the_same_event_id_writes_once() -> None:
    store = FakeStore()
    service = NotifyCustomerService(notifications=store)
    source = event()

    first = asyncio.run(service.run(source))
    second = asyncio.run(service.run(source))

    assert first.duplicate is False
    assert second.duplicate is True
    assert len(store.documents) == 1


def test_two_events_of_the_same_order_are_two_notifications() -> None:
    store = FakeStore()
    service = NotifyCustomerService(notifications=store)

    asyncio.run(service.run(event()))
    asyncio.run(service.run(event()))

    assert len(store.documents) == 2
