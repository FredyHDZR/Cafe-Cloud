import asyncio
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import DESCENDING

from app.infra.mongo import Document
from app.repositories.notifications import NotificationRepository

CUSTOMER = "abc123"


def document(*, order_id: str, created_at: datetime, customer_id: str = CUSTOMER) -> Document:
    suffix = order_id[-12:]
    return {
        "_id": f"objectid-{suffix}",
        "event_id": f"11111111-1111-4111-8111-{suffix}",
        "order_id": order_id,
        "customer_id": customer_id,
        "status": "COMPLETED",
        "message": f"Tu pedido {order_id} esta listo",
        "trace_id": f"22222222-2222-4222-8222-{suffix}",
        "created_at": created_at,
    }


class FakeCursor:
    def __init__(self, collection: "FakeCollection", documents: list[Document]) -> None:
        self._collection = collection
        self._documents = documents

    def sort(self, field: str, direction: int) -> "FakeCursor":
        self._collection.calls["sort"] = (field, direction)
        self._documents.sort(key=lambda item: item["created_at"], reverse=direction == DESCENDING)
        return self

    def skip(self, offset: int) -> "FakeCursor":
        self._collection.calls["skip"] = offset
        self._documents = self._documents[offset:]
        return self

    def limit(self, limit: int) -> "FakeCursor":
        self._collection.calls["limit"] = limit
        self._documents = self._documents[:limit]
        return self

    async def to_list(self, length: int | None) -> list[Document]:
        self._collection.calls["to_list"] = length
        return self._documents


class FakeCollection:
    def __init__(self, documents: list[Document]) -> None:
        self._documents = documents
        self.calls: dict[str, Any] = {}

    def _matching(self, criteria: Document) -> list[Document]:
        return [item for item in self._documents if item["customer_id"] == criteria["customer_id"]]

    def find(self, criteria: Document, projection: Document | None = None) -> FakeCursor:
        self.calls["find"] = (criteria, projection)
        return FakeCursor(self, self._matching(criteria))

    async def count_documents(self, criteria: Document) -> int:
        self.calls["count_documents"] = criteria
        return len(self._matching(criteria))


def repository_over(documents: list[Document]) -> tuple[NotificationRepository, FakeCollection]:
    collection = FakeCollection(documents)
    typed = cast(AsyncIOMotorCollection[Document], collection)
    return NotificationRepository(typed), collection


def moment(day: int) -> datetime:
    return datetime(2026, 9, day, 12, 0, tzinfo=UTC)


def test_list_by_customer_excludes_the_mongo_identifier_in_the_query() -> None:
    repository, collection = repository_over([])

    asyncio.run(repository.list_by_customer(CUSTOMER, limit=50, offset=0))

    assert collection.calls["find"] == ({"customer_id": CUSTOMER}, {"_id": 0})


def test_list_by_customer_sorts_by_created_at_descending() -> None:
    documents = [
        document(order_id="aaaaaaaa-aaaa-4aaa-8aaa-000000000001", created_at=moment(1)),
        document(order_id="aaaaaaaa-aaaa-4aaa-8aaa-000000000003", created_at=moment(3)),
        document(order_id="aaaaaaaa-aaaa-4aaa-8aaa-000000000002", created_at=moment(2)),
    ]
    repository, collection = repository_over(documents)

    page = asyncio.run(repository.list_by_customer(CUSTOMER, limit=50, offset=0))

    assert collection.calls["sort"] == ("created_at", DESCENDING)
    assert [item.created_at for item in page.items] == [moment(3), moment(2), moment(1)]


def test_list_by_customer_applies_limit_and_offset() -> None:
    documents = [
        document(order_id=f"aaaaaaaa-aaaa-4aaa-8aaa-00000000000{day}", created_at=moment(day))
        for day in (1, 2, 3)
    ]
    repository, collection = repository_over(documents)

    page = asyncio.run(repository.list_by_customer(CUSTOMER, limit=1, offset=1))

    assert (collection.calls["skip"], collection.calls["limit"]) == (1, 1)
    assert page.limit == 1
    assert page.offset == 1
    assert [item.created_at for item in page.items] == [moment(2)]


def test_total_counts_every_notification_of_the_customer_not_the_page() -> None:
    documents = [
        document(order_id=f"aaaaaaaa-aaaa-4aaa-8aaa-00000000000{day}", created_at=moment(day))
        for day in (1, 2, 3)
    ]
    documents.append(
        document(
            order_id="bbbbbbbb-bbbb-4bbb-8bbb-000000000009",
            created_at=moment(4),
            customer_id="otro",
        )
    )
    repository, _ = repository_over(documents)

    page = asyncio.run(repository.list_by_customer(CUSTOMER, limit=1, offset=0))

    assert len(page.items) == 1
    assert page.total == 3


def test_offset_beyond_the_total_returns_an_empty_page_with_the_real_total() -> None:
    documents = [document(order_id="aaaaaaaa-aaaa-4aaa-8aaa-000000000001", created_at=moment(1))]
    repository, _ = repository_over(documents)

    page = asyncio.run(repository.list_by_customer(CUSTOMER, limit=50, offset=99))

    assert page.items == ()
    assert page.total == 1


def test_documents_map_to_the_domain_notification() -> None:
    order_id = "aaaaaaaa-aaaa-4aaa-8aaa-000000000001"
    repository, _ = repository_over([document(order_id=order_id, created_at=moment(1))])

    page = asyncio.run(repository.list_by_customer(CUSTOMER, limit=50, offset=0))

    notification = page.items[0]
    assert notification.order_id == UUID(order_id)
    assert notification.event_id == UUID("11111111-1111-4111-8111-000000000001")
    assert notification.customer_id == CUSTOMER
    assert notification.status == "COMPLETED"
    assert notification.trace_id == UUID("22222222-2222-4222-8222-000000000001")


def test_naive_dates_are_read_as_utc() -> None:
    naive = datetime(2026, 9, 1, 12, 0)
    repository, _ = repository_over(
        [document(order_id="aaaaaaaa-aaaa-4aaa-8aaa-000000000001", created_at=naive)]
    )

    page = asyncio.run(repository.list_by_customer(CUSTOMER, limit=50, offset=0))

    assert page.items[0].created_at == naive.replace(tzinfo=UTC)
