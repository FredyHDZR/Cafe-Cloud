from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.deps import get_notification_repository
from app.domain.notification import Notification, NotificationPage

CUSTOMER = "abc123"


def notification(created_at: datetime) -> Notification:
    order_id = uuid4()
    return Notification(
        event_id=uuid4(),
        order_id=order_id,
        customer_id=CUSTOMER,
        status="COMPLETED",
        message=f"Tu pedido {order_id} esta listo",
        trace_id=uuid4(),
        created_at=created_at,
    )


class FakeRepository:
    def __init__(self, notifications: list[Notification]) -> None:
        self._notifications = notifications
        self.calls: list[dict[str, Any]] = []

    async def list_by_customer(
        self, customer_id: str, *, limit: int, offset: int
    ) -> NotificationPage:
        self.calls.append({"customer_id": customer_id, "limit": limit, "offset": offset})
        owned = [item for item in self._notifications if item.customer_id == customer_id]
        window = owned[offset : offset + limit]
        return NotificationPage(items=tuple(window), total=len(owned), limit=limit, offset=offset)


@pytest.fixture
def repository(client: TestClient) -> Iterator[FakeRepository]:
    fake = FakeRepository(
        [
            notification(datetime(2026, 9, 9, 12, 0, tzinfo=UTC)),
            notification(datetime(2026, 9, 8, 12, 0, tzinfo=UTC)),
            notification(datetime(2026, 9, 7, 12, 0, tzinfo=UTC)),
        ]
    )
    app = cast(FastAPI, client.app)
    app.dependency_overrides[get_notification_repository] = lambda: fake
    try:
        yield fake
    finally:
        app.dependency_overrides.clear()


def test_returns_the_page_wrapper(client: TestClient, repository: FakeRepository) -> None:
    response = client.get(f"/notifications/{CUSTOMER}")

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"items", "total", "limit", "offset"}
    assert body["total"] == 3
    assert len(body["items"]) == 3


def test_items_carry_the_seven_fields_of_the_document(
    client: TestClient, repository: FakeRepository
) -> None:
    body = client.get(f"/notifications/{CUSTOMER}").json()

    item = body["items"][0]
    assert set(item) == {
        "event_id",
        "order_id",
        "customer_id",
        "status",
        "message",
        "trace_id",
        "created_at",
    }
    UUID(item["order_id"])
    assert item["created_at"].endswith("Z")


def test_the_mongo_identifier_never_reaches_the_client(
    client: TestClient, repository: FakeRepository
) -> None:
    response = client.get(f"/notifications/{CUSTOMER}")

    assert '"_id"' not in response.text
    assert all("_id" not in item for item in response.json()["items"])


def test_a_customer_without_notifications_gets_an_empty_page_not_a_404(
    client: TestClient, repository: FakeRepository
) -> None:
    response = client.get("/notifications/no-existe")

    assert response.status_code == 200
    assert response.json() == {"items": [], "total": 0, "limit": 50, "offset": 0}


def test_default_pagination_is_fifty_from_zero(
    client: TestClient, repository: FakeRepository
) -> None:
    client.get(f"/notifications/{CUSTOMER}")

    assert repository.calls == [{"customer_id": CUSTOMER, "limit": 50, "offset": 0}]


def test_pagination_walks_the_customer_history(
    client: TestClient, repository: FakeRepository
) -> None:
    first = client.get(f"/notifications/{CUSTOMER}?limit=1&offset=0").json()
    second = client.get(f"/notifications/{CUSTOMER}?limit=1&offset=1").json()

    assert first["total"] == second["total"] == 3
    assert len(first["items"]) == len(second["items"]) == 1
    assert first["items"][0]["order_id"] != second["items"][0]["order_id"]


def test_offset_beyond_the_total_is_an_empty_page(
    client: TestClient, repository: FakeRepository
) -> None:
    body = client.get(f"/notifications/{CUSTOMER}?offset=99").json()

    assert body["items"] == []
    assert body["total"] == 3
    assert body["offset"] == 99


@pytest.mark.parametrize(
    "query",
    ["limit=201", "limit=0", "limit=-1", "offset=-1", "limit=abc"],
)
def test_pagination_out_of_range_is_rejected_with_the_error_contract(
    client: TestClient, repository: FakeRepository, query: str
) -> None:
    response = client.get(f"/notifications/{CUSTOMER}?{query}")

    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "validation_error"
    assert UUID(error["trace_id"])
    assert repository.calls == []


def test_the_maximum_limit_is_accepted(client: TestClient, repository: FakeRepository) -> None:
    response = client.get(f"/notifications/{CUSTOMER}?limit=200")

    assert response.status_code == 200
    assert repository.calls == [{"customer_id": CUSTOMER, "limit": 200, "offset": 0}]
