from collections.abc import Iterator
from typing import cast
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.deps import get_health_service
from app.domain.health import Dependency, HealthService
from app.infra.tracing import TRACE_ID_HEADER

PROBE_TIMEOUT_SECONDS = 0.05


async def up() -> None:
    return None


async def down() -> None:
    raise ConnectionRefusedError("puerto cerrado")


@pytest.fixture
def health(client: TestClient) -> Iterator[list[Dependency]]:
    dependencies: list[Dependency] = []
    app = cast(FastAPI, client.app)
    app.dependency_overrides[get_health_service] = lambda: HealthService(
        dependencies, timeout_seconds=PROBE_TIMEOUT_SECONDS
    )
    try:
        yield dependencies
    finally:
        app.dependency_overrides.clear()


def test_liveness_does_not_look_at_any_dependency(client: TestClient) -> None:
    response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "alive", "service": "orders-service"}


def test_health_reports_every_dependency_when_all_are_up(
    client: TestClient, health: list[Dependency]
) -> None:
    health.extend(
        [Dependency(name="postgres", probe=up), Dependency(name="redis", probe=up, critical=False)]
    )

    response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "orders-service"
    assert [(item["name"], item["status"]) for item in body["dependencies"]] == [
        ("postgres", "up"),
        ("redis", "up"),
    ]


def test_health_returns_503_when_a_critical_dependency_is_down(
    client: TestClient, health: list[Dependency]
) -> None:
    health.append(Dependency(name="postgres", probe=down))

    response = client.get("/health")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "down"
    assert body["dependencies"][0]["critical"] is True
    assert "ConnectionRefusedError" in body["dependencies"][0]["error"]


def test_redis_down_degrades_orders_service_without_taking_it_out_of_service(
    client: TestClient, health: list[Dependency]
) -> None:
    # ADR-002: el outbox absorbe la caida del broker, asi que POST /orders sigue en pie.
    health.extend(
        [
            Dependency(name="postgres", probe=up),
            Dependency(name="redis", probe=down, critical=False),
        ]
    )

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "degraded"


def test_readiness_is_the_same_report_as_health(
    client: TestClient, health: list[Dependency]
) -> None:
    health.append(Dependency(name="postgres", probe=up))

    ready = client.get("/health/ready").json()
    health_body = client.get("/health").json()

    assert ready["status"] == health_body["status"]
    assert [item["name"] for item in ready["dependencies"]] == [
        item["name"] for item in health_body["dependencies"]
    ]


def test_health_generates_trace_id_when_absent(client: TestClient) -> None:
    response = client.get("/health/live")

    UUID(response.headers[TRACE_ID_HEADER])


def test_health_propagates_incoming_trace_id(client: TestClient) -> None:
    trace_id = "2f8a6f1c-0f0e-4a1a-9c3c-9f2f1f0a1b2c"

    response = client.get("/health/live", headers={TRACE_ID_HEADER: trace_id})

    assert response.headers[TRACE_ID_HEADER] == trace_id


def test_unknown_route_uses_the_error_contract(client: TestClient) -> None:
    response = client.get("/no-existe")

    assert response.status_code == 404
    body = response.json()
    assert body["error"]["code"] == "not_found"
    assert UUID(body["error"]["trace_id"])
