import asyncio
from collections.abc import Iterator
from typing import cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from prometheus_client.registry import REGISTRY

from app.api.deps import get_metrics_collector
from app.api.metrics import MetricsCollector, guarded
from app.infra.metrics import (
    CONTENT_TYPE,
    METRICS_COLLECTION_ERRORS,
    NOTIFICATIONS_INSERTED,
    register_consumer_metrics,
)


def sample(name: str, labels: dict[str, str] | None = None) -> float:
    value = REGISTRY.get_sample_value(name, labels or {})
    return 0.0 if value is None else value


class FakeCollector(MetricsCollector):
    def __init__(self) -> None:
        self.calls = 0

    async def collect(self) -> None:
        self.calls += 1


@pytest.fixture
def collector(client: TestClient) -> Iterator[FakeCollector]:
    fake = FakeCollector()
    app = cast(FastAPI, client.app)
    app.dependency_overrides[get_metrics_collector] = lambda: fake
    try:
        yield fake
    finally:
        app.dependency_overrides.clear()


def test_metrics_are_served_in_the_prometheus_exposition_format(
    client: TestClient, collector: FakeCollector
) -> None:
    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"] == CONTENT_TYPE
    assert "# TYPE cafecloud_notifications_stored gauge" in response.text


def test_the_scrape_refreshes_the_gauges_read_from_the_store(
    client: TestClient, collector: FakeCollector
) -> None:
    client.get("/metrics")
    client.get("/metrics")

    assert collector.calls == 2


def test_the_service_is_named_in_its_own_metrics(
    client: TestClient, collector: FakeCollector
) -> None:
    body = client.get("/metrics").text

    assert 'cafecloud_service_info{service="notifier-service",version="0.1.0"} 1.0' in body


def test_the_consumer_role_publishes_the_counters_it_increments(
    client: TestClient, collector: FakeCollector
) -> None:
    # La API no los publica porque no los mueve: son del proceso consumidor.
    register_consumer_metrics("orders.completed")
    before = sample("cafecloud_notifications_inserted_total")

    NOTIFICATIONS_INSERTED.inc()

    assert sample("cafecloud_notifications_inserted_total") == before + 1


def test_http_responses_are_counted_by_code(client: TestClient, collector: FakeCollector) -> None:
    client.get("/no-existe")

    body = client.get("/metrics").text
    assert 'cafecloud_http_requests_total{method="GET",path="unmatched",status="404"}' in body


def test_a_store_that_fails_is_counted_and_does_not_break_the_scrape(
    client: TestClient, collector: FakeCollector
) -> None:
    async def failing() -> None:
        raise ConnectionRefusedError("puerto cerrado")

    METRICS_COLLECTION_ERRORS.labels(source="mongo")
    before = sample("cafecloud_metrics_collection_errors_total", {"source": "mongo"})

    asyncio.run(guarded("mongo", failing()))

    assert sample("cafecloud_metrics_collection_errors_total", {"source": "mongo"}) == before + 1
    assert client.get("/metrics").status_code == 200
