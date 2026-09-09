from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from app.infra.mongo import MissingDedupIndexError, MongoDatabase
from app.infra.tracing import TRACE_ID_HEADER


def test_health_returns_ok(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "notifier-service"}


def test_health_generates_trace_id_when_absent(client: TestClient) -> None:
    response = client.get("/health")

    UUID(response.headers[TRACE_ID_HEADER])


def test_health_propagates_incoming_trace_id(client: TestClient) -> None:
    trace_id = "2f8a6f1c-0f0e-4a1a-9c3c-9f2f1f0a1b2c"

    response = client.get("/health", headers={TRACE_ID_HEADER: trace_id})

    assert response.headers[TRACE_ID_HEADER] == trace_id


def test_unknown_route_uses_the_error_contract(client: TestClient) -> None:
    response = client.get("/no-existe")

    assert response.status_code == 404
    body = response.json()
    assert body["error"]["code"] == "not_found"
    assert UUID(body["error"]["trace_id"])


def test_app_refuses_to_start_without_the_dedup_index(
    environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def missing(self: MongoDatabase) -> str:
        raise MissingDedupIndexError("cafecloud.notifications")

    monkeypatch.setattr(MongoDatabase, "verify_dedup_index", missing)

    from app.main import create_app

    with pytest.raises(MissingDedupIndexError), TestClient(create_app()):
        pass
