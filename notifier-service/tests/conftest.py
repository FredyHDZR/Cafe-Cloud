from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

TEST_ENVIRONMENT = {
    "MONGO_URL": "mongodb://localhost:27017",
    "MONGO_DB": "cafecloud",
    "REDIS_URL": "redis://localhost:6379/0",
    "SERVICE_NAME": "notifier-service",
    "LOG_LEVEL": "INFO",
}

DEDUP_INDEX = "uq_notifications_event_id"


@pytest.fixture
def environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name, value in TEST_ENVIRONMENT.items():
        monkeypatch.setenv(name, value)

    from app.infra.config import get_consumer_settings, get_settings

    get_settings.cache_clear()
    get_consumer_settings.cache_clear()
    try:
        yield
    finally:
        get_settings.cache_clear()
        get_consumer_settings.cache_clear()


@pytest.fixture
def client(environment: None, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    from app.infra.mongo import MongoDatabase

    async def verified(self: MongoDatabase) -> str:
        return DEDUP_INDEX

    # Las pruebas unitarias no hablan con Mongo; el arranque real se verifica sobre el Compose.
    monkeypatch.setattr(MongoDatabase, "verify_dedup_index", verified)

    from app.main import create_app

    with TestClient(create_app()) as test_client:
        yield test_client
