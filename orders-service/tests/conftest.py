from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

TEST_ENVIRONMENT = {
    "DATABASE_URL": "postgresql+asyncpg://orders_rw:orders_dev_pw@localhost:5432/cafecloud",
    "REDIS_URL": "redis://localhost:6379/0",
    "SERVICE_NAME": "orders-service",
    "LOG_LEVEL": "INFO",
}


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    for name, value in TEST_ENVIRONMENT.items():
        monkeypatch.setenv(name, value)

    # La configuracion se resuelve al importar app.main, asi que el entorno tiene que existir antes.
    from app.infra.config import get_settings
    from app.main import create_app

    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as test_client:
            yield test_client
    finally:
        get_settings.cache_clear()
