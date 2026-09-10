from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

TEST_ENVIRONMENT = {
    "DATABASE_URL": "postgresql+asyncpg://processor_rw:processor_dev_pw@localhost:5432/cafecloud",
    "REDIS_URL": "redis://localhost:6379/0",
    "SERVICE_NAME": "processor-service",
    "LOG_LEVEL": "INFO",
}


@pytest.fixture
def environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name, value in TEST_ENVIRONMENT.items():
        monkeypatch.setenv(name, value)

    from app.infra.config import get_consumer_settings, get_settings

    get_settings.cache_clear()
    get_consumer_settings.cache_clear()
    try:
        yield None
    finally:
        get_settings.cache_clear()
        get_consumer_settings.cache_clear()


@pytest.fixture
def client(environment: None) -> Iterator[TestClient]:
    from app.api.observability import create_observability_app
    from app.infra.config import get_consumer_settings
    from app.infra.database import Database
    from app.infra.streams import RedisStreamConsumer

    settings = get_consumer_settings()
    # Ni el motor de SQLAlchemy ni el cliente de Redis conectan al construirse: la aplicacion
    # se levanta entera sin almacenes, y las sondas se sustituyen en cada prueba.
    app = create_observability_app(
        settings=settings,
        database=Database.from_settings(settings),
        stream=RedisStreamConsumer.from_settings(settings),
    )
    with TestClient(app) as test_client:
        yield test_client
