from collections.abc import Iterator
from typing import Any

import httpx
import psycopg
import pytest
from pymongo import MongoClient
from pymongo.collection import Collection
from redis import Redis

from tests.support import (
    Broker,
    Case,
    Config,
    MetricsReader,
    NotificationsApi,
    OrdersApi,
    Warehouse,
    wait_until,
)


@pytest.fixture(scope="session")
def config() -> Config:
    return Config.from_environment()


@pytest.fixture(scope="session")
def orders_client(config: Config) -> Iterator[httpx.Client]:
    with httpx.Client(base_url=config.orders_url, timeout=10.0) as client:
        yield client


@pytest.fixture(scope="session")
def notifier_client(config: Config) -> Iterator[httpx.Client]:
    with httpx.Client(base_url=config.notifier_url, timeout=10.0) as client:
        yield client


@pytest.fixture(scope="session")
def processor_client(config: Config) -> Iterator[httpx.Client]:
    with httpx.Client(base_url=config.processor_url, timeout=10.0) as client:
        yield client


@pytest.fixture(scope="session")
def notifier_consumer_client(config: Config) -> Iterator[httpx.Client]:
    with httpx.Client(base_url=config.notifier_consumer_url, timeout=10.0) as client:
        yield client


@pytest.fixture(scope="session", autouse=True)
def environment_ready(
    config: Config,
    orders_client: httpx.Client,
    processor_client: httpx.Client,
    notifier_client: httpx.Client,
    notifier_consumer_client: httpx.Client,
) -> None:
    # Un contenedor sano solo dice que su proceso vive; la preparacion se pregunta aparte.
    clients = {
        "orders-service": orders_client,
        "processor-service": processor_client,
        "notifier-service": notifier_client,
        "notifier-consumer": notifier_consumer_client,
    }
    for name, client in clients.items():
        _wait_for_ready(client, name=name, timeout=config.ready_timeout)


def _wait_for_ready(client: httpx.Client, *, name: str, timeout: float) -> None:
    def ready() -> bool | None:
        try:
            return True if client.get("/health/ready").status_code == 200 else None
        except httpx.HTTPError:
            return None

    wait_until(ready, timeout=timeout, description=f"{name} no llego a /health/ready")


@pytest.fixture(scope="session")
def connection(config: Config) -> Iterator[psycopg.Connection[tuple[Any, ...]]]:
    with psycopg.connect(config.database_url, autocommit=True) as open_connection:
        yield open_connection


@pytest.fixture(scope="session")
def warehouse(connection: psycopg.Connection[tuple[Any, ...]]) -> Warehouse:
    return Warehouse(connection)


@pytest.fixture(scope="session")
def notifications_collection(config: Config) -> Iterator[Collection[dict[str, Any]]]:
    with MongoClient[dict[str, Any]](config.mongo_url) as client:
        yield client[config.mongo_db]["notifications"]


@pytest.fixture(scope="session")
def broker(config: Config) -> Iterator[Broker]:
    client = Redis.from_url(config.redis_url, decode_responses=True)
    try:
        yield Broker(client)
    finally:
        client.close()


@pytest.fixture
def orders(orders_client: httpx.Client) -> OrdersApi:
    return OrdersApi(orders_client)


@pytest.fixture
def notifications(notifier_client: httpx.Client) -> NotificationsApi:
    return NotificationsApi(notifier_client)


@pytest.fixture
def processor_metrics(processor_client: httpx.Client) -> MetricsReader:
    return MetricsReader(processor_client)


@pytest.fixture
def notifier_metrics(notifier_consumer_client: httpx.Client) -> MetricsReader:
    return MetricsReader(notifier_consumer_client)


@pytest.fixture
def case(
    warehouse: Warehouse,
    notifications_collection: Collection[dict[str, Any]],
    broker: Broker,
) -> Iterator[Case]:
    running = Case.new()
    try:
        yield running
    finally:
        for stream, entry_id in running.tracked_entries:
            broker.delete(stream, entry_id)
        broker.drop(*running.tracked_streams)
        notifications_collection.delete_many({"customer_id": running.customer_id})
        warehouse.forget(running.customer_id, running.key_prefix)
