import pytest

from app.infra.config import ConsumerSettings
from tests.conftest import TEST_ENVIRONMENT


def build(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> ConsumerSettings:
    for name, value in {**TEST_ENVIRONMENT, **overrides}.items():
        monkeypatch.setenv(name, value)
    return ConsumerSettings()


def test_the_consumer_name_follows_the_event_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("socket.gethostname", lambda: "b3d4c5e6a1b2")
    settings = build(monkeypatch)

    assert settings.resolved_consumer_name == "notifier-b3d4c5e6a1b2"
    assert settings.stream == "orders.completed"
    assert settings.group == "notifier"


def test_an_explicit_consumer_name_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = build(monkeypatch, CONSUMER_NAME="notifier-1")

    assert settings.resolved_consumer_name == "notifier-1"


def test_the_collection_defaults_to_the_one_of_adr_005(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = build(monkeypatch)

    assert settings.mongo_collection == "notifications"
    assert settings.mongo_database == "cafecloud"
