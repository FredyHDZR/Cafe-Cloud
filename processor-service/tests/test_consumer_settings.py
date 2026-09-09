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

    assert settings.resolved_consumer_name == "processor-b3d4c5e6a1b2"
    assert settings.stream == "orders.created"
    assert settings.group == "processor"


def test_an_explicit_consumer_name_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = build(monkeypatch, CONSUMER_NAME="processor-1")

    assert settings.resolved_consumer_name == "processor-1"


def test_the_preparation_window_defaults_to_the_two_to_five_seconds_of_the_ticket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build(monkeypatch)

    assert (settings.prep_min_seconds, settings.prep_max_seconds) == (2.0, 5.0)


def test_an_inverted_preparation_window_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="PREP_MAX_SECONDS"):
        build(monkeypatch, PREP_MIN_SECONDS="5", PREP_MAX_SECONDS="2")
