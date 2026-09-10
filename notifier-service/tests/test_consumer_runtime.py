import asyncio
import json
from collections.abc import Mapping, Sequence
from typing import Any, cast

import pytest
from pymongo.errors import ServerSelectionTimeoutError
from redis.exceptions import ConnectionError as RedisConnectionError

from app.consumer import NotificationConsumer, StreamJanitor
from app.domain.envelope import OrderCompletedEnvelope
from app.domain.notification import notification_from_event
from app.domain.notify import NotifyOutcome
from app.infra.config import ConsumerSettings
from app.infra.streams import RedisStreamConsumer, StreamMessage
from tests.conftest import TEST_ENVIRONMENT

VALID_FIELDS = {
    "event_id": "b3d4c5e6-1a2b-4c3d-9e8f-0a1b2c3d4e5f",
    "event_type": "orders.completed",
    "event_version": "1",
    "occurred_at": "2026-09-09T20:00:03.412Z",
    "trace_id": "1f2a3b4c-5d6e-4f70-8123-9a0b1c2d3e4f",
    "payload": json.dumps(
        {
            "order_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
            "customer_id": "abc123",
            "status": "COMPLETED",
            "items": [{"name": "latte", "qty": 1}],
            "created_at": "2026-09-09T20:00:00.000Z",
            "completed_at": "2026-09-09T20:00:03.412Z",
            "processing_ms": 3412,
        }
    ),
}

FAST_BACKOFF = {
    "CONSUMER_BACKOFF_BASE_SECONDS": "0.001",
    "CONSUMER_BACKOFF_MAX_SECONDS": "0.002",
}


class FakeStream:
    def __init__(self) -> None:
        self.stream = "orders.completed"
        self.group = "notifier"
        self.consumer = "notifier-test"
        self.dlq_stream = "orders.completed.dlq"
        self.calls: list[str] = []
        self.dead_letters: list[dict[str, str]] = []
        self.acked: list[str] = []
        self.claimed: list[StreamMessage] = []
        self.deliveries: dict[str, int] = {}
        self.ack_error: Exception | None = None
        self.dlq_error: Exception | None = None

    async def autoclaim(self, *, min_idle_ms: int, count: int) -> list[StreamMessage]:
        self.calls.append("autoclaim")
        claimed, self.claimed = self.claimed, []
        return claimed

    async def delivery_counts(self, entry_ids: Sequence[str]) -> dict[str, int]:
        return {entry_id: self.deliveries.get(entry_id, 1) for entry_id in entry_ids}

    async def dead_letter(self, fields: Mapping[str, str]) -> str:
        if self.dlq_error is not None:
            raise self.dlq_error
        self.calls.append("dead_letter")
        self.dead_letters.append(dict(fields))
        return f"dlq-{len(self.dead_letters)}"

    async def ack(self, entry_id: str) -> None:
        if self.ack_error is not None:
            raise self.ack_error
        self.calls.append("ack")
        self.acked.append(entry_id)


class RecordingConsumer(NotificationConsumer):
    def __init__(self, *, settings: ConsumerSettings, stream: FakeStream) -> None:
        super().__init__(settings=settings, mongo=None, stream=stream)  # type: ignore[arg-type]
        self.outcomes: list[Exception | NotifyOutcome] = []
        self.notified = 0

    async def _notify(self, event: OrderCompletedEnvelope) -> NotifyOutcome:
        self.notified += 1
        result = self.outcomes.pop(0) if self.outcomes else self._written(event)
        if isinstance(result, Exception):
            raise result
        return result

    @staticmethod
    def _written(event: OrderCompletedEnvelope) -> NotifyOutcome:
        return NotifyOutcome(notification=notification_from_event(event), duplicate=False)


def settings_for(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> ConsumerSettings:
    for name, value in {**TEST_ENVIRONMENT, **FAST_BACKOFF, **overrides}.items():
        monkeypatch.setenv(name, value)
    return ConsumerSettings()


def message(fields: Mapping[str, str] | None = None, entry_id: str = "1-0") -> StreamMessage:
    return StreamMessage(entry_id=entry_id, fields=dict(VALID_FIELDS if fields is None else fields))


def build(
    monkeypatch: pytest.MonkeyPatch, **overrides: str
) -> tuple[RecordingConsumer, FakeStream, ConsumerSettings]:
    stream = FakeStream()
    settings = settings_for(monkeypatch, **overrides)
    return RecordingConsumer(settings=settings, stream=stream), stream, settings


def run(coroutine: Any) -> Any:
    return asyncio.run(coroutine)


def janitor_for(
    settings: ConsumerSettings, stream: FakeStream, consumer: RecordingConsumer
) -> StreamJanitor:
    return StreamJanitor(
        settings=settings, stream=cast(RedisStreamConsumer, stream), consumer=consumer
    )


def test_a_healthy_event_is_written_once_and_acked(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer, stream, _settings = build(monkeypatch)

    run(consumer.handle(message(), stop=asyncio.Event()))

    assert consumer.notified == 1
    assert stream.acked == ["1-0"]
    assert stream.dead_letters == []


def test_a_broken_envelope_goes_to_the_dlq_without_a_single_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, stream, _settings = build(monkeypatch)

    run(consumer.handle(message({"event_type": "orders.completed"}), stop=asyncio.Event()))

    assert consumer.notified == 0
    assert stream.dead_letters[0]["reason"] == "invalid_envelope"
    assert stream.dead_letters[0]["original_stream"] == "orders.completed"
    assert stream.dead_letters[0]["consumer_group"] == "notifier"


def test_the_dlq_is_written_before_the_ack(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer, stream, _settings = build(monkeypatch)

    run(consumer.handle(message({"event_type": "orders.completed"}), stop=asyncio.Event()))

    assert stream.calls == ["dead_letter", "ack"]


def test_a_message_is_not_acked_if_the_dlq_write_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer, stream, _settings = build(monkeypatch)
    stream.dlq_error = RedisConnectionError("connection reset")

    run(consumer.handle(message({"event_type": "orders.completed"}), stop=asyncio.Event()))

    assert stream.acked == []


def test_an_unsupported_version_goes_to_the_dlq_with_its_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, stream, _settings = build(monkeypatch)

    run(consumer.handle(message({**VALID_FIELDS, "event_version": "99"}), stop=asyncio.Event()))

    assert stream.dead_letters[0]["reason"] == "unsupported_event_version"
    assert stream.acked == ["1-0"]


def test_an_event_of_another_stream_goes_to_the_dlq(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer, stream, _settings = build(monkeypatch)

    run(
        consumer.handle(
            message({**VALID_FIELDS, "event_type": "orders.created"}), stop=asyncio.Event()
        )
    )

    assert stream.dead_letters[0]["reason"] == "unexpected_event_type"


def test_the_whole_envelope_travels_to_the_dlq(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer, stream, _settings = build(monkeypatch)
    broken = {**VALID_FIELDS, "event_version": "99"}

    run(consumer.handle(message(broken), stop=asyncio.Event()))

    assert json.loads(stream.dead_letters[0]["envelope"]) == broken


def test_a_transient_failure_is_retried_and_ends_well(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer, stream, _settings = build(monkeypatch)
    consumer.outcomes = [
        ServerSelectionTimeoutError("no servers"),
        ServerSelectionTimeoutError("no servers"),
    ]

    run(consumer.handle(message(), stop=asyncio.Event()))

    assert consumer.notified == 3
    assert stream.acked == ["1-0"]
    assert stream.dead_letters == []


def test_an_exhausted_transient_failure_ends_in_the_dlq(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer, stream, _settings = build(monkeypatch)
    consumer.outcomes = [ServerSelectionTimeoutError("no servers") for _ in range(3)]

    run(consumer.handle(message(), stop=asyncio.Event()))

    assert consumer.notified == 3
    assert stream.dead_letters[0]["reason"] == "mongo_unavailable"
    assert stream.calls[-2:] == ["dead_letter", "ack"]


def test_the_number_of_attempts_is_configurable(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer, _stream, _settings = build(monkeypatch, CONSUMER_MAX_ATTEMPTS="5")
    consumer.outcomes = [ServerSelectionTimeoutError("no servers") for _ in range(5)]

    run(consumer.handle(message(), stop=asyncio.Event()))

    assert consumer.notified == 5


def test_a_failed_ack_does_not_kill_the_consumer(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer, stream, _settings = build(monkeypatch)
    stream.ack_error = RedisConnectionError("connection reset")

    run(consumer.handle(message(), stop=asyncio.Event()))

    assert consumer.notified == 1
    assert stream.acked == []


def test_a_retry_is_abandoned_when_the_process_is_stopping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, stream, _settings = build(monkeypatch)
    consumer.outcomes = [ServerSelectionTimeoutError("no servers")]
    stop = asyncio.Event()
    stop.set()

    run(consumer.handle(message(), stop=stop))

    assert consumer.notified == 1
    assert stream.acked == []
    assert stream.dead_letters == []


def test_the_janitor_reprocesses_what_it_claims(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer, stream, settings = build(monkeypatch)
    stream.claimed = [message()]
    stream.deliveries = {"1-0": 2}
    janitor = janitor_for(settings, stream, consumer)

    run(janitor._sweep(asyncio.Event()))

    assert consumer.notified == 1
    assert stream.acked == ["1-0"]


def test_too_many_deliveries_go_straight_to_the_dlq(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer, stream, settings = build(monkeypatch)
    stream.claimed = [message()]
    stream.deliveries = {"1-0": 6}
    janitor = janitor_for(settings, stream, consumer)

    run(janitor._sweep(asyncio.Event()))

    assert consumer.notified == 0
    assert stream.dead_letters[0]["reason"] == "delivery_count_exceeded"
    assert stream.dead_letters[0]["delivery_count"] == "6"
    assert stream.calls == ["autoclaim", "dead_letter", "ack"]


def test_the_janitor_does_not_steal_a_message_from_its_own_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, stream, settings = build(monkeypatch)
    stream.claimed = [message()]
    stream.deliveries = {"1-0": 9}
    janitor = janitor_for(settings, stream, consumer)

    async def sweep_while_working() -> None:
        consumer._inflight.add("1-0")
        await janitor._sweep(asyncio.Event())

    run(sweep_while_working())

    assert consumer.notified == 0
    assert stream.dead_letters == []


def test_a_dead_letter_whose_ack_fails_is_written_again_on_the_next_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, stream, _settings = build(monkeypatch)
    stream.ack_error = RedisConnectionError("connection reset")
    broken = message({"event_type": "orders.completed"})

    run(consumer.handle(broken, stop=asyncio.Event()))
    run(consumer.handle(broken, stop=asyncio.Event(), delivery_count=2))

    assert stream.acked == []
    assert [entry["delivery_count"] for entry in stream.dead_letters] == ["1", "2"]


def test_a_failure_in_the_middle_of_a_claimed_batch_does_not_stop_the_rest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, stream, settings = build(monkeypatch)
    stream.claimed = [
        message(entry_id="1-0"),
        message({**VALID_FIELDS, "event_version": "99"}, entry_id="2-0"),
        message(entry_id="3-0"),
    ]
    stream.deliveries = {"1-0": 2, "2-0": 2, "3-0": 2}
    janitor = janitor_for(settings, stream, consumer)

    run(janitor._sweep(asyncio.Event()))

    assert consumer.notified == 2
    assert stream.acked == ["1-0", "2-0", "3-0"]
    assert [entry["original_id"] for entry in stream.dead_letters] == ["2-0"]
