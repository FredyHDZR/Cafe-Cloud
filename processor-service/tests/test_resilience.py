import json

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy.exc import OperationalError, ProgrammingError

from app.domain.dead_letter import DeadLetter, dlq_stream_for
from app.domain.errors import (
    InvalidEnvelopeError,
    OrderNotCompletedError,
    UnsupportedEventVersionError,
)
from app.domain.failures import MAX_ERROR_LENGTH, classify
from app.domain.retry import RetryPolicy

POLICY = RetryPolicy(
    max_attempts=3, base_seconds=1.0, max_seconds=30.0, jitter_min=0.8, jitter_max=1.2
)


class FakePostgresError(Exception):
    def __init__(self, sqlstate: str) -> None:
        super().__init__(f"error {sqlstate}")
        self.sqlstate = sqlstate


def dbapi_error(sqlstate: str) -> OperationalError:
    return OperationalError("UPDATE orders", None, FakePostgresError(sqlstate))


@pytest.mark.parametrize(
    ("attempt", "expected"), [(1, 1.0), (2, 2.0), (3, 4.0), (6, 30.0), (10, 30.0)]
)
def test_the_delay_doubles_per_attempt_and_stays_under_the_cap(
    attempt: int, expected: float
) -> None:
    delays = [POLICY.delay_for(attempt) for _ in range(200)]

    assert min(delays) >= expected * 0.8
    assert max(delays) <= expected * 1.2


def test_the_jitter_actually_varies_the_delay() -> None:
    delays = {POLICY.delay_for(1) for _ in range(50)}

    assert len(delays) > 1


def test_the_last_attempt_is_the_one_configured() -> None:
    assert POLICY.is_last(2) is False
    assert POLICY.is_last(3) is True


def test_an_envelope_error_is_permanent_and_keeps_its_reason() -> None:
    failure = classify(InvalidEnvelopeError("faltan campos"))

    assert failure.retryable is False
    assert failure.reason == "invalid_envelope"
    assert "faltan campos" in failure.message


def test_an_unsupported_version_is_permanent() -> None:
    assert classify(UnsupportedEventVersionError("version 99")).reason == (
        "unsupported_event_version"
    )


def test_an_order_that_did_not_complete_is_permanent() -> None:
    failure = classify(OrderNotCompletedError("no quedo COMPLETED"))

    assert (failure.retryable, failure.reason) == (False, "order_not_completed")


def test_a_lock_timeout_is_retryable() -> None:
    failure = classify(dbapi_error("55P03"))

    assert failure.retryable is True
    assert failure.reason == "database_lock_timeout"


@pytest.mark.parametrize(
    ("sqlstate", "reason"),
    [
        ("57014", "database_statement_timeout"),
        ("25P03", "database_idle_in_transaction_timeout"),
        ("40001", "database_serialization_failure"),
        ("40P01", "database_deadlock"),
    ],
)
def test_the_timeouts_of_the_processor_role_are_retryable(sqlstate: str, reason: str) -> None:
    failure = classify(dbapi_error(sqlstate))

    assert (failure.retryable, failure.reason) == (True, reason)


def test_an_unreachable_database_is_retryable() -> None:
    failure = classify(OperationalError("SELECT 1", None, OSError("connection refused")))

    assert (failure.retryable, failure.reason) == (True, "database_unavailable")


def test_another_database_error_is_retryable_without_a_specific_reason() -> None:
    failure = classify(ProgrammingError("SELECT 1", None, FakePostgresError("42501")))

    assert (failure.retryable, failure.reason) == (True, "database_error")


def test_a_broker_error_is_retryable() -> None:
    failure = classify(RedisConnectionError("connection reset"))

    assert (failure.retryable, failure.reason) == (True, "broker_error")


def test_an_unclassified_error_is_retryable() -> None:
    failure = classify(ValueError("vaya"))

    assert (failure.retryable, failure.reason) == (True, "unexpected_error")
    assert failure.message == "ValueError: vaya"


def test_a_huge_error_is_truncated_before_reaching_the_dlq() -> None:
    failure = classify(ValueError("x" * 5000))

    assert len(failure.message) == MAX_ERROR_LENGTH + 3


def test_the_dlq_of_a_stream_is_its_name_plus_the_suffix() -> None:
    assert dlq_stream_for("orders.created") == "orders.created.dlq"
    assert dlq_stream_for("orders.completed") == "orders.completed.dlq"


def test_the_dead_letter_carries_the_context_and_the_whole_envelope() -> None:
    envelope = {
        "event_id": "8f14e45f-ea0d-4b6c-9a1e-2c3b5d7f9012",
        "event_type": "orders.created",
        "event_version": "1",
        "occurred_at": "2026-09-09T20:00:00.000Z",
        "trace_id": "1f2a3b4c-5d6e-4f70-8123-9a0b1c2d3e4f",
        "payload": '{"order_id":"7c9e6679-7425-40de-944b-e07fc1f90ae7"}',
    }
    fields = DeadLetter(
        original_stream="orders.created",
        original_id="1757448000000-0",
        consumer_group="processor",
        delivery_count=6,
        first_failed_at="2026-09-09T20:00:01.000Z",
        last_error="OperationalError: connection refused",
        reason="database_unavailable",
        envelope=envelope,
    ).to_fields()

    assert fields["original_stream"] == "orders.created"
    assert fields["original_id"] == "1757448000000-0"
    assert fields["consumer_group"] == "processor"
    assert fields["delivery_count"] == "6"
    assert fields["first_failed_at"] == "2026-09-09T20:00:01.000Z"
    assert fields["last_error"] == "OperationalError: connection refused"
    assert fields["reason"] == "database_unavailable"
    assert fields["dead_lettered_at"].endswith("Z")
    assert json.loads(fields["envelope"]) == envelope


def test_every_value_of_the_dead_letter_is_a_string_that_redis_accepts() -> None:
    fields = DeadLetter(
        original_stream="orders.created",
        original_id="1-0",
        consumer_group="processor",
        delivery_count=1,
        first_failed_at="2026-09-09T20:00:01.000Z",
        last_error="boom",
        reason="invalid_envelope",
        envelope={},
    ).to_fields()

    assert all(isinstance(value, str) for value in fields.values())


def test_an_envelope_with_colliding_names_does_not_overwrite_the_dlq_context() -> None:
    envelope = {
        "event_id": "8f14e45f-ea0d-4b6c-9a1e-2c3b5d7f9012",
        "event_type": "orders.created",
        "event_version": "1",
        "occurred_at": "2026-09-09T20:00:00.000Z",
        "trace_id": "1f2a3b4c-5d6e-4f70-8123-9a0b1c2d3e4f",
        "payload": '{"order_id":"7c9e6679-7425-40de-944b-e07fc1f90ae7"}',
        "reason": "PWNED",
        "delivery_count": "999",
        "original_id": "PWNED",
        "consumer_group": "PWNED",
        "last_error": "PWNED",
        "envelope": "PWNED",
    }
    fields = DeadLetter(
        original_stream="orders.created",
        original_id="1757448000000-0",
        consumer_group="processor",
        delivery_count=3,
        first_failed_at="2026-09-09T20:00:01.000Z",
        last_error="OperationalError: connection refused",
        reason="database_unavailable",
        envelope=envelope,
    ).to_fields()

    assert fields["reason"] == "database_unavailable"
    assert fields["delivery_count"] == "3"
    assert fields["original_id"] == "1757448000000-0"
    assert fields["consumer_group"] == "processor"
    assert fields["last_error"] == "OperationalError: connection refused"
    assert json.loads(fields["envelope"]) == envelope
