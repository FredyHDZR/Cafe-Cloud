import json
from typing import Any
from uuid import uuid4

import pytest

from app.domain.envelope import parse_order_completed
from app.domain.errors import (
    InvalidEnvelopeError,
    UnexpectedEventTypeError,
    UnsupportedEventVersionError,
)

ORDER_ID = "7c9e6679-7425-40de-944b-e07fc1f90ae7"


def payload(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "order_id": ORDER_ID,
        "customer_id": "abc123",
        "status": "COMPLETED",
        "items": [{"name": "latte", "qty": 1}],
        "created_at": "2026-09-09T20:00:00.000Z",
        "completed_at": "2026-09-09T20:00:03.412Z",
        "processing_ms": 3412,
    }
    body.update(overrides)
    return body


def fields(**overrides: Any) -> dict[str, str]:
    envelope: dict[str, str] = {
        "event_id": str(uuid4()),
        "event_type": "orders.completed",
        "event_version": "1",
        "occurred_at": "2026-09-09T20:00:03.412Z",
        "trace_id": str(uuid4()),
        "payload": json.dumps(payload()),
    }
    envelope.update(overrides)
    return envelope


def test_parses_a_valid_envelope() -> None:
    event = parse_order_completed(fields())

    assert event.event_type == "orders.completed"
    assert str(event.payload.order_id) == ORDER_ID
    assert event.payload.processing_ms == 3412


def test_ignores_unknown_payload_fields() -> None:
    event = parse_order_completed(fields(payload=json.dumps(payload(loyalty_points=10))))

    assert event.payload.customer_id == "abc123"


def test_processing_ms_is_optional() -> None:
    body = payload()
    del body["processing_ms"]

    event = parse_order_completed(fields(payload=json.dumps(body)))

    assert event.payload.processing_ms is None


def test_rejects_a_missing_envelope_field() -> None:
    incomplete = fields()
    del incomplete["trace_id"]

    with pytest.raises(InvalidEnvelopeError) as error:
        parse_order_completed(incomplete)

    assert error.value.reason == "invalid_envelope"


def test_rejects_a_payload_that_is_not_json() -> None:
    with pytest.raises(InvalidEnvelopeError):
        parse_order_completed(fields(payload="{"))


def test_rejects_a_payload_that_is_not_an_object() -> None:
    with pytest.raises(InvalidEnvelopeError):
        parse_order_completed(fields(payload="[]"))


def test_rejects_a_missing_payload_field() -> None:
    body = payload()
    del body["customer_id"]

    with pytest.raises(InvalidEnvelopeError):
        parse_order_completed(fields(payload=json.dumps(body)))


def test_rejects_another_event_type() -> None:
    with pytest.raises(UnexpectedEventTypeError) as error:
        parse_order_completed(fields(event_type="orders.created"))

    assert error.value.reason == "unexpected_event_type"


def test_rejects_an_unsupported_version() -> None:
    with pytest.raises(UnsupportedEventVersionError) as error:
        parse_order_completed(fields(event_version="2"))

    assert error.value.reason == "unsupported_event_version"


def test_rejects_a_version_that_is_not_an_integer() -> None:
    with pytest.raises(InvalidEnvelopeError):
        parse_order_completed(fields(event_version="uno"))
