import json
from datetime import UTC, datetime
from uuid import UUID

import pytest

from app.domain.envelope import parse_order_created
from app.domain.errors import (
    InvalidEnvelopeError,
    UnexpectedEventTypeError,
    UnsupportedEventVersionError,
)

EVENT_ID = "8f14e45f-ea0d-4b6c-9a1e-2c3b5d7f9012"
TRACE_ID = "1f2a3b4c-5d6e-4f70-8123-9a0b1c2d3e4f"
ORDER_ID = "7c9e6679-7425-40de-944b-e07fc1f90ae7"

PAYLOAD = {
    "order_id": ORDER_ID,
    "customer_id": "abc123",
    "status": "PENDING",
    "items": [{"name": "latte", "qty": 1}, {"name": "muffin", "qty": 2}],
    "created_at": "2026-09-08T20:00:00.000Z",
}


def fields(**overrides: str) -> dict[str, str]:
    base = {
        "event_id": EVENT_ID,
        "event_type": "orders.created",
        "event_version": "1",
        "occurred_at": "2026-09-08T20:00:00.000Z",
        "trace_id": TRACE_ID,
        "payload": json.dumps(PAYLOAD),
    }
    base.update(overrides)
    return base


def test_parses_the_six_fields_of_the_contract() -> None:
    event = parse_order_created(fields())

    assert event.event_id == UUID(EVENT_ID)
    assert event.trace_id == UUID(TRACE_ID)
    assert event.event_version == 1
    assert event.occurred_at == datetime(2026, 9, 8, 20, 0, tzinfo=UTC)
    assert event.payload.order_id == UUID(ORDER_ID)
    assert [item.name for item in event.payload.items] == ["latte", "muffin"]


def test_an_unknown_payload_field_does_not_break_the_reader() -> None:
    items = [{"name": "latte", "qty": 1, "hot": True}]
    tolerated = dict(PAYLOAD, discount_code="FREE", items=items)

    event = parse_order_created(fields(payload=json.dumps(tolerated)))

    assert event.payload.customer_id == "abc123"
    assert event.payload.items[0].qty == 1


@pytest.mark.parametrize("missing", ["event_id", "trace_id", "payload", "occurred_at"])
def test_a_missing_envelope_field_is_not_retryable(missing: str) -> None:
    incomplete = fields()
    del incomplete[missing]

    with pytest.raises(InvalidEnvelopeError):
        parse_order_created(incomplete)


def test_a_payload_that_is_not_json_is_not_retryable() -> None:
    with pytest.raises(InvalidEnvelopeError):
        parse_order_created(fields(payload="{no soy json"))


def test_an_empty_item_list_is_not_retryable() -> None:
    with pytest.raises(InvalidEnvelopeError):
        parse_order_created(fields(payload=json.dumps(dict(PAYLOAD, items=[]))))


def test_an_unsupported_version_has_its_own_reason() -> None:
    with pytest.raises(UnsupportedEventVersionError) as error:
        parse_order_created(fields(event_version="2"))

    assert error.value.reason == "unsupported_event_version"


def test_another_event_type_on_this_stream_is_rejected() -> None:
    with pytest.raises(UnexpectedEventTypeError) as error:
        parse_order_created(fields(event_type="orders.completed"))

    assert error.value.reason == "unexpected_event_type"
