import hashlib
import json

import pytest

from app.api.schemas.orders import CreateOrderRequest
from app.domain.errors import IdempotencyKeyInvalidError, IdempotencyKeyRequiredError
from app.domain.idempotency import (
    MAX_IDEMPOTENCY_KEY_LENGTH,
    canonical_json,
    new_order_fingerprint,
    validate_idempotency_key,
)
from app.domain.order import NewOrder, OrderItem

CANONICAL_BODY = (
    '{"customer_id":"abc123","items":[{"name":"latte","qty":1},{"name":"muffin","qty":2}]}'
)


def _order(customer_id: str = "abc123") -> NewOrder:
    return NewOrder(
        customer_id=customer_id,
        items=(OrderItem(name="latte", qty=1), OrderItem(name="muffin", qty=2)),
    )


def test_canonical_json_sorts_keys_and_drops_spaces() -> None:
    assert canonical_json({"b": 1, "a": [1, 2]}) == '{"a":[1,2],"b":1}'


def test_fingerprint_is_the_sha256_of_the_canonical_body() -> None:
    expected = hashlib.sha256(CANONICAL_BODY.encode("utf-8")).hexdigest()

    assert new_order_fingerprint(_order()) == expected


def test_fingerprint_ignores_key_order_and_whitespace_of_the_received_body() -> None:
    received = json.dumps(
        {
            "items": [{"qty": 1, "name": "latte"}, {"name": "muffin", "qty": 2}],
            "customer_id": "abc123",
        },
        indent=4,
    )
    parsed = CreateOrderRequest.model_validate(json.loads(received))

    assert new_order_fingerprint(parsed.to_domain()) == new_order_fingerprint(_order())


def test_fingerprint_normalizes_qty_to_integer() -> None:
    parsed = CreateOrderRequest.model_validate(
        {
            "customer_id": "abc123",
            "items": [{"name": "latte", "qty": "1"}, {"name": "muffin", "qty": 2}],
        }
    )

    assert new_order_fingerprint(parsed.to_domain()) == new_order_fingerprint(_order())


def test_fingerprint_changes_with_the_body() -> None:
    assert new_order_fingerprint(_order()) != new_order_fingerprint(_order("otro"))


def test_fingerprint_changes_with_the_item_order() -> None:
    reversed_items = NewOrder(
        customer_id="abc123",
        items=(OrderItem(name="muffin", qty=2), OrderItem(name="latte", qty=1)),
    )

    assert new_order_fingerprint(_order()) != new_order_fingerprint(reversed_items)


def test_missing_idempotency_key_is_rejected() -> None:
    with pytest.raises(IdempotencyKeyRequiredError) as excinfo:
        validate_idempotency_key(None)

    assert excinfo.value.code == "idempotency_key_required"
    assert int(excinfo.value.status) == 400


@pytest.mark.parametrize(
    "raw",
    ["", "   ", "k 001", "k\t001", "x" * (MAX_IDEMPOTENCY_KEY_LENGTH + 1)],
)
def test_malformed_idempotency_keys_are_rejected(raw: str) -> None:
    with pytest.raises(IdempotencyKeyInvalidError):
        validate_idempotency_key(raw)


def test_valid_idempotency_key_is_trimmed() -> None:
    assert validate_idempotency_key(" k-001 ") == "k-001"
