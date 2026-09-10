import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from uuid import UUID

from app.domain.errors import IdempotencyKeyInvalidError, IdempotencyKeyRequiredError
from app.domain.order import NewOrder

CREATE_ORDER_ENDPOINT = "POST /orders"
MAX_IDEMPOTENCY_KEY_LENGTH = 255


class IdempotencyState(StrEnum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


@dataclass(frozen=True, slots=True)
class IdempotencyRecord:
    request_hash: str
    state: IdempotencyState
    order_id: UUID | None
    response_status: int | None
    response_body: dict[str, Any] | None


def canonical_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def request_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def new_order_fingerprint(order: NewOrder) -> str:
    return request_hash(
        {
            "customer_id": order.customer_id,
            "items": [{"name": item.name, "qty": int(item.qty)} for item in order.items],
        }
    )


def validate_idempotency_key(raw: str | None) -> str:
    if raw is None:
        raise IdempotencyKeyRequiredError(
            "La cabecera Idempotency-Key es obligatoria en POST /orders"
        )
    key = raw.strip()
    if not key:
        raise IdempotencyKeyInvalidError("La cabecera Idempotency-Key no puede estar vacia")
    if len(key) > MAX_IDEMPOTENCY_KEY_LENGTH:
        raise IdempotencyKeyInvalidError(
            f"La cabecera Idempotency-Key admite hasta {MAX_IDEMPOTENCY_KEY_LENGTH} caracteres"
        )
    if any(character.isspace() or not character.isprintable() for character in key):
        raise IdempotencyKeyInvalidError(
            "La cabecera Idempotency-Key solo admite caracteres imprimibles sin espacios"
        )
    return key
