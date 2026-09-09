import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.domain.errors import (
    InvalidEnvelopeError,
    UnexpectedEventTypeError,
    UnsupportedEventVersionError,
)

ORDER_CREATED_TYPE = "orders.created"
SUPPORTED_VERSIONS = frozenset({1})
ENVELOPE_FIELDS = ("event_id", "event_type", "event_version", "occurred_at", "trace_id", "payload")


class IncomingItem(BaseModel):
    # extra="ignore" es el lector tolerante del contrato: un campo de mas no rompe nada.
    model_config = ConfigDict(extra="ignore", frozen=True)

    name: str = Field(min_length=1, max_length=80)
    qty: int = Field(ge=1)


class OrderCreatedPayload(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    order_id: UUID
    customer_id: str = Field(min_length=1, max_length=64)
    status: str = Field(min_length=1)
    items: list[IncomingItem] = Field(min_length=1)
    created_at: datetime


class OrderCreatedEnvelope(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    event_id: UUID
    event_type: str = Field(min_length=1)
    event_version: int = Field(ge=1)
    occurred_at: datetime
    trace_id: UUID
    payload: OrderCreatedPayload


def parse_order_created(fields: Mapping[str, str]) -> OrderCreatedEnvelope:
    raw = _decode(fields)
    _require_event_type(raw)
    _require_supported_version(raw)
    try:
        return OrderCreatedEnvelope.model_validate(raw)
    except ValidationError as error:
        message = f"envelope invalido en: {_locations(error)}"
        raise InvalidEnvelopeError(message) from error


def _locations(error: ValidationError) -> str:
    return ", ".join(".".join(str(part) for part in item["loc"]) for item in error.errors())


def _decode(fields: Mapping[str, str]) -> dict[str, Any]:
    missing = [name for name in ENVELOPE_FIELDS if name not in fields]
    if missing:
        message = f"faltan campos obligatorios del envelope: {', '.join(missing)}"
        raise InvalidEnvelopeError(message)
    raw: dict[str, Any] = dict(fields)
    try:
        raw["payload"] = json.loads(fields["payload"])
    except json.JSONDecodeError as error:
        message = f"payload no es JSON valido: {error}"
        raise InvalidEnvelopeError(message) from error
    if not isinstance(raw["payload"], dict):
        message = "payload no es un objeto JSON"
        raise InvalidEnvelopeError(message)
    return raw


def _require_event_type(raw: Mapping[str, Any]) -> None:
    event_type = raw["event_type"]
    if event_type != ORDER_CREATED_TYPE:
        message = f"event_type inesperado en este stream: {event_type!r}"
        raise UnexpectedEventTypeError(message)


def _require_supported_version(raw: Mapping[str, Any]) -> None:
    try:
        version = int(raw["event_version"])
    except (TypeError, ValueError) as error:
        message = f"event_version no es un entero: {raw['event_version']!r}"
        raise InvalidEnvelopeError(message) from error
    if version not in SUPPORTED_VERSIONS:
        message = f"event_version {version} no soportada para {ORDER_CREATED_TYPE}"
        raise UnsupportedEventVersionError(message)
