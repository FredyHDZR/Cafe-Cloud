from collections.abc import Mapping
from http import HTTPStatus
from types import MappingProxyType
from typing import ClassVar


class DomainError(Exception):
    code: ClassVar[str] = "domain_error"
    status: ClassVar[HTTPStatus] = HTTPStatus.BAD_REQUEST
    headers: ClassVar[Mapping[str, str]] = MappingProxyType({})

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class ValidationError(DomainError):
    code = "validation_error"
    status = HTTPStatus.UNPROCESSABLE_ENTITY


class NotFoundError(DomainError):
    code = "not_found"
    status = HTTPStatus.NOT_FOUND


class ConflictError(DomainError):
    code = "conflict"
    status = HTTPStatus.CONFLICT


class OrderNotFoundError(NotFoundError):
    code = "order_not_found"


class IdempotencyKeyRequiredError(DomainError):
    code = "idempotency_key_required"


class IdempotencyKeyInvalidError(DomainError):
    code = "idempotency_key_invalid"


class IdempotencyKeyReuseError(ConflictError):
    code = "idempotency_key_reuse"


class IdempotencyKeyInProgressError(ConflictError):
    code = "idempotency_key_in_progress"
    headers = MappingProxyType({"Retry-After": "1"})
