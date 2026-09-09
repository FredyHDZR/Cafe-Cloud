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


class NonRetryableError(Exception):
    reason: ClassVar[str] = "non_retryable"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class InvalidEnvelopeError(NonRetryableError):
    reason = "invalid_envelope"


class UnexpectedEventTypeError(NonRetryableError):
    reason = "unexpected_event_type"


class UnsupportedEventVersionError(NonRetryableError):
    reason = "unsupported_event_version"
