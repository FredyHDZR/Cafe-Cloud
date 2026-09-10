from typing import ClassVar


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


class OrderNotCompletedError(NonRetryableError):
    reason = "order_not_completed"


class OrderAlreadyCompletedError(NonRetryableError):
    reason = "order_already_completed"
