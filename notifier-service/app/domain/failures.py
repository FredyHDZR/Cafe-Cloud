from dataclasses import dataclass

from pymongo.errors import ConnectionFailure, ExecutionTimeout, PyMongoError, WTimeoutError
from redis.exceptions import RedisError

from app.domain.errors import NonRetryableError

MAX_ERROR_LENGTH = 500


@dataclass(frozen=True, slots=True)
class Failure:
    retryable: bool
    reason: str
    message: str


def classify(error: Exception) -> Failure:
    if isinstance(error, NonRetryableError):
        return Failure(retryable=False, reason=error.reason, message=_describe(error))
    if isinstance(error, PyMongoError):
        return Failure(retryable=True, reason=_mongo_reason(error), message=_describe(error))
    if isinstance(error, RedisError):
        return Failure(retryable=True, reason="broker_error", message=_describe(error))
    if isinstance(error, OSError | TimeoutError):
        return Failure(retryable=True, reason="io_error", message=_describe(error))
    # Lo no clasificado se reintenta: agotar intentos deja el mensaje en la DLQ, y llamarlo
    # permanente por defecto lo mandaria alli en la primera sacudida de la red.
    return Failure(retryable=True, reason="unexpected_error", message=_describe(error))


def _mongo_reason(error: PyMongoError) -> str:
    if isinstance(error, ConnectionFailure):
        return "mongo_unavailable"
    if isinstance(error, ExecutionTimeout | WTimeoutError):
        return "mongo_timeout"
    return "mongo_error"


def _describe(error: Exception) -> str:
    text = f"{type(error).__name__}: {error}".replace("\n", " ")
    if len(text) <= MAX_ERROR_LENGTH:
        return text
    return f"{text[:MAX_ERROR_LENGTH]}..."
