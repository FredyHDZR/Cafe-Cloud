from dataclasses import dataclass

from redis.exceptions import RedisError
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError, SQLAlchemyError

from app.domain.errors import NonRetryableError

MAX_ERROR_LENGTH = 500

SQLSTATE_REASONS = {
    "55P03": "database_lock_timeout",
    "57014": "database_statement_timeout",
    "25P03": "database_idle_in_transaction_timeout",
    "40001": "database_serialization_failure",
    "40P01": "database_deadlock",
}


@dataclass(frozen=True, slots=True)
class Failure:
    retryable: bool
    reason: str
    message: str


def classify(error: Exception) -> Failure:
    if isinstance(error, NonRetryableError):
        return Failure(retryable=False, reason=error.reason, message=_describe(error))
    if isinstance(error, DBAPIError):
        return Failure(retryable=True, reason=_database_reason(error), message=_describe(error))
    if isinstance(error, SQLAlchemyError):
        return Failure(retryable=True, reason="database_error", message=_describe(error))
    if isinstance(error, RedisError):
        return Failure(retryable=True, reason="broker_error", message=_describe(error))
    if isinstance(error, OSError | TimeoutError):
        return Failure(retryable=True, reason="io_error", message=_describe(error))
    # Lo no clasificado se reintenta: agotar intentos deja el mensaje en la DLQ, y llamarlo
    # permanente por defecto lo mandaria alli en la primera sacudida de la red.
    return Failure(retryable=True, reason="unexpected_error", message=_describe(error))


def _database_reason(error: DBAPIError) -> str:
    sqlstate = getattr(error.orig, "sqlstate", None)
    if isinstance(sqlstate, str) and sqlstate in SQLSTATE_REASONS:
        return SQLSTATE_REASONS[sqlstate]
    if isinstance(error, OperationalError | InterfaceError):
        return "database_unavailable"
    return "database_error"


def _describe(error: Exception) -> str:
    text = f"{type(error).__name__}: {error}".replace("\n", " ")
    if len(text) <= MAX_ERROR_LENGTH:
        return text
    return f"{text[:MAX_ERROR_LENGTH]}..."
