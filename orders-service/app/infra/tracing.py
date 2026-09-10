from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from uuid import UUID, uuid4

TRACE_ID_HEADER = "X-Trace-Id"

_trace_id: ContextVar[str | None] = ContextVar("trace_id", default=None)


def get_trace_id() -> str | None:
    return _trace_id.get()


def bind_trace_id(trace_id: str) -> Token[str | None]:
    return _trace_id.set(trace_id)


def reset_trace_id(token: Token[str | None]) -> None:
    _trace_id.reset(token)


@contextmanager
def trace_id_scope(trace_id: str | None) -> Iterator[None]:
    if trace_id is None:
        yield
        return
    token = bind_trace_id(trace_id)
    try:
        yield
    finally:
        reset_trace_id(token)


def new_trace_id() -> str:
    return str(uuid4())


def ensure_trace_id(incoming: str | None) -> str:
    if incoming is None:
        return new_trace_id()
    try:
        return str(UUID(incoming))
    except ValueError:
        # Un trace_id ajeno mal formado se descarta en lugar de propagarse a los tres servicios.
        return new_trace_id()
