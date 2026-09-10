import logging
from collections.abc import Awaitable, Callable, Mapping
from http import HTTPStatus
from typing import Any, cast

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.schemas.errors import ErrorDetail, ErrorResponse
from app.infra.logging import log_context
from app.infra.tracing import TRACE_ID_HEADER, get_trace_id, trace_id_scope

ExceptionHandler = Callable[[Request, Exception], Response | Awaitable[Response]]

logger = logging.getLogger(__name__)


def resolve_trace_id(request: Request) -> str | None:
    trace_id = request.scope.get("state", {}).get("trace_id")
    return trace_id if isinstance(trace_id, str) else get_trace_id()


def error_response(
    request: Request,
    *,
    status_code: int,
    code: str,
    message: str,
    details: list[dict[str, Any]] | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    trace_id = resolve_trace_id(request)
    body = ErrorResponse(
        error=ErrorDetail(code=code, message=message, trace_id=trace_id, details=details)
    )
    response_headers = dict(headers or {})
    if trace_id is not None:
        response_headers[TRACE_ID_HEADER] = trace_id
    return JSONResponse(
        status_code=status_code,
        content=body.model_dump(exclude_none=True),
        headers=response_headers or None,
    )


def _status_code_slug(status_code: int) -> str:
    try:
        return HTTPStatus(status_code).phrase.lower().replace(" ", "_")
    except ValueError:
        return "http_error"


async def _http_exception_handler(request: Request, exc: StarletteHTTPException) -> Response:
    message = exc.detail if isinstance(exc.detail, str) else _status_code_slug(exc.status_code)
    return error_response(
        request,
        status_code=exc.status_code,
        code=_status_code_slug(exc.status_code),
        message=message,
    )


async def _validation_error_handler(request: Request, exc: RequestValidationError) -> Response:
    details = [
        {"field": ".".join(str(part) for part in error["loc"]), "reason": error["msg"]}
        for error in exc.errors()
    ]
    return error_response(
        request,
        status_code=int(HTTPStatus.UNPROCESSABLE_ENTITY),
        code="validation_error",
        message="El cuerpo de la peticion no es valido",
        details=details,
    )


async def _unhandled_error_handler(request: Request, exc: Exception) -> Response:
    trace_id = resolve_trace_id(request)
    # ServerErrorMiddleware corre por fuera del middleware de trazas, que ya limpio el ContextVar.
    with trace_id_scope(trace_id):
        logger.exception("unhandled_exception", extra=log_context(path=request.url.path))
    return error_response(
        request,
        status_code=int(HTTPStatus.INTERNAL_SERVER_ERROR),
        code="internal_error",
        message="Error interno del servicio",
    )


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(
        StarletteHTTPException, cast(ExceptionHandler, _http_exception_handler)
    )
    app.add_exception_handler(
        RequestValidationError, cast(ExceptionHandler, _validation_error_handler)
    )
    app.add_exception_handler(Exception, cast(ExceptionHandler, _unhandled_error_handler))
