import logging
from time import perf_counter
from typing import Any

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.infra.logging import log_context
from app.infra.metrics import HTTP_DURATION, HTTP_REQUESTS
from app.infra.tracing import TRACE_ID_HEADER, bind_trace_id, ensure_trace_id, reset_trace_id

logger = logging.getLogger("app.request")

UNMATCHED_PATH = "unmatched"


class TraceIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        trace_id = ensure_trace_id(Headers(scope=scope).get(TRACE_ID_HEADER))
        state: dict[str, Any] = scope.setdefault("state", {})
        # El scope sobrevive al reset del ContextVar, y de ahi lo lee el manejador de errores 500.
        state["trace_id"] = trace_id
        token = bind_trace_id(trace_id)

        async def send_with_trace_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)[TRACE_ID_HEADER] = trace_id
            await send(message)

        try:
            await self._app(scope, receive, send_with_trace_id)
        finally:
            reset_trace_id(token)


class RequestLogMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        status_code = 500
        started_at = perf_counter()

        async def send_with_status(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
            await send(message)

        try:
            await self._app(scope, receive, send_with_status)
        finally:
            elapsed = perf_counter() - started_at
            method = str(scope.get("method", ""))
            # La etiqueta es la plantilla de la ruta, no la URL: /orders/{order_id} tiene una
            # serie, y /orders/<uuid> tendria una por pedido.
            template = getattr(scope.get("route"), "path", UNMATCHED_PATH)
            HTTP_REQUESTS.labels(method=method, path=template, status=str(status_code)).inc()
            HTTP_DURATION.labels(method=method, path=template).observe(elapsed)
            logger.info(
                "http_request",
                extra=log_context(
                    method=scope.get("method"),
                    path=scope.get("path"),
                    status_code=status_code,
                    duration_ms=round(elapsed * 1000, 2),
                ),
            )
