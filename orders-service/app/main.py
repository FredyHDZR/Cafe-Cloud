import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.errors import register_exception_handlers
from app.api.middleware import RequestLogMiddleware, TraceIdMiddleware
from app.api.routers import health, orders
from app.infra.config import Settings, get_settings
from app.infra.database import Database
from app.infra.logging import configure_logging, log_context

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = get_settings()
    database = Database.from_settings(settings)
    app.state.database = database
    logger.info("service_started", extra=log_context(service=settings.service_name))
    try:
        yield
    finally:
        await database.dispose()
        logger.info("service_stopped", extra=log_context(service=settings.service_name))


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.service_name, settings.log_level)

    app = FastAPI(
        title=settings.service_name,
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(RequestLogMiddleware)
    app.add_middleware(TraceIdMiddleware)

    register_exception_handlers(app)
    app.include_router(health.router)
    app.include_router(orders.router)
    return app


app = create_app()
