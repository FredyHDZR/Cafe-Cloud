import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.errors import register_exception_handlers
from app.api.middleware import RequestLogMiddleware, TraceIdMiddleware
from app.api.routers import health, notifications
from app.infra.config import Settings, get_settings
from app.infra.logging import configure_logging, log_context
from app.infra.mongo import MongoDatabase

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = get_settings()
    mongo = MongoDatabase.from_settings(settings)
    try:
        await mongo.verify_dedup_index()
    except Exception:
        mongo.close()
        logger.exception("service_start_failed")
        raise
    app.state.mongo = mongo
    logger.info("service_started", extra=log_context(service=settings.service_name))
    try:
        yield
    finally:
        mongo.close()
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
    app.include_router(notifications.router)
    return app


app = create_app()
