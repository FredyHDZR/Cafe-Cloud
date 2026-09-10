import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.errors import register_exception_handlers
from app.api.metrics import MetricsCollector
from app.api.middleware import RequestLogMiddleware, TraceIdMiddleware
from app.api.routers import health, metrics, orders
from app.domain.health import Dependency, HealthService
from app.infra.config import Settings, get_settings
from app.infra.database import Database
from app.infra.logging import configure_logging, log_context
from app.infra.metrics import set_service_info
from app.infra.streams import RedisBroker

logger = logging.getLogger(__name__)

SERVICE_VERSION = "0.1.0"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = get_settings()
    database = Database.from_settings(settings)
    # Redis solo se usa aqui para la sonda de salud: quien publica es app.publisher.
    broker = RedisBroker.from_settings(settings)
    app.state.database = database
    app.state.broker = broker
    app.state.health = health_service(settings, database, broker)
    app.state.metrics = MetricsCollector(database=database)
    logger.info("service_started", extra=log_context(service=settings.service_name))
    try:
        yield
    finally:
        await broker.close()
        await database.dispose()
        logger.info("service_stopped", extra=log_context(service=settings.service_name))


def health_service(settings: Settings, database: Database, broker: RedisBroker) -> HealthService:
    return HealthService(
        [
            Dependency(name="postgres", probe=database.ping),
            # Redis no es critica aqui: con el broker caido POST /orders sigue devolviendo 201
            # y el evento espera en el outbox (ADR-002).
            Dependency(name="redis", probe=broker.ping, critical=False),
        ],
        timeout_seconds=settings.health_probe_timeout_seconds,
    )


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.service_name, settings.log_level)

    set_service_info(settings.service_name, SERVICE_VERSION)

    app = FastAPI(
        title=settings.service_name,
        version=SERVICE_VERSION,
        lifespan=lifespan,
    )

    app.add_middleware(RequestLogMiddleware)
    app.add_middleware(TraceIdMiddleware)

    register_exception_handlers(app)
    app.include_router(health.router)
    app.include_router(metrics.router)
    app.include_router(orders.router)
    return app


app = create_app()
