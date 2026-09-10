import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.errors import register_exception_handlers
from app.api.metrics import ApiMetricsCollector
from app.api.middleware import RequestLogMiddleware, TraceIdMiddleware
from app.api.routers import health, metrics, notifications
from app.domain.health import Dependency, HealthService
from app.infra.config import Settings, get_settings
from app.infra.logging import configure_logging, log_context
from app.infra.metrics import register_api_metrics, set_service_info
from app.infra.mongo import MongoDatabase
from app.infra.streams import RedisBroker
from app.repositories.notifications import NotificationRepository

logger = logging.getLogger(__name__)

SERVICE_VERSION = "0.1.0"


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
    # Redis solo se usa aqui para la sonda de salud: quien lee el stream es app.consumer.
    broker = RedisBroker.from_settings(settings)
    app.state.mongo = mongo
    app.state.broker = broker
    app.state.health = health_service(settings, mongo, broker)
    app.state.metrics = ApiMetricsCollector(
        notifications=NotificationRepository(mongo.notifications)
    )
    logger.info("service_started", extra=log_context(service=settings.service_name))
    try:
        yield
    finally:
        await broker.close()
        mongo.close()
        logger.info("service_stopped", extra=log_context(service=settings.service_name))


def health_service(settings: Settings, mongo: MongoDatabase, broker: RedisBroker) -> HealthService:
    return HealthService(
        [
            Dependency(name="mongo", probe=mongo.ping),
            # Redis no es critica para esta API: quien lee el stream es notifier-consumer, y es
            # su sonda la que lo declara critico (TICKET-009, decision 3).
            Dependency(name="redis", probe=broker.ping, critical=False),
        ],
        timeout_seconds=settings.health_probe_timeout_seconds,
    )


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.service_name, settings.log_level)

    set_service_info(settings.service_name, SERVICE_VERSION)
    register_api_metrics()

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
    app.include_router(notifications.router)
    return app


app = create_app()
