import asyncio
import logging

import uvicorn
from fastapi import FastAPI

from app.api.errors import register_exception_handlers
from app.api.metrics import MetricsCollector
from app.api.middleware import RequestLogMiddleware, TraceIdMiddleware
from app.api.routers import health, metrics
from app.domain.health import Dependency, HealthService
from app.infra.config import ConsumerSettings
from app.infra.database import Database
from app.infra.logging import log_context
from app.infra.metrics import set_service_info
from app.infra.streams import RedisStreamConsumer

logger = logging.getLogger(__name__)

SERVICE_VERSION = "0.1.0"


def create_observability_app(
    *,
    settings: ConsumerSettings,
    database: Database,
    stream: RedisStreamConsumer,
) -> FastAPI:
    set_service_info(settings.service_name, SERVICE_VERSION)

    app = FastAPI(title=settings.service_name, version=SERVICE_VERSION)
    app.state.settings = settings
    # Las dos criticas: sin Postgres no hay transicion y sin Redis no hay de donde leer.
    app.state.health = HealthService(
        [
            Dependency(name="postgres", probe=database.ping),
            Dependency(name="redis", probe=stream.ping),
        ],
        timeout_seconds=settings.health_probe_timeout_seconds,
    )
    app.state.metrics = MetricsCollector(database=database, stream=stream)

    app.add_middleware(RequestLogMiddleware)
    app.add_middleware(TraceIdMiddleware)

    register_exception_handlers(app)
    app.include_router(health.router)
    app.include_router(metrics.router)
    return app


class EmbeddedServer(uvicorn.Server):
    def install_signal_handlers(self) -> None:
        # El consumidor ya tiene los suyos y son los que ordenan el apagado del proceso entero.
        return None


async def serve(app: FastAPI, *, settings: ConsumerSettings, stop: asyncio.Event) -> None:
    server = EmbeddedServer(
        uvicorn.Config(
            app,
            host=settings.http_host,
            port=settings.http_port,
            log_config=None,
            access_log=False,
        )
    )
    serving = asyncio.create_task(server.serve())
    logger.info(
        "observability_server_started",
        extra=log_context(host=settings.http_host, port=settings.http_port),
    )
    try:
        await stop.wait()
    finally:
        server.should_exit = True
        await serving
        logger.info("observability_server_stopped")
