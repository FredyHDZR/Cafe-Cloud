from typing import Annotated

from fastapi import Depends, Request

from app.api.metrics import MetricsCollector
from app.domain.health import HealthService
from app.infra.config import ConsumerSettings


def get_settings(request: Request) -> ConsumerSettings:
    settings = getattr(request.app.state, "settings", None)
    if not isinstance(settings, ConsumerSettings):
        raise RuntimeError("La configuracion no esta en el estado de la aplicacion")
    return settings


def get_health_service(request: Request) -> HealthService:
    health = getattr(request.app.state, "health", None)
    if not isinstance(health, HealthService):
        raise RuntimeError("El servicio de salud no esta inicializado")
    return health


def get_metrics_collector(request: Request) -> MetricsCollector:
    collector = getattr(request.app.state, "metrics", None)
    if not isinstance(collector, MetricsCollector):
        raise RuntimeError("El recolector de metricas no esta inicializado")
    return collector


SettingsDep = Annotated[ConsumerSettings, Depends(get_settings)]
HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
MetricsCollectorDep = Annotated[MetricsCollector, Depends(get_metrics_collector)]
