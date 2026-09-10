from typing import Annotated

from fastapi import Depends, Request
from motor.motor_asyncio import AsyncIOMotorCollection

from app.api.metrics import MetricsCollector
from app.domain.health import HealthService
from app.infra.config import Settings, get_settings
from app.infra.mongo import Document, MongoDatabase
from app.infra.streams import RedisBroker
from app.repositories.notifications import NotificationRepository


def get_mongo(request: Request) -> MongoDatabase:
    mongo = getattr(request.app.state, "mongo", None)
    if not isinstance(mongo, MongoDatabase):
        raise RuntimeError("Mongo no esta inicializado en el ciclo de vida de la app")
    return mongo


def get_broker(request: Request) -> RedisBroker:
    broker = getattr(request.app.state, "broker", None)
    if not isinstance(broker, RedisBroker):
        raise RuntimeError("Redis no esta inicializado en el ciclo de vida de la app")
    return broker


def get_notifications_collection(
    mongo: Annotated[MongoDatabase, Depends(get_mongo)],
) -> AsyncIOMotorCollection[Document]:
    return mongo.notifications


def get_notification_repository(
    collection: Annotated[AsyncIOMotorCollection[Document], Depends(get_notifications_collection)],
) -> NotificationRepository:
    return NotificationRepository(collection)


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


SettingsDep = Annotated[Settings, Depends(get_settings)]
MongoDep = Annotated[MongoDatabase, Depends(get_mongo)]
NotificationRepositoryDep = Annotated[NotificationRepository, Depends(get_notification_repository)]
BrokerDep = Annotated[RedisBroker, Depends(get_broker)]
HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
MetricsCollectorDep = Annotated[MetricsCollector, Depends(get_metrics_collector)]
