from typing import Annotated

from fastapi import Depends, Request
from motor.motor_asyncio import AsyncIOMotorCollection

from app.infra.config import Settings, get_settings
from app.infra.mongo import Document, MongoDatabase
from app.repositories.notifications import NotificationRepository


def get_mongo(request: Request) -> MongoDatabase:
    mongo = getattr(request.app.state, "mongo", None)
    if not isinstance(mongo, MongoDatabase):
        raise RuntimeError("Mongo no esta inicializado en el ciclo de vida de la app")
    return mongo


def get_notifications_collection(
    mongo: Annotated[MongoDatabase, Depends(get_mongo)],
) -> AsyncIOMotorCollection[Document]:
    return mongo.notifications


def get_notification_repository(
    collection: Annotated[AsyncIOMotorCollection[Document], Depends(get_notifications_collection)],
) -> NotificationRepository:
    return NotificationRepository(collection)


SettingsDep = Annotated[Settings, Depends(get_settings)]
MongoDep = Annotated[MongoDatabase, Depends(get_mongo)]
NotificationRepositoryDep = Annotated[NotificationRepository, Depends(get_notification_repository)]
