import logging
from collections.abc import MutableMapping
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

from app.infra.config import Settings
from app.infra.logging import log_context

logger = logging.getLogger(__name__)

DEDUP_FIELD = "event_id"
DEDUP_INDEX_KEY = [(DEDUP_FIELD, 1)]
# Los 30 s por defecto de motor dejarian a cada intento del consumidor esperando media vida.
DEFAULT_SERVER_SELECTION_TIMEOUT_MS = 5000

Document = dict[str, Any]


class MissingDedupIndexError(RuntimeError):
    def __init__(self, namespace: str) -> None:
        message = (
            f"{namespace} no tiene indice unico sobre {DEDUP_FIELD}: sin el, la entrega "
            "at-least-once duplicaria notificaciones. Lo crea el init de Mongo (TICKET-001), "
            "no este servicio; si falta, recrear el volumen con docker compose down -v."
        )
        super().__init__(message)
        self.message = message


class MongoDatabase:
    def __init__(
        self,
        dsn: str,
        *,
        database: str,
        collection: str,
        server_selection_timeout_ms: int = DEFAULT_SERVER_SELECTION_TIMEOUT_MS,
    ) -> None:
        self._client: AsyncIOMotorClient[Document] = AsyncIOMotorClient(
            dsn,
            tz_aware=True,
            serverSelectionTimeoutMS=server_selection_timeout_ms,
        )
        self._database: AsyncIOMotorDatabase[Document] = self._client[database]
        self._collection_name = collection

    @classmethod
    def from_settings(cls, settings: Settings) -> "MongoDatabase":
        return cls(
            settings.mongo_dsn,
            database=settings.mongo_database,
            collection=settings.mongo_collection,
            server_selection_timeout_ms=settings.mongo_server_selection_timeout_ms,
        )

    @property
    def notifications(self) -> AsyncIOMotorCollection[Document]:
        return self._database[self._collection_name]

    @property
    def namespace(self) -> str:
        return f"{self._database.name}.{self._collection_name}"

    async def ping(self) -> None:
        await self._database.command("ping")

    async def verify_dedup_index(self) -> str:
        indexes: MutableMapping[str, Any] = await self.notifications.index_information()
        for name, definition in indexes.items():
            key = [(field, direction) for field, direction in definition.get("key", [])]
            # Un unique parcial deja duplicar todo lo que su filtro excluye: no deduplica.
            partial = definition.get("partialFilterExpression") is not None
            if key == DEDUP_INDEX_KEY and definition.get("unique", False) and not partial:
                logger.info(
                    "dedup_index_verified",
                    extra=log_context(namespace=self.namespace, index=name),
                )
                return name
        raise MissingDedupIndexError(self.namespace)

    def close(self) -> None:
        self._client.close()
