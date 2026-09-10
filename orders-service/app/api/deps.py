from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID

from fastapi import Depends, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.headers import IDEMPOTENCY_KEY_HEADER
from app.api.metrics import MetricsCollector
from app.api.schemas.orders import render_order
from app.domain.create_order import CreateOrderService
from app.domain.health import HealthService
from app.domain.idempotency import validate_idempotency_key
from app.infra.config import Settings, get_settings
from app.infra.database import Database
from app.infra.streams import RedisBroker
from app.infra.tracing import ensure_trace_id
from app.repositories.idempotency import IdempotencyRepository
from app.repositories.orders import OrderRepository
from app.repositories.outbox import OutboxRepository


def get_database(request: Request) -> Database:
    database = getattr(request.app.state, "database", None)
    if not isinstance(database, Database):
        raise RuntimeError("La base de datos no esta inicializada en el ciclo de vida de la app")
    return database


def get_broker(request: Request) -> RedisBroker:
    broker = getattr(request.app.state, "broker", None)
    if not isinstance(broker, RedisBroker):
        raise RuntimeError("Redis no esta inicializado en el ciclo de vida de la app")
    return broker


async def get_session(
    database: Annotated[Database, Depends(get_database)],
) -> AsyncIterator[AsyncSession]:
    async with database.session() as session:
        yield session


def get_trace_id(request: Request) -> UUID:
    trace_id = request.scope.get("state", {}).get("trace_id")
    return UUID(ensure_trace_id(trace_id if isinstance(trace_id, str) else None))


def get_idempotency_key(
    idempotency_key: Annotated[str | None, Header(alias=IDEMPOTENCY_KEY_HEADER)] = None,
) -> str:
    return validate_idempotency_key(idempotency_key)


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


def get_order_repository(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> OrderRepository:
    return OrderRepository(session)


def get_create_order_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    orders: Annotated[OrderRepository, Depends(get_order_repository)],
) -> CreateOrderService:
    return CreateOrderService(
        orders=orders,
        outbox=OutboxRepository(session),
        idempotency=IdempotencyRepository(session),
        unit_of_work=session,
        render=render_order,
    )


SettingsDep = Annotated[Settings, Depends(get_settings)]
DatabaseDep = Annotated[Database, Depends(get_database)]
BrokerDep = Annotated[RedisBroker, Depends(get_broker)]
HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
MetricsCollectorDep = Annotated[MetricsCollector, Depends(get_metrics_collector)]
SessionDep = Annotated[AsyncSession, Depends(get_session)]
TraceIdDep = Annotated[UUID, Depends(get_trace_id)]
IdempotencyKeyDep = Annotated[str, Depends(get_idempotency_key)]
OrderRepositoryDep = Annotated[OrderRepository, Depends(get_order_repository)]
CreateOrderServiceDep = Annotated[CreateOrderService, Depends(get_create_order_service)]
