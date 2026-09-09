import logging
from collections.abc import Callable
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Protocol
from uuid import UUID

from app.domain.errors import IdempotencyKeyInProgressError, IdempotencyKeyReuseError
from app.domain.events import EventDraft, order_created_event
from app.domain.idempotency import (
    CREATE_ORDER_ENDPOINT,
    IdempotencyRecord,
    IdempotencyState,
    new_order_fingerprint,
)
from app.domain.order import NewOrder, Order
from app.infra.logging import log_context

logger = logging.getLogger(__name__)

OrderRenderer = Callable[[Order], dict[str, Any]]


class OrderStore(Protocol):
    async def add(self, draft: NewOrder) -> Order: ...


class OutboxStore(Protocol):
    async def append(self, event: EventDraft) -> UUID: ...


class IdempotencyStore(Protocol):
    async def reserve(self, endpoint: str, key: str, fingerprint: str) -> bool: ...

    async def find(self, endpoint: str, key: str) -> IdempotencyRecord | None: ...

    async def complete(
        self,
        endpoint: str,
        key: str,
        *,
        order_id: UUID,
        response_status: int,
        response_body: dict[str, Any],
    ) -> None: ...


class UnitOfWork(Protocol):
    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


@dataclass(frozen=True, slots=True)
class CreateOrderCommand:
    idempotency_key: str
    trace_id: UUID
    order: NewOrder


@dataclass(frozen=True, slots=True)
class CreateOrderResult:
    status_code: int
    body: dict[str, Any]
    replayed: bool


class CreateOrderService:
    def __init__(
        self,
        *,
        orders: OrderStore,
        outbox: OutboxStore,
        idempotency: IdempotencyStore,
        unit_of_work: UnitOfWork,
        render: OrderRenderer,
    ) -> None:
        self._orders = orders
        self._outbox = outbox
        self._idempotency = idempotency
        self._unit_of_work = unit_of_work
        self._render = render

    async def execute(self, command: CreateOrderCommand) -> CreateOrderResult:
        fingerprint = new_order_fingerprint(command.order)
        reserved = await self._idempotency.reserve(
            CREATE_ORDER_ENDPOINT, command.idempotency_key, fingerprint
        )
        if not reserved:
            return await self._resolve_conflict(command.idempotency_key, fingerprint)
        return await self._create(command)

    async def _create(self, command: CreateOrderCommand) -> CreateOrderResult:
        order = await self._orders.add(command.order)
        event_id = await self._outbox.append(order_created_event(order, command.trace_id))
        body = self._render(order)
        await self._idempotency.complete(
            CREATE_ORDER_ENDPOINT,
            command.idempotency_key,
            order_id=order.id,
            response_status=int(HTTPStatus.CREATED),
            response_body=body,
        )
        await self._unit_of_work.commit()
        logger.info(
            "order_created",
            extra=log_context(
                order_id=str(order.id), event_id=str(event_id), customer_id=order.customer_id
            ),
        )
        return CreateOrderResult(status_code=int(HTTPStatus.CREATED), body=body, replayed=False)

    async def _resolve_conflict(self, key: str, fingerprint: str) -> CreateOrderResult:
        record = await self._idempotency.find(CREATE_ORDER_ENDPOINT, key)
        await self._unit_of_work.rollback()

        if record is None:
            # ON CONFLICT DO NOTHING si espera a la fila ajena sin confirmar, asi que chocar y no
            # verla solo pasa si la purga de claves caducadas la borro en medio (ADR-003).
            raise IdempotencyKeyInProgressError(
                "Hay otra peticion en curso con la misma Idempotency-Key"
            )
        if record.request_hash != fingerprint:
            raise IdempotencyKeyReuseError(
                "La Idempotency-Key ya se uso con un cuerpo de peticion distinto"
            )
        if (
            record.state is IdempotencyState.IN_PROGRESS
            or record.response_status is None
            or record.response_body is None
        ):
            raise IdempotencyKeyInProgressError(
                "Hay otra peticion en curso con la misma Idempotency-Key"
            )

        logger.info(
            "order_response_replayed",
            extra=log_context(order_id=str(record.order_id), idempotency_key=key),
        )
        return CreateOrderResult(
            status_code=record.response_status, body=record.response_body, replayed=True
        )
