from uuid import UUID

from sqlalchemy import func, select, update

from app.domain.order import OrderStatus, OrderTransition
from app.infra.models import orders_table
from app.repositories.base import SQLAlchemyRepository


class OrdersRepository(SQLAlchemyRepository):
    async def complete(self, order_id: UUID) -> OrderTransition:
        # ADR-006
        completed_at = (
            await self._session.execute(
                update(orders_table)
                .where(
                    orders_table.c.id == order_id,
                    orders_table.c.status == OrderStatus.PENDING.value,
                )
                .values(
                    status=OrderStatus.COMPLETED.value,
                    completed_at=func.now(),
                    updated_at=func.now(),
                )
                .returning(orders_table.c.completed_at)
            )
        ).scalar_one_or_none()
        if completed_at is not None:
            return OrderTransition(
                transitioned=True,
                completed_at=completed_at,
                status=OrderStatus.COMPLETED.value,
            )

        # El evento lleva la marca escrita en la fila, no la del proceso: bajo contencion difieren.
        existing = (
            await self._session.execute(
                select(orders_table.c.status, orders_table.c.completed_at).where(
                    orders_table.c.id == order_id
                )
            )
        ).one_or_none()
        if existing is None:
            return OrderTransition(transitioned=False, completed_at=None, status=None)
        return OrderTransition(
            transitioned=False, completed_at=existing.completed_at, status=existing.status
        )
