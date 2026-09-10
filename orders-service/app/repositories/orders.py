from uuid import UUID

from sqlalchemy import insert, select
from sqlalchemy.orm import selectinload

from app.domain.order import NewOrder, Order, OrderItem, OrderStatus
from app.infra.models import Order as OrderRow
from app.infra.models import OrderItem as OrderItemRow
from app.repositories.base import SQLAlchemyRepository


def _to_domain(row: OrderRow) -> Order:
    return Order(
        id=row.id,
        customer_id=row.customer_id,
        status=row.status,
        items=tuple(OrderItem(name=item.name, qty=item.qty) for item in row.items),
        created_at=row.created_at,
        completed_at=row.completed_at,
    )


class OrderRepository(SQLAlchemyRepository):
    async def add(self, draft: NewOrder) -> Order:
        created = (
            await self._session.execute(
                insert(OrderRow)
                .values(customer_id=draft.customer_id, status=OrderStatus.PENDING)
                .returning(OrderRow.id, OrderRow.created_at)
            )
        ).one()
        await self._session.execute(
            insert(OrderItemRow),
            [{"order_id": created.id, "name": item.name, "qty": item.qty} for item in draft.items],
        )
        return Order(
            id=created.id,
            customer_id=draft.customer_id,
            status=OrderStatus.PENDING,
            items=draft.items,
            created_at=created.created_at,
        )

    async def get(self, order_id: UUID) -> Order | None:
        row = (
            await self._session.execute(
                select(OrderRow)
                .where(OrderRow.id == order_id)
                .options(selectinload(OrderRow.items))
            )
        ).scalar_one_or_none()
        return None if row is None else _to_domain(row)
