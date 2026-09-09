from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from app.domain.order import NewOrder, Order, OrderItem, OrderStatus
from app.domain.timestamps import to_rfc3339


class OrderItemRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=80)
    qty: int = Field(ge=1)


class CreateOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_id: str = Field(min_length=1, max_length=64)
    items: list[OrderItemRequest] = Field(min_length=1)

    @field_validator("customer_id")
    @classmethod
    def _reject_surrounding_spaces(cls, value: str) -> str:
        if value != value.strip():
            message = "customer_id no admite espacios en los extremos"
            raise ValueError(message)
        return value

    def to_domain(self) -> NewOrder:
        return NewOrder(
            customer_id=self.customer_id,
            items=tuple(OrderItem(name=item.name, qty=item.qty) for item in self.items),
        )


class OrderItemResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    qty: int


class OrderResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: UUID
    customer_id: str
    status: OrderStatus
    items: list[OrderItemResponse]
    created_at: datetime

    @field_serializer("created_at")
    def _serialize_created_at(self, value: datetime) -> str:
        return to_rfc3339(value)

    @classmethod
    def from_domain(cls, order: Order) -> "OrderResponse":
        return cls(
            order_id=order.id,
            customer_id=order.customer_id,
            status=order.status,
            items=[OrderItemResponse(name=item.name, qty=item.qty) for item in order.items],
            created_at=order.created_at,
        )


def render_order(order: Order) -> dict[str, Any]:
    return OrderResponse.from_domain(order).model_dump(mode="json")
