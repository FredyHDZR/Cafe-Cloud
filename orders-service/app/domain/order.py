from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"


ALLOWED_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.PENDING: frozenset({OrderStatus.COMPLETED}),
    OrderStatus.COMPLETED: frozenset(),
}


def can_transition(current: OrderStatus, target: OrderStatus) -> bool:
    return target in ALLOWED_TRANSITIONS[current]


@dataclass(frozen=True, slots=True)
class OrderItem:
    name: str
    qty: int


@dataclass(frozen=True, slots=True)
class NewOrder:
    customer_id: str
    items: tuple[OrderItem, ...]


@dataclass(frozen=True, slots=True)
class Order:
    id: UUID
    customer_id: str
    status: OrderStatus
    items: tuple[OrderItem, ...]
    created_at: datetime
    completed_at: datetime | None = None
