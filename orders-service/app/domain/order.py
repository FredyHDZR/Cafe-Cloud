from enum import StrEnum


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"


ALLOWED_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.PENDING: frozenset({OrderStatus.COMPLETED}),
    OrderStatus.COMPLETED: frozenset(),
}


def can_transition(current: OrderStatus, target: OrderStatus) -> bool:
    return target in ALLOWED_TRANSITIONS[current]
