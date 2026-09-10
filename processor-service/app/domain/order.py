from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"


@dataclass(frozen=True, slots=True)
class OrderTransition:
    transitioned: bool
    completed_at: datetime | None
    status: str | None
