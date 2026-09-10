import asyncio
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.sql import ClauseElement

from app.repositories.orders import OrdersRepository

COMPLETED_AT = datetime(2026, 9, 10, 6, 15, 50, tzinfo=UTC)


class FakeRow:
    def __init__(self, status: str, completed_at: datetime | None) -> None:
        self.status = status
        self.completed_at = completed_at


class FakeResult:
    def __init__(self, value: Any) -> None:
        self._value = value

    def scalar_one_or_none(self) -> Any:
        return self._value

    def one_or_none(self) -> Any:
        return self._value


class RecordingSession:
    def __init__(self, results: list[Any]) -> None:
        self.statements: list[ClauseElement] = []
        self._results = results

    async def execute(self, statement: ClauseElement) -> FakeResult:
        self.statements.append(statement)
        return FakeResult(self._results[len(self.statements) - 1])


def repository_over(results: list[Any]) -> tuple[OrdersRepository, RecordingSession]:
    session = RecordingSession(results)
    return OrdersRepository(cast(AsyncSession, session)), session


# El SQL se mira compilado contra el dialecto real, sin conectar con Postgres.
DIALECT = create_async_engine("postgresql+asyncpg://processor_rw@postgres/cafecloud").dialect


def compiled(statement: ClauseElement) -> str:
    return str(statement.compile(dialect=DIALECT, compile_kwargs={"literal_binds": True}))


def test_the_update_only_touches_a_pending_order_and_returns_its_mark() -> None:
    repository, session = repository_over([COMPLETED_AT])

    transition = asyncio.run(repository.complete(uuid4()))

    sql = compiled(session.statements[0])
    assert "UPDATE orders.orders SET" in sql
    assert "orders.orders.status = 'PENDING'" in sql
    assert "RETURNING orders.orders.completed_at" in sql
    assert transition.transitioned is True
    assert transition.completed_at == COMPLETED_AT
    assert transition.status == "COMPLETED"


def test_the_winning_transition_does_not_need_a_second_query() -> None:
    repository, session = repository_over([COMPLETED_AT])

    asyncio.run(repository.complete(uuid4()))

    assert len(session.statements) == 1


def test_an_order_already_completed_is_reported_with_its_original_mark() -> None:
    repository, session = repository_over([None, FakeRow("COMPLETED", COMPLETED_AT)])

    transition = asyncio.run(repository.complete(uuid4()))

    assert len(session.statements) == 2
    assert "SELECT orders.orders.status" in compiled(session.statements[1])
    assert transition.transitioned is False
    assert transition.status == "COMPLETED"
    assert transition.completed_at == COMPLETED_AT


def test_an_order_that_does_not_exist_is_told_apart_from_one_already_completed() -> None:
    repository, _ = repository_over([None, None])

    transition = asyncio.run(repository.complete(uuid4()))

    assert transition.transitioned is False
    assert transition.status is None
    assert transition.completed_at is None


def test_the_update_writes_the_three_columns_granted_by_adr_006() -> None:
    repository, session = repository_over([COMPLETED_AT])

    asyncio.run(repository.complete(uuid4()))

    sql = compiled(session.statements[0])
    assert "status='COMPLETED'" in sql
    assert "completed_at=now()" in sql
    assert "updated_at=now()" in sql
