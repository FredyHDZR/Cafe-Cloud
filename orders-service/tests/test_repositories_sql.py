import asyncio
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.sql import ClauseElement

from app.domain.events import EventDraft
from app.repositories.idempotency import IdempotencyRepository
from app.repositories.outbox import OutboxRepository

ENDPOINT = "POST /orders"
KEY = "clave-1"
FINGERPRINT = "f" * 64


class FakeResult:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def first(self) -> Any:
        return self._rows[0] if self._rows else None

    def all(self) -> list[Any]:
        return self._rows

    def one_or_none(self) -> Any:
        return self._rows[0] if self._rows else None


class RecordingSession:
    def __init__(self, rows: list[Any] | None = None) -> None:
        self.statements: list[ClauseElement] = []
        self._rows = rows if rows is not None else []

    async def execute(self, statement: ClauseElement) -> FakeResult:
        self.statements.append(statement)
        return FakeResult(self._rows)


def as_session(session: RecordingSession) -> AsyncSession:
    return cast(AsyncSession, session)


# El SQL se mira compilado contra el dialecto real, sin conectar con Postgres.
DIALECT = create_async_engine("postgresql+asyncpg://orders_rw@postgres/cafecloud").dialect


def compiled(statement: ClauseElement) -> str:
    return str(statement.compile(dialect=DIALECT))


@pytest.fixture
def session() -> RecordingSession:
    return RecordingSession()


def test_the_reservation_of_a_key_lets_postgres_decide_the_winner(
    session: RecordingSession,
) -> None:
    repository = IdempotencyRepository(as_session(session))

    asyncio.run(repository.reserve(ENDPOINT, KEY, FINGERPRINT))

    sql = compiled(session.statements[0])
    assert "INSERT INTO orders.idempotency_keys" in sql
    assert "ON CONFLICT (endpoint, idempotency_key) DO NOTHING" in sql
    assert "RETURNING orders.idempotency_keys.idempotency_key" in sql


def test_losing_the_race_for_a_key_returns_false_without_raising(
    session: RecordingSession,
) -> None:
    repository = IdempotencyRepository(as_session(session))

    assert asyncio.run(repository.reserve(ENDPOINT, KEY, FINGERPRINT)) is False


def test_winning_the_race_for_a_key_returns_true() -> None:
    session = RecordingSession(rows=[(KEY,)])
    repository = IdempotencyRepository(as_session(session))

    assert asyncio.run(repository.reserve(ENDPOINT, KEY, FINGERPRINT)) is True


def test_the_publisher_claims_rows_without_waiting_for_another_worker(
    session: RecordingSession,
) -> None:
    repository = OutboxRepository(as_session(session))

    asyncio.run(repository.claim_pending(100))

    sql = compiled(session.statements[0])
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "ORDER BY orders.outbox.id" in sql
    assert "LIMIT" in sql


def test_the_publisher_only_claims_what_is_unpublished_and_due(
    session: RecordingSession,
) -> None:
    repository = OutboxRepository(as_session(session))

    asyncio.run(repository.claim_pending(100))

    sql = compiled(session.statements[0])
    assert "orders.outbox.published_at IS NULL" in sql
    assert "orders.outbox.failed_at IS NULL" in sql
    assert "orders.outbox.next_attempt_at <= now()" in sql


def test_the_event_id_is_born_in_the_outbox_row_and_is_returned(
    session: RecordingSession,
) -> None:
    repository = OutboxRepository(as_session(session))
    order_id = uuid4()
    draft = EventDraft(
        event_type="orders.created",
        event_version=1,
        aggregate_id=order_id,
        trace_id=uuid4(),
        occurred_at=datetime.now(UTC),
        payload={"order_id": str(order_id)},
    )

    first = asyncio.run(repository.append(draft))
    second = asyncio.run(repository.append(draft))

    assert first != second
    assert "INSERT INTO orders.outbox" in compiled(session.statements[0])
