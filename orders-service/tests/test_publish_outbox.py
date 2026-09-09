import asyncio
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from app.domain.events import PendingEvent, envelope_fields
from app.domain.publish_outbox import (
    PublisherPolicy,
    PublishOutboxService,
    PublishOutcome,
    PurgeOutboxService,
    PurgePolicy,
    backoff_delay,
)

TRACE_ID = UUID("1f2a3b4c-5d6e-4f70-8123-9a0b1c2d3e4f")
OCCURRED_AT = datetime(2026, 9, 9, 20, 0, 0, 123456, tzinfo=UTC)

POLICY = PublisherPolicy(
    batch_size=100, max_attempts=10, backoff_base_seconds=1.0, backoff_max_seconds=300.0
)
PURGE_POLICY = PurgePolicy(outbox_retention_hours=24, idempotency_grace_hours=1, batch_size=1000)


def pending(
    row_id: int, *, attempts: int = 0, payload: dict[str, Any] | None = None
) -> PendingEvent:
    return PendingEvent(
        row_id=row_id,
        event_id=UUID(int=row_id),
        event_type="orders.created",
        event_version=1,
        trace_id=TRACE_ID,
        occurred_at=OCCURRED_AT,
        payload={"order_id": str(UUID(int=row_id))} if payload is None else payload,
        attempts=attempts,
    )


class FakeOutboxStore:
    def __init__(self, batch: list[PendingEvent]) -> None:
        self._batch = batch
        self.published: list[int] = []
        self.rescheduled: list[tuple[int, float, str]] = []
        self.failed: list[tuple[int, str]] = []
        self.deleted = 0

    async def claim_pending(self, limit: int) -> list[PendingEvent]:
        return self._batch[:limit]

    async def mark_published(self, row_ids: Sequence[int]) -> None:
        self.published.extend(row_ids)

    async def reschedule(self, row_id: int, *, delay_seconds: float, error: str) -> None:
        self.rescheduled.append((row_id, delay_seconds, error))

    async def mark_failed(self, row_id: int, *, error: str) -> None:
        self.failed.append((row_id, error))

    async def delete_published_before(self, *, hours: int, limit: int) -> int:
        self.deleted = hours + limit
        return 7


class FakeIdempotencyStore:
    async def delete_expired(self, *, grace_hours: int, limit: int) -> int:
        return 3


class FakeStream:
    def __init__(self, *, fails_from: int | None = None) -> None:
        self._fails_from = fails_from
        self.published: list[tuple[str, Mapping[str, str]]] = []

    async def publish(self, stream: str, fields: Mapping[str, str]) -> str:
        if self._fails_from is not None and len(self.published) >= self._fails_from:
            raise ConnectionError("Error 111 connecting to redis:6379")
        self.published.append((stream, fields))
        return f"1757000000000-{len(self.published)}"


class FakeUnitOfWork:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


def run_publish(
    *,
    outbox: FakeOutboxStore,
    stream: FakeStream,
    unit_of_work: FakeUnitOfWork,
    policy: PublisherPolicy = POLICY,
) -> PublishOutcome:
    service = PublishOutboxService(
        outbox=outbox, stream=stream, unit_of_work=unit_of_work, policy=policy
    )
    return asyncio.run(service.run_once())


def test_envelope_carries_the_six_fields_with_milliseconds() -> None:
    fields = envelope_fields(pending(1))

    assert set(fields) == {
        "event_id",
        "event_type",
        "event_version",
        "occurred_at",
        "trace_id",
        "payload",
    }
    assert fields["occurred_at"] == "2026-09-09T20:00:00.123Z"
    assert fields["event_version"] == "1"
    assert fields["payload"].startswith('{"order_id":')


def test_publishes_every_pending_row_and_marks_it_published() -> None:
    outbox = FakeOutboxStore([pending(1), pending(2)])
    stream = FakeStream()
    unit_of_work = FakeUnitOfWork()

    outcome = run_publish(outbox=outbox, stream=stream, unit_of_work=unit_of_work)

    assert [entry[0] for entry in stream.published] == ["orders.created", "orders.created"]
    assert outbox.published == [1, 2]
    assert outcome.published == 2
    assert unit_of_work.commits == 1


def test_an_empty_batch_neither_publishes_nor_commits() -> None:
    outbox = FakeOutboxStore([])
    stream = FakeStream()
    unit_of_work = FakeUnitOfWork()

    outcome = run_publish(outbox=outbox, stream=stream, unit_of_work=unit_of_work)

    assert outcome.published == 0
    assert stream.published == []
    assert unit_of_work.commits == 0
    assert unit_of_work.rollbacks == 1


def test_a_transient_failure_reschedules_the_row_and_stops_the_batch() -> None:
    outbox = FakeOutboxStore([pending(1), pending(2, attempts=3), pending(3)])
    stream = FakeStream(fails_from=1)
    unit_of_work = FakeUnitOfWork()

    outcome = run_publish(outbox=outbox, stream=stream, unit_of_work=unit_of_work)

    assert outbox.published == [1]
    assert outbox.rescheduled == [(2, 8.0, "ConnectionError: Error 111 connecting to redis:6379")]
    assert outbox.failed == []
    assert outcome.rescheduled == 1
    assert unit_of_work.commits == 1


def test_the_row_that_exhausts_the_attempts_is_marked_failed() -> None:
    outbox = FakeOutboxStore([pending(1, attempts=9)])
    stream = FakeStream(fails_from=0)
    unit_of_work = FakeUnitOfWork()

    outcome = run_publish(outbox=outbox, stream=stream, unit_of_work=unit_of_work)

    assert outbox.rescheduled == []
    assert [row_id for row_id, _ in outbox.failed] == [1]
    assert outcome.failed == 1
    assert unit_of_work.commits == 1


def test_a_payload_that_cannot_be_serialised_fails_without_retrying() -> None:
    outbox = FakeOutboxStore([pending(1, payload={"qty": {1, 2}}), pending(2)])
    stream = FakeStream()
    unit_of_work = FakeUnitOfWork()

    outcome = run_publish(outbox=outbox, stream=stream, unit_of_work=unit_of_work)

    assert [row_id for row_id, _ in outbox.failed] == [1]
    assert outbox.rescheduled == []
    assert outbox.published == [2]
    assert outcome.failed == 1
    assert outcome.published == 1


@pytest.mark.parametrize(
    ("attempts", "expected"),
    [(0, 1.0), (1, 2.0), (2, 4.0), (8, 256.0), (9, 300.0), (20, 300.0)],
)
def test_the_backoff_doubles_until_the_ceiling(attempts: int, expected: float) -> None:
    assert backoff_delay(attempts, base_seconds=1.0, max_seconds=300.0) == expected


def test_the_purge_commits_and_reports_both_counts() -> None:
    outbox = FakeOutboxStore([])
    unit_of_work = FakeUnitOfWork()
    service = PurgeOutboxService(
        outbox=outbox,
        idempotency=FakeIdempotencyStore(),
        unit_of_work=unit_of_work,
        policy=PURGE_POLICY,
    )

    outcome = asyncio.run(service.run())

    assert outcome.outbox_rows == 7
    assert outcome.idempotency_keys == 3
    assert unit_of_work.commits == 1
