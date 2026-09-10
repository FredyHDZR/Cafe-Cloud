import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from io import StringIO
from typing import Any, cast

import pytest
from pymongo.errors import PyMongoError

from job import (
    CLEANUP_JOB_ID,
    Document,
    JsonFormatter,
    NotificationCleaner,
    Settings,
    build_scheduler,
    run_cleanup_pass,
)

RETENTION = timedelta(hours=24)

ENVIRONMENT = {
    "MONGO_URL": "mongodb://mongo:27017",
    "MONGO_DB": "cafecloud",
    "SERVICE_NAME": "cleanup-job",
    "LOG_LEVEL": "info",
}


class FakeDeleteResult:
    def __init__(self, deleted_count: int) -> None:
        self.deleted_count = deleted_count


class FakeCollection:
    def __init__(self, deleted_count: int = 0, error: Exception | None = None) -> None:
        self.filters: list[Document] = []
        self._deleted_count = deleted_count
        self._error = error

    def delete_many(self, criteria: Document) -> FakeDeleteResult:
        self.filters.append(criteria)
        if self._error is not None:
            raise self._error
        return FakeDeleteResult(self._deleted_count)


def cleaner_over(collection: FakeCollection) -> NotificationCleaner:
    return NotificationCleaner(cast(Any, collection), RETENTION)


def capture_json_logs(work: Callable[[], object]) -> list[dict[str, Any]]:
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter("cleanup-job"))
    root = logging.getLogger()
    previous_handlers, previous_level = root.handlers, root.level
    root.handlers, root.level = [handler], logging.INFO
    try:
        work()
    finally:
        root.handlers, root.level = previous_handlers, previous_level
    return [json.loads(line) for line in stream.getvalue().splitlines()]


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    for name, value in ENVIRONMENT.items():
        monkeypatch.setenv(name, value)
    return Settings()


def test_the_pass_deletes_only_what_is_older_than_the_retention() -> None:
    collection = FakeCollection(deleted_count=3)

    result = cleaner_over(collection).run_once()

    criteria = collection.filters[0]
    assert set(criteria) == {"created_at"}
    assert set(criteria["created_at"]) == {"$lt"}
    assert result.deleted_count == 3


def test_the_threshold_is_now_minus_the_retention() -> None:
    collection = FakeCollection()

    before = datetime.now(tz=UTC)
    result = cleaner_over(collection).run_once()
    after = datetime.now(tz=UTC)

    assert before - RETENTION <= result.threshold <= after - RETENTION
    assert collection.filters[0]["created_at"]["$lt"] == result.threshold


def test_a_pass_that_deletes_nothing_is_reported_too(
    caplog: pytest.LogCaptureFixture,
) -> None:
    collection = FakeCollection(deleted_count=0)

    with caplog.at_level(logging.INFO):
        completed = run_cleanup_pass(cleaner_over(collection))

    assert completed is True
    record = next(entry for entry in caplog.records if entry.message == "cleanup_pass_completed")
    assert record.__dict__["context"]["deleted_count"] == 0


def test_a_mongo_failure_does_not_kill_the_process(caplog: pytest.LogCaptureFixture) -> None:
    collection = FakeCollection(error=PyMongoError("mongo caido"))

    with caplog.at_level(logging.ERROR):
        completed = run_cleanup_pass(cleaner_over(collection))

    assert completed is False
    failure = next(entry for entry in caplog.records if entry.message == "cleanup_pass_failed")
    assert failure.exc_info is not None


def test_every_pass_logs_json_with_its_own_trace_id() -> None:
    cleaner = cleaner_over(FakeCollection())

    lines = capture_json_logs(lambda: (run_cleanup_pass(cleaner), run_cleanup_pass(cleaner)))

    passes = [line for line in lines if line["message"] == "cleanup_pass_completed"]
    assert len(passes) == 2
    assert all(
        set(line) >= {"timestamp", "level", "service", "trace_id", "message"} for line in passes
    )
    assert len({line["trace_id"] for line in passes}) == 2


def test_the_scheduled_job_never_skips_a_late_pass(settings: Settings) -> None:
    scheduler = build_scheduler(cleaner_over(FakeCollection()), settings)

    job = scheduler.get_job(CLEANUP_JOB_ID)

    assert job is not None
    assert job.misfire_grace_time is None
    assert job.max_instances == 1
    assert job.coalesce is True


def test_the_first_pass_does_not_wait_for_the_interval(settings: Settings) -> None:
    scheduler = build_scheduler(cleaner_over(FakeCollection()), settings)

    job = scheduler.get_job(CLEANUP_JOB_ID)

    assert job is not None
    assert job.next_run_time <= datetime.now(tz=UTC)
    assert job.trigger.interval == timedelta(seconds=settings.interval_seconds)


def test_the_retention_window_accepts_fractions_of_an_hour(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name, value in ENVIRONMENT.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("RETENTION_HOURS", "0.5")

    assert Settings().retention == timedelta(minutes=30)


def test_a_retention_of_zero_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in ENVIRONMENT.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("RETENTION_HOURS", "0")

    with pytest.raises(ValueError, match="RETENTION_HOURS"):
        Settings()
