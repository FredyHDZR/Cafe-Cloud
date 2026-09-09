import asyncio
from typing import Any

import pytest

from app.infra.mongo import MissingDedupIndexError, MongoDatabase

DSN = "mongodb://localhost:27017"

WITH_DEDUP_INDEX: dict[str, Any] = {
    "_id_": {"key": [("_id", 1)], "v": 2},
    "uq_notifications_event_id": {"key": [("event_id", 1)], "unique": True, "v": 2},
    "ix_notifications_customer_id_created_at": {
        "key": [("customer_id", 1), ("created_at", -1)],
        "v": 2,
    },
}


def verify(monkeypatch: pytest.MonkeyPatch, indexes: dict[str, Any]) -> str:
    async def index_information(self: Any) -> dict[str, Any]:
        return indexes

    monkeypatch.setattr(
        "motor.motor_asyncio.AsyncIOMotorCollection.index_information", index_information
    )
    mongo = MongoDatabase(DSN, database="cafecloud", collection="notifications")
    try:
        return asyncio.run(mongo.verify_dedup_index())
    finally:
        mongo.close()


def test_the_unique_index_on_event_id_is_found_by_its_definition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert verify(monkeypatch, WITH_DEDUP_INDEX) == "uq_notifications_event_id"


def test_an_index_on_event_id_that_is_not_unique_does_not_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    indexes = {
        "_id_": {"key": [("_id", 1)], "v": 2},
        "ix_notifications_event_id": {"key": [("event_id", 1)], "v": 2},
    }

    with pytest.raises(MissingDedupIndexError):
        verify(monkeypatch, indexes)


def test_a_compound_index_that_starts_with_event_id_does_not_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    indexes = {
        "_id_": {"key": [("_id", 1)], "v": 2},
        "uq_event_id_customer_id": {
            "key": [("event_id", 1), ("customer_id", 1)],
            "unique": True,
            "v": 2,
        },
    }

    with pytest.raises(MissingDedupIndexError):
        verify(monkeypatch, indexes)


def test_an_empty_collection_without_indexes_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(MissingDedupIndexError, match=r"cafecloud\.notifications"):
        verify(monkeypatch, {})


def test_a_partial_unique_index_on_event_id_does_not_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    indexes = {
        "_id_": {"key": [("_id", 1)], "v": 2},
        "uq_notifications_event_id": {
            "key": [("event_id", 1)],
            "unique": True,
            "partialFilterExpression": {"status": {"$eq": "PENDING"}},
            "v": 2,
        },
    }

    with pytest.raises(MissingDedupIndexError):
        verify(monkeypatch, indexes)
