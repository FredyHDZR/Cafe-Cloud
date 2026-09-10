import json
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import monotonic, sleep
from typing import Any, TypeVar, cast
from uuid import uuid4

import httpx
import psycopg
from redis import Redis

T = TypeVar("T")


def new_uuid() -> str:
    return str(uuid4())


POLL_INTERVAL_SECONDS = 0.25
ORDERS_ENDPOINT = "/orders"
IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"
IDEMPOTENCY_REPLAYED_HEADER = "Idempotency-Replayed"
TRACE_ID_HEADER = "X-Trace-Id"


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


@dataclass(frozen=True, slots=True)
class Config:
    orders_url: str
    processor_url: str
    notifier_url: str
    notifier_consumer_url: str
    database_url: str
    redis_url: str
    mongo_url: str
    mongo_db: str
    ready_timeout: float
    flow_timeout: float
    dlq_replay_script: str
    compose_shim: str

    @classmethod
    def from_environment(cls) -> "Config":
        return cls(
            orders_url=env("ORDERS_URL", "http://orders-service:8000"),
            processor_url=env("PROCESSOR_URL", "http://processor-service:8000"),
            notifier_url=env("NOTIFIER_URL", "http://notifier-service:8000"),
            notifier_consumer_url=env("NOTIFIER_CONSUMER_URL", "http://notifier-consumer:8000"),
            database_url=env(
                "DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/cafecloud"
            ),
            redis_url=env("REDIS_URL", "redis://redis:6379/0"),
            mongo_url=env("MONGO_URL", "mongodb://mongo:27017"),
            mongo_db=env("MONGO_DB", "cafecloud"),
            ready_timeout=env_float("READY_TIMEOUT_SECONDS", 90.0),
            flow_timeout=env_float("FLOW_TIMEOUT_SECONDS", 45.0),
            dlq_replay_script=env("DLQ_REPLAY_SCRIPT", "/opt/scripts/dlq-replay.sh"),
            compose_shim=env("COMPOSE_SHIM", "/app/bin/compose-shim"),
        )


def wait_until(probe: Callable[[], T | None], *, timeout: float, description: str) -> T:
    deadline = monotonic() + timeout
    started_at = monotonic()
    while True:
        found = probe()
        if found is not None:
            return found
        if monotonic() >= deadline:
            waited = monotonic() - started_at
            message = f"{description}: no ocurrio tras {waited:.1f} s de espera activa"
            raise AssertionError(message)
        sleep(POLL_INTERVAL_SECONDS)


def to_rfc3339(value: datetime) -> str:
    moment = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def envelope_fields(
    *,
    event_id: str,
    event_type: str,
    trace_id: str,
    occurred_at: datetime,
    payload: Mapping[str, Any],
    event_version: int = 1,
) -> dict[str, str]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_version": str(event_version),
        "occurred_at": to_rfc3339(occurred_at),
        "trace_id": trace_id,
        "payload": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
    }


@dataclass(frozen=True, slots=True)
class OutboxEvent:
    event_id: str
    event_type: str
    event_version: int
    trace_id: str
    occurred_at: datetime
    payload: dict[str, Any]
    published_at: datetime | None

    def as_stream_fields(self) -> dict[str, str]:
        return envelope_fields(
            event_id=self.event_id,
            event_type=self.event_type,
            event_version=self.event_version,
            trace_id=self.trace_id,
            occurred_at=self.occurred_at,
            payload=self.payload,
        )


@dataclass(frozen=True, slots=True)
class OrderRow:
    status: str
    completed_at: datetime | None


class Warehouse:
    def __init__(self, connection: psycopg.Connection[tuple[Any, ...]]) -> None:
        self._connection = connection

    def _query(self, sql: str, parameters: Sequence[Any]) -> list[tuple[Any, ...]]:
        with self._connection.cursor() as cursor:
            cursor.execute(sql, parameters)
            return cursor.fetchall()

    def _execute(self, sql: str, parameters: Sequence[Any]) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(sql, parameters)

    def order(self, order_id: str) -> OrderRow | None:
        rows = self._query(
            "SELECT status, completed_at FROM orders.orders WHERE id = %s", (order_id,)
        )
        if not rows:
            return None
        return OrderRow(status=rows[0][0], completed_at=rows[0][1])

    def outbox_events(self, schema: str, order_id: str) -> list[OutboxEvent]:
        rows = self._query(
            f"SELECT event_id, event_type, event_version, trace_id, occurred_at, payload,"
            f" published_at FROM {schema}.outbox WHERE aggregate_id = %s ORDER BY id",
            (order_id,),
        )
        return [
            OutboxEvent(
                event_id=str(row[0]),
                event_type=row[1],
                event_version=row[2],
                trace_id=str(row[3]),
                occurred_at=row[4],
                payload=row[5],
                published_at=row[6],
            )
            for row in rows
        ]

    def processed_events(self, event_id: str) -> int:
        rows = self._query(
            "SELECT count(*) FROM processor.processed_events WHERE event_id = %s", (event_id,)
        )
        return int(rows[0][0])

    def idempotency_rows(self, key: str) -> list[tuple[str, str | None]]:
        rows = self._query(
            "SELECT state, order_id::text FROM orders.idempotency_keys WHERE idempotency_key = %s",
            (key,),
        )
        return [(row[0], row[1]) for row in rows]

    def column_grants(self, *, role: str, schema: str, table: str, privilege: str) -> set[str]:
        rows = self._query(
            "SELECT column_name FROM information_schema.column_privileges"
            " WHERE grantee = %s AND table_schema = %s AND table_name = %s"
            " AND privilege_type = %s",
            (role, schema, table, privilege),
        )
        return {row[0] for row in rows}

    def forget(self, customer_id: str, key_prefix: str) -> None:
        order_ids = [
            str(row[0])
            for row in self._query(
                "SELECT id FROM orders.orders WHERE customer_id = %s", (customer_id,)
            )
        ]
        event_ids: list[str] = []
        for schema in ("orders", "processor"):
            event_ids.extend(
                str(row[0])
                for row in self._query(
                    f"SELECT event_id FROM {schema}.outbox WHERE aggregate_id = ANY(%s)",
                    (order_ids,),
                )
            )
        self._execute(
            "DELETE FROM processor.processed_events WHERE event_id = ANY(%s)", (event_ids,)
        )
        self._execute("DELETE FROM processor.outbox WHERE aggregate_id = ANY(%s)", (order_ids,))
        self._execute("DELETE FROM orders.outbox WHERE aggregate_id = ANY(%s)", (order_ids,))
        self._execute(
            "DELETE FROM orders.idempotency_keys WHERE idempotency_key LIKE %s",
            (f"{key_prefix}%",),
        )
        # order_items cae con el pedido por el ON DELETE CASCADE de la migracion 0001.
        self._execute("DELETE FROM orders.orders WHERE customer_id = %s", (customer_id,))


class Broker:
    def __init__(self, client: Redis) -> None:
        self._client = client

    def publish(self, stream: str, fields: Mapping[str, str]) -> str:
        entry: dict[Any, Any] = dict(fields)
        return str(self._client.xadd(stream, entry))

    def delete(self, stream: str, entry_id: str) -> None:
        self._client.xdel(stream, entry_id)

    def entries(self, stream: str, count: int = 50) -> list[tuple[str, dict[str, str]]]:
        raw = cast(list[tuple[str, dict[str, str]]], self._client.xrevrange(stream, count=count))
        return [(str(entry_id), dict(fields)) for entry_id, fields in raw]

    def find_dead_letter(self, stream: str, event_id: str) -> dict[str, str] | None:
        for _, fields in self.entries(stream):
            envelope = fields.get("envelope", "")
            if f'"{event_id}"' in envelope:
                return fields
        return None

    def dead_letter_entry_id(self, stream: str, event_id: str) -> str | None:
        for entry_id, fields in self.entries(stream):
            if f'"{event_id}"' in fields.get("envelope", ""):
                return entry_id
        return None

    def pending(self, stream: str, group: str) -> int:
        summary = cast(dict[str, Any], self._client.xpending(stream, group))
        return int(summary["pending"])

    def length(self, stream: str) -> int:
        return int(cast(int, self._client.xlen(stream)))

    def drop(self, *streams: str) -> None:
        for stream in streams:
            self._client.delete(stream)


class MetricsReader:
    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    def value(self, sample: str) -> float:
        body = self._client.get("/metrics").text
        for line in body.splitlines():
            if line.startswith(sample) and line[len(sample) : len(sample) + 1] in (" ", "{"):
                return float(line.rsplit(" ", 1)[1])
        return 0.0


@dataclass
class Case:
    customer_id: str
    tracked_entries: list[tuple[str, str]] = field(default_factory=list)
    tracked_streams: list[str] = field(default_factory=list)

    @classmethod
    def new(cls) -> "Case":
        return cls(customer_id=f"it-{uuid4().hex[:12]}")

    @property
    def key_prefix(self) -> str:
        return f"{self.customer_id}-"

    def key(self, suffix: str) -> str:
        return f"{self.key_prefix}{suffix}"

    def track(self, stream: str, entry_id: str) -> str:
        self.tracked_entries.append((stream, entry_id))
        return entry_id

    def track_stream(self, stream: str) -> str:
        self.tracked_streams.append(stream)
        return stream


class OrdersApi:
    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    def create(
        self,
        *,
        customer_id: str,
        key: str,
        trace_id: str,
        items: Sequence[Mapping[str, Any]] | None = None,
    ) -> httpx.Response:
        body = {
            "customer_id": customer_id,
            "items": list(items) if items is not None else [{"name": "flat white", "qty": 1}],
        }
        return self._client.post(
            ORDERS_ENDPOINT,
            json=body,
            headers={IDEMPOTENCY_KEY_HEADER: key, TRACE_ID_HEADER: trace_id},
        )

    def get(self, order_id: str) -> httpx.Response:
        return self._client.get(f"{ORDERS_ENDPOINT}/{order_id}")

    def status_of(self, order_id: str) -> str:
        response = self.get(order_id)
        response.raise_for_status()
        status: str = response.json()["status"]
        return status


class NotificationsApi:
    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    def page(self, customer_id: str, **params: int) -> httpx.Response:
        return self._client.get(f"/notifications/{customer_id}", params=params)

    def items(self, customer_id: str) -> list[dict[str, Any]]:
        response = self.page(customer_id)
        response.raise_for_status()
        items: list[dict[str, Any]] = response.json()["items"]
        return items

    def total(self, customer_id: str) -> int:
        response = self.page(customer_id, limit=1)
        response.raise_for_status()
        total: int = response.json()["total"]
        return total


def completed_order(
    *,
    orders: OrdersApi,
    notifications: NotificationsApi,
    warehouse: Warehouse,
    case: Case,
    timeout: float,
    key: str = "flow",
) -> tuple[str, str]:
    trace_id = new_uuid()
    response = orders.create(customer_id=case.customer_id, key=case.key(key), trace_id=trace_id)
    assert response.status_code == 201, response.text
    order_id: str = response.json()["order_id"]

    wait_until(
        lambda: True if orders.status_of(order_id) == "COMPLETED" else None,
        timeout=timeout,
        description=f"el pedido {order_id} no llego a COMPLETED",
    )
    wait_until(
        lambda: True if notifications.total(case.customer_id) >= 1 else None,
        timeout=timeout,
        description=f"la notificacion de {case.customer_id} no aparecio",
    )
    assert warehouse.order(order_id) is not None
    return order_id, trace_id
