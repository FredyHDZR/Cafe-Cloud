from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from redis.typing import EncodableT, FieldT

from app.infra.config import ConsumerSettings, Settings

GROUP_EXISTS_PREFIX = "BUSYGROUP"
NEW_MESSAGES = ">"
CLAIM_START_ID = "0-0"
PENDING_LOOKUP_COUNT = 1000


@dataclass(frozen=True, slots=True)
class StreamMessage:
    entry_id: str
    fields: dict[str, str]


class RedisStreamConsumer:
    def __init__(
        self,
        client: Redis,
        *,
        stream: str,
        group: str,
        consumer: str,
        block_ms: int,
        batch_size: int,
        dlq_stream: str,
    ) -> None:
        self._client = client
        self._stream = stream
        self._group = group
        self._consumer = consumer
        self._block_ms = block_ms
        self._batch_size = batch_size
        self._dlq_stream = dlq_stream

    @classmethod
    def from_settings(cls, settings: ConsumerSettings) -> "RedisStreamConsumer":
        client: Redis = Redis.from_url(settings.redis_dsn, decode_responses=True)
        return cls(
            client,
            stream=settings.stream,
            group=settings.group,
            consumer=settings.resolved_consumer_name,
            block_ms=settings.block_ms,
            batch_size=settings.batch_size,
            dlq_stream=settings.resolved_dlq_stream,
        )

    @property
    def stream(self) -> str:
        return self._stream

    @property
    def group(self) -> str:
        return self._group

    @property
    def consumer(self) -> str:
        return self._consumer

    @property
    def dlq_stream(self) -> str:
        return self._dlq_stream

    async def ensure_group(self) -> bool:
        try:
            await self._client.xgroup_create(self._stream, self._group, id="$", mkstream=True)
        except ResponseError as error:
            if not str(error).startswith(GROUP_EXISTS_PREFIX):
                raise
            return False
        return True

    async def read(self) -> list[StreamMessage]:
        response = await self._client.xreadgroup(
            groupname=self._group,
            consumername=self._consumer,
            streams={self._stream: NEW_MESSAGES},
            count=self._batch_size,
            block=self._block_ms,
        )
        return _to_messages(response)

    async def autoclaim(self, *, min_idle_ms: int, count: int) -> list[StreamMessage]:
        response = await self._client.xautoclaim(
            self._stream,
            self._group,
            self._consumer,
            min_idle_ms,
            start_id=CLAIM_START_ID,
            count=count,
        )
        return _to_claimed(response)

    async def delivery_counts(self, entry_ids: Sequence[str]) -> dict[str, int]:
        if not entry_ids:
            return {}
        pending = await self._client.xpending_range(
            self._stream,
            self._group,
            min=entry_ids[0],
            max=entry_ids[-1],
            count=PENDING_LOOKUP_COUNT,
            consumername=self._consumer,
        )
        return {str(item["message_id"]): int(item["times_delivered"]) for item in pending}

    async def pending_count(self) -> int:
        summary = await self._client.xpending(self._stream, self._group)
        return int(summary["pending"])

    async def dlq_length(self) -> int:
        return int(await self._client.xlen(self._dlq_stream))

    async def ping(self) -> None:
        await self._client.ping()

    async def dead_letter(self, fields: Mapping[str, str]) -> str:
        entry = cast(dict[FieldT, EncodableT], dict(fields))
        # Sin MAXLEN: recortar la DLQ seria tirar lo que se guarda justo para no perderlo.
        entry_id = await self._client.xadd(self._dlq_stream, entry)
        return str(entry_id)

    async def ack(self, entry_id: str) -> None:
        await self._client.xack(self._stream, self._group, entry_id)

    async def close(self) -> None:
        await self._client.aclose()


def _to_messages(response: Any) -> list[StreamMessage]:
    if not response:
        return []
    messages: list[StreamMessage] = []
    for _stream, entries in response:
        messages.extend(
            StreamMessage(entry_id=str(entry_id), fields=dict(fields))
            for entry_id, fields in entries
        )
    return messages


def _to_claimed(response: Any) -> list[StreamMessage]:
    if not response or len(response) < 2:
        return []
    return [
        StreamMessage(entry_id=str(entry_id), fields=dict(fields))
        for entry_id, fields in response[1]
        if fields
    ]


class RedisBroker:
    def __init__(self, client: Redis) -> None:
        self._client = client

    @classmethod
    def from_settings(cls, settings: Settings) -> "RedisBroker":
        client: Redis = Redis.from_url(settings.redis_dsn, decode_responses=True)
        return cls(client)

    async def ping(self) -> None:
        await self._client.ping()

    async def close(self) -> None:
        await self._client.aclose()
