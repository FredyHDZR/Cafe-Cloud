from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.infra.config import ConsumerSettings

GROUP_EXISTS_PREFIX = "BUSYGROUP"
NEW_MESSAGES = ">"


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
    ) -> None:
        self._client = client
        self._stream = stream
        self._group = group
        self._consumer = consumer
        self._block_ms = block_ms
        self._batch_size = batch_size

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
