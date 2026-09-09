from collections.abc import Mapping
from typing import cast

from redis.asyncio import Redis
from redis.typing import EncodableT, FieldT

from app.infra.config import PublisherSettings


class RedisEventStream:
    def __init__(self, client: Redis, *, maxlen: int) -> None:
        self._client = client
        self._maxlen = maxlen

    @classmethod
    def from_settings(cls, settings: PublisherSettings) -> "RedisEventStream":
        client: Redis = Redis.from_url(settings.redis_dsn, decode_responses=True)
        return cls(client, maxlen=settings.stream_maxlen)

    async def publish(self, stream: str, fields: Mapping[str, str]) -> str:
        entry = cast(dict[FieldT, EncodableT], dict(fields))
        entry_id = await self._client.xadd(stream, entry, maxlen=self._maxlen, approximate=True)
        return str(entry_id)

    async def close(self) -> None:
        await self._client.aclose()
