from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.domain.idempotency import IdempotencyRecord, IdempotencyState
from app.infra.models import IdempotencyKey as IdempotencyRow
from app.repositories.base import SQLAlchemyRepository


class IdempotencyRepository(SQLAlchemyRepository):
    async def reserve(self, endpoint: str, key: str, fingerprint: str) -> bool:
        result = await self._session.execute(
            pg_insert(IdempotencyRow)
            .values(
                endpoint=endpoint,
                idempotency_key=key,
                request_hash=fingerprint,
                state=IdempotencyState.IN_PROGRESS,
            )
            .on_conflict_do_nothing(index_elements=["endpoint", "idempotency_key"])
            .returning(IdempotencyRow.idempotency_key)
        )
        return result.first() is not None

    async def find(self, endpoint: str, key: str) -> IdempotencyRecord | None:
        row = (
            await self._session.execute(
                select(
                    IdempotencyRow.request_hash,
                    IdempotencyRow.state,
                    IdempotencyRow.order_id,
                    IdempotencyRow.response_status,
                    IdempotencyRow.response_body,
                ).where(
                    IdempotencyRow.endpoint == endpoint,
                    IdempotencyRow.idempotency_key == key,
                )
            )
        ).one_or_none()
        if row is None:
            return None
        return IdempotencyRecord(
            request_hash=row.request_hash,
            state=row.state,
            order_id=row.order_id,
            response_status=row.response_status,
            response_body=row.response_body,
        )

    async def complete(
        self,
        endpoint: str,
        key: str,
        *,
        order_id: UUID,
        response_status: int,
        response_body: dict[str, Any],
    ) -> None:
        await self._session.execute(
            update(IdempotencyRow)
            .where(
                IdempotencyRow.endpoint == endpoint,
                IdempotencyRow.idempotency_key == key,
            )
            .values(
                state=IdempotencyState.COMPLETED,
                order_id=order_id,
                response_status=response_status,
                response_body=response_body,
                completed_at=func.now(),
            )
        )
