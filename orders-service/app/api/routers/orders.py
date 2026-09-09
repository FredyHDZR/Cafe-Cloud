from uuid import UUID

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from starlette.responses import Response

from app.api.deps import CreateOrderServiceDep, IdempotencyKeyDep, OrderRepositoryDep, TraceIdDep
from app.api.headers import IDEMPOTENCY_REPLAYED_HEADER
from app.api.schemas.errors import ErrorResponse
from app.api.schemas.orders import CreateOrderRequest, OrderResponse
from app.domain.create_order import CreateOrderCommand
from app.domain.errors import OrderNotFoundError

router = APIRouter(prefix="/orders", tags=["orders"])

ERROR_RESPONSES: dict[int | str, dict[str, type[ErrorResponse]]] = {
    status.HTTP_400_BAD_REQUEST: {"model": ErrorResponse},
    status.HTTP_409_CONFLICT: {"model": ErrorResponse},
    status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse},
}


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=OrderResponse,
    responses=ERROR_RESPONSES,
)
async def create_order(
    payload: CreateOrderRequest,
    idempotency_key: IdempotencyKeyDep,
    trace_id: TraceIdDep,
    service: CreateOrderServiceDep,
) -> Response:
    result = await service.execute(
        CreateOrderCommand(
            idempotency_key=idempotency_key,
            trace_id=trace_id,
            order=payload.to_domain(),
        )
    )
    headers = {IDEMPOTENCY_REPLAYED_HEADER: "true"} if result.replayed else None
    return JSONResponse(status_code=result.status_code, content=result.body, headers=headers)


@router.get(
    "/{order_id}",
    response_model=OrderResponse,
    responses={status.HTTP_404_NOT_FOUND: {"model": ErrorResponse}},
)
async def get_order(order_id: UUID, orders: OrderRepositoryDep) -> OrderResponse:
    order = await orders.get(order_id)
    if order is None:
        raise OrderNotFoundError(f"No existe el pedido {order_id}")
    return OrderResponse.from_domain(order)
