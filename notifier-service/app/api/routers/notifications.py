from typing import Annotated

from fastapi import APIRouter, Path, Query, status

from app.api.deps import NotificationRepositoryDep
from app.api.schemas.errors import ErrorResponse
from app.api.schemas.notifications import NotificationPageResponse
from app.domain.notification import DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT

router = APIRouter(prefix="/notifications", tags=["notifications"])

CustomerId = Annotated[str, Path(min_length=1, max_length=64)]
Limit = Annotated[int, Query(ge=1, le=MAX_PAGE_LIMIT)]
Offset = Annotated[int, Query(ge=0)]


@router.get(
    "/{customer_id}",
    status_code=status.HTTP_200_OK,
    response_model=NotificationPageResponse,
    responses={status.HTTP_422_UNPROCESSABLE_CONTENT: {"model": ErrorResponse}},
)
async def list_notifications(
    customer_id: CustomerId,
    notifications: NotificationRepositoryDep,
    limit: Limit = DEFAULT_PAGE_LIMIT,
    offset: Offset = 0,
) -> NotificationPageResponse:
    # Un cliente sin notificaciones es una pagina vacia, no un 404: el recurso es la consulta.
    page = await notifications.list_by_customer(customer_id, limit=limit, offset=offset)
    return NotificationPageResponse.from_domain(page)
