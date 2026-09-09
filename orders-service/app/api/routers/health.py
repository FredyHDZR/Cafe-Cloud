from fastapi import APIRouter, status

from app.api.deps import SettingsDep
from app.api.schemas.health import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse, status_code=status.HTTP_200_OK)
async def health(settings: SettingsDep) -> HealthResponse:
    return HealthResponse(status="ok", service=settings.service_name)
