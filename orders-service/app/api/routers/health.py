from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from starlette.responses import Response

from app.api.deps import HealthServiceDep, SettingsDep
from app.api.schemas.health import HealthResponse, LivenessResponse

router = APIRouter(tags=["observability"])

ALIVE = "alive"

READINESS_RESPONSES: dict[int | str, dict[str, type[HealthResponse]]] = {
    status.HTTP_503_SERVICE_UNAVAILABLE: {"model": HealthResponse}
}


async def _readiness(health: HealthServiceDep, service: str) -> Response:
    report = await health.check()
    body = HealthResponse.from_domain(report, service=service)
    code = status.HTTP_200_OK if report.ready else status.HTTP_503_SERVICE_UNAVAILABLE
    return JSONResponse(status_code=code, content=body.model_dump(mode="json", exclude_none=True))


@router.get("/health", response_model=HealthResponse, responses=READINESS_RESPONSES)
async def health(health_service: HealthServiceDep, settings: SettingsDep) -> Response:
    return await _readiness(health_service, settings.service_name)


@router.get("/health/live", response_model=LivenessResponse)
async def liveness(settings: SettingsDep) -> LivenessResponse:
    return LivenessResponse(status=ALIVE, service=settings.service_name)


@router.get("/health/ready", response_model=HealthResponse, responses=READINESS_RESPONSES)
async def readiness(health_service: HealthServiceDep, settings: SettingsDep) -> Response:
    return await _readiness(health_service, settings.service_name)
