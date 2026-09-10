from fastapi import APIRouter
from starlette.responses import Response

from app.api.deps import MetricsCollectorDep
from app.infra.metrics import CONTENT_TYPE, render

router = APIRouter(tags=["observability"])


@router.get("/metrics", response_class=Response)
async def metrics(collector: MetricsCollectorDep) -> Response:
    await collector.collect()
    return Response(content=render(), media_type=CONTENT_TYPE)
