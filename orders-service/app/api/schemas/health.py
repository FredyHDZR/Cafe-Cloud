from pydantic import BaseModel, ConfigDict

from app.domain.health import DependencyState, DependencyStatus, HealthReport, HealthState


class DependencyResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    status: DependencyState
    critical: bool
    latency_ms: float
    error: str | None = None

    @classmethod
    def from_domain(cls, status: DependencyStatus) -> "DependencyResponse":
        return cls(
            name=status.name,
            status=status.state,
            critical=status.critical,
            latency_ms=status.latency_ms,
            error=status.error,
        )


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: HealthState
    service: str
    dependencies: list[DependencyResponse]

    @classmethod
    def from_domain(cls, report: HealthReport, *, service: str) -> "HealthResponse":
        return cls(
            status=report.state,
            service=service,
            dependencies=[DependencyResponse.from_domain(status) for status in report.dependencies],
        )


class LivenessResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    service: str
