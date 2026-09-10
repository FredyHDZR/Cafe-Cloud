import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from time import perf_counter

from app.infra.logging import log_context

logger = logging.getLogger(__name__)

MAX_ERROR_LENGTH = 200


class DependencyState(StrEnum):
    UP = "up"
    DOWN = "down"


class HealthState(StrEnum):
    OK = "ok"
    DEGRADED = "degraded"
    DOWN = "down"


Probe = Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class Dependency:
    name: str
    probe: Probe
    critical: bool = True


@dataclass(frozen=True, slots=True)
class DependencyStatus:
    name: str
    state: DependencyState
    critical: bool
    latency_ms: float
    error: str | None = None


@dataclass(frozen=True, slots=True)
class HealthReport:
    dependencies: tuple[DependencyStatus, ...]

    @property
    def ready(self) -> bool:
        return all(
            status.state is DependencyState.UP for status in self.dependencies if status.critical
        )

    @property
    def state(self) -> HealthState:
        if not self.ready:
            return HealthState.DOWN
        if any(status.state is DependencyState.DOWN for status in self.dependencies):
            return HealthState.DEGRADED
        return HealthState.OK


class HealthService:
    def __init__(self, dependencies: Sequence[Dependency], *, timeout_seconds: float) -> None:
        self._dependencies = tuple(dependencies)
        self._timeout_seconds = timeout_seconds

    async def check(self) -> HealthReport:
        statuses = await asyncio.gather(
            *(self._check_one(dependency) for dependency in self._dependencies)
        )
        return HealthReport(dependencies=tuple(statuses))

    async def _check_one(self, dependency: Dependency) -> DependencyStatus:
        started = perf_counter()
        error: str | None = None
        try:
            await asyncio.wait_for(dependency.probe(), timeout=self._timeout_seconds)
        except TimeoutError:
            error = f"la sonda no respondio en {self._timeout_seconds}s"
        except Exception as failure:
            error = describe(failure)
        latency_ms = round((perf_counter() - started) * 1000, 2)
        if error is None:
            return DependencyStatus(
                name=dependency.name,
                state=DependencyState.UP,
                critical=dependency.critical,
                latency_ms=latency_ms,
            )
        logger.warning(
            "dependency_check_failed",
            extra=log_context(
                dependency=dependency.name,
                critical=dependency.critical,
                latency_ms=latency_ms,
                error=error,
            ),
        )
        return DependencyStatus(
            name=dependency.name,
            state=DependencyState.DOWN,
            critical=dependency.critical,
            latency_ms=latency_ms,
            error=error,
        )


def describe(error: Exception) -> str:
    text = f"{type(error).__name__}: {error}".replace("\n", " ")
    if len(text) <= MAX_ERROR_LENGTH:
        return text
    return f"{text[:MAX_ERROR_LENGTH]}..."
