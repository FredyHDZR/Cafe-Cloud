import asyncio

from app.domain.health import (
    Dependency,
    DependencyState,
    HealthService,
    HealthState,
)

TIMEOUT_SECONDS = 0.05


async def up() -> None:
    return None


async def down() -> None:
    raise ConnectionRefusedError("puerto cerrado")


async def hangs() -> None:
    await asyncio.sleep(1)


def check(*dependencies: Dependency, timeout_seconds: float = TIMEOUT_SECONDS) -> HealthService:
    return HealthService(dependencies, timeout_seconds=timeout_seconds)


def test_every_dependency_up_is_ok_and_ready() -> None:
    report = asyncio.run(
        check(Dependency(name="postgres", probe=up), Dependency(name="redis", probe=up)).check()
    )

    assert report.ready is True
    assert report.state is HealthState.OK
    assert [status.state for status in report.dependencies] == [
        DependencyState.UP,
        DependencyState.UP,
    ]


def test_a_critical_dependency_down_leaves_the_process_not_ready() -> None:
    report = asyncio.run(
        check(Dependency(name="postgres", probe=down), Dependency(name="redis", probe=up)).check()
    )

    assert report.ready is False
    assert report.state is HealthState.DOWN
    assert report.dependencies[0].error is not None


def test_a_non_critical_dependency_down_degrades_without_breaking_readiness() -> None:
    report = asyncio.run(
        check(
            Dependency(name="postgres", probe=up),
            Dependency(name="redis", probe=down, critical=False),
        ).check()
    )

    assert report.ready is True
    assert report.state is HealthState.DEGRADED


def test_a_probe_that_hangs_is_cut_by_the_timeout() -> None:
    report = asyncio.run(check(Dependency(name="postgres", probe=hangs)).check())

    assert report.ready is False
    assert report.dependencies[0].error == f"la sonda no respondio en {TIMEOUT_SECONDS}s"
    assert report.dependencies[0].latency_ms < 1000


def test_the_failure_of_one_probe_does_not_hide_the_others() -> None:
    report = asyncio.run(
        check(Dependency(name="postgres", probe=down), Dependency(name="redis", probe=up)).check()
    )

    assert [(status.name, status.state) for status in report.dependencies] == [
        ("postgres", DependencyState.DOWN),
        ("redis", DependencyState.UP),
    ]
