import asyncio
import logging
import signal
from contextlib import suppress

from app.domain.publish_outbox import (
    PublisherPolicy,
    PublishOutboxService,
    PublishOutcome,
    PurgeOutboxService,
    PurgePolicy,
)
from app.infra.config import PublisherSettings, get_publisher_settings
from app.infra.database import Database
from app.infra.logging import configure_logging, log_context
from app.infra.streams import RedisEventStream
from app.repositories.idempotency import IdempotencyRepository
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)

ERROR_PAUSE_SECONDS = 1.0


class OutboxPublisher:
    def __init__(
        self,
        *,
        settings: PublisherSettings,
        database: Database,
        stream: RedisEventStream,
    ) -> None:
        self._settings = settings
        self._database = database
        self._stream = stream
        self._publisher_policy = PublisherPolicy(
            batch_size=settings.batch_size,
            max_attempts=settings.max_attempts,
            backoff_base_seconds=settings.backoff_base_seconds,
            backoff_max_seconds=settings.backoff_max_seconds,
        )
        self._purge_policy = PurgePolicy(
            outbox_retention_hours=settings.outbox_retention_hours,
            idempotency_grace_hours=settings.idempotency_grace_hours,
            batch_size=settings.purge_batch_size,
        )

    async def run(self, stop: asyncio.Event) -> None:
        cycles = 0
        while not stop.is_set():
            cycles += 1
            try:
                outcome = await self._publish()
                if cycles % self._settings.purge_every_cycles == 0:
                    await self._purge()
            except Exception:
                logger.exception("outbox_cycle_failed", extra=log_context(cycle=cycles))
                await self._wait(stop, ERROR_PAUSE_SECONDS)
                continue
            if outcome.published < self._settings.batch_size:
                await self._wait(stop, self._settings.poll_interval_seconds)

    async def _publish(self) -> PublishOutcome:
        async with self._database.session() as session:
            service = PublishOutboxService(
                outbox=OutboxRepository(session),
                stream=self._stream,
                unit_of_work=session,
                policy=self._publisher_policy,
            )
            return await service.run_once()

    async def _purge(self) -> None:
        async with self._database.session() as session:
            service = PurgeOutboxService(
                outbox=OutboxRepository(session),
                idempotency=IdempotencyRepository(session),
                unit_of_work=session,
                policy=self._purge_policy,
            )
            await service.run()

    @staticmethod
    async def _wait(stop: asyncio.Event, seconds: float) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=seconds)


def _stop_on_signal(stop: asyncio.Event, received: signal.Signals) -> None:
    logger.info("shutdown_requested", extra=log_context(signal=received.name))
    stop.set()


def _install_stop_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for received in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(received, _stop_on_signal, stop, received)


async def main() -> None:
    settings = get_publisher_settings()
    configure_logging(settings.service_name, settings.log_level)

    database = Database.from_settings(settings)
    stream = RedisEventStream.from_settings(settings)
    stop = asyncio.Event()
    _install_stop_handlers(stop)

    logger.info(
        "outbox_publisher_started",
        extra=log_context(
            poll_interval_ms=settings.poll_interval_ms,
            batch_size=settings.batch_size,
            max_attempts=settings.max_attempts,
            purge_every_cycles=settings.purge_every_cycles,
        ),
    )
    try:
        await OutboxPublisher(settings=settings, database=database, stream=stream).run(stop)
    finally:
        await stream.close()
        await database.dispose()
        logger.info("outbox_publisher_stopped")


if __name__ == "__main__":
    asyncio.run(main())
