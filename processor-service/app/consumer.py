import asyncio
import logging
import random
import signal
import time
from contextlib import suppress

from app.domain.complete_order import CompleteOrderService, CompleteOutcome
from app.domain.envelope import OrderCreatedEnvelope, parse_order_created
from app.domain.errors import NonRetryableError
from app.infra.config import ConsumerSettings, get_consumer_settings
from app.infra.database import Database
from app.infra.logging import configure_logging, log_context
from app.infra.streams import RedisStreamConsumer, StreamMessage
from app.infra.tracing import trace_id_scope
from app.repositories.orders import OrdersRepository
from app.repositories.outbox import OutboxRepository
from app.repositories.processed_events import ProcessedEventRepository

logger = logging.getLogger(__name__)

MILLISECONDS = 1000


class OrderConsumer:
    def __init__(
        self,
        *,
        settings: ConsumerSettings,
        database: Database,
        stream: RedisStreamConsumer,
    ) -> None:
        self._settings = settings
        self._database = database
        self._stream = stream

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                messages = await self._stream.read()
            except Exception:
                logger.exception("consumer_read_failed")
                await self._wait(stop, self._settings.error_pause_seconds)
                continue
            for message in messages:
                if stop.is_set():
                    break
                await self._handle(message)

    async def _handle(self, message: StreamMessage) -> None:
        with trace_id_scope(message.fields.get("trace_id")):
            try:
                event = parse_order_created(message.fields)
            except NonRetryableError as error:
                self._reject(message, error)
                return

            try:
                processing_ms = await self._prepare()
                outcome = await self._complete(event, processing_ms)
            except NonRetryableError as error:
                self._reject(message, error, event_id=str(event.event_id))
                return
            except Exception:
                # Sin XACK tampoco aqui: el mensaje sigue pendiente y TICKET-007 lo reclamara.
                logger.exception(
                    "event_processing_failed",
                    extra=log_context(
                        stream_entry_id=message.entry_id,
                        event_id=str(event.event_id),
                        order_id=str(event.payload.order_id),
                    ),
                )
                return

            await self._stream.ack(message.entry_id)
            logger.info(
                "event_acked",
                extra=log_context(
                    stream_entry_id=message.entry_id,
                    event_id=str(event.event_id),
                    order_id=str(event.payload.order_id),
                    duplicate=outcome.duplicate,
                ),
            )

    @staticmethod
    def _reject(message: StreamMessage, error: NonRetryableError, **context: object) -> None:
        # Sin XACK: el mensaje se queda en la PEL hasta que TICKET-007 lo lleve a la DLQ.
        logger.error(
            "event_rejected",
            extra=log_context(
                stream_entry_id=message.entry_id,
                reason=error.reason,
                error=error.message,
                retryable=False,
                **context,
            ),
        )

    async def _prepare(self) -> int:
        seconds = random.uniform(self._settings.prep_min_seconds, self._settings.prep_max_seconds)
        started = time.monotonic()
        await asyncio.sleep(seconds)
        return round((time.monotonic() - started) * MILLISECONDS)

    async def _complete(self, event: OrderCreatedEnvelope, processing_ms: int) -> CompleteOutcome:
        async with self._database.session() as session:
            service = CompleteOrderService(
                processed_events=ProcessedEventRepository(session),
                orders=OrdersRepository(session),
                outbox=OutboxRepository(session),
                unit_of_work=session,
                consumer_group=self._settings.group,
            )
            return await service.run(event, processing_ms=processing_ms)

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
    settings = get_consumer_settings()
    configure_logging(settings.service_name, settings.log_level)

    database = Database.from_settings(settings)
    stream = RedisStreamConsumer.from_settings(settings)
    stop = asyncio.Event()
    _install_stop_handlers(stop)

    group_created = await stream.ensure_group()
    logger.info(
        "consumer_started",
        extra=log_context(
            stream=stream.stream,
            group=stream.group,
            consumer=stream.consumer,
            group_created=group_created,
            batch_size=settings.batch_size,
            block_ms=settings.block_ms,
            prep_seconds=[settings.prep_min_seconds, settings.prep_max_seconds],
        ),
    )
    try:
        await OrderConsumer(settings=settings, database=database, stream=stream).run(stop)
    finally:
        await stream.close()
        await database.dispose()
        logger.info("consumer_stopped")


if __name__ == "__main__":
    asyncio.run(main())
