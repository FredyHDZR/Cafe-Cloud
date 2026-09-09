import asyncio
import logging
import signal
from contextlib import suppress

from app.domain.envelope import OrderCompletedEnvelope, parse_order_completed
from app.domain.errors import NonRetryableError
from app.domain.notify import NotifyCustomerService, NotifyOutcome
from app.infra.config import ConsumerSettings, get_consumer_settings
from app.infra.logging import configure_logging, log_context
from app.infra.mongo import MongoDatabase
from app.infra.streams import RedisStreamConsumer, StreamMessage
from app.infra.tracing import trace_id_scope
from app.repositories.notifications import NotificationRepository

logger = logging.getLogger(__name__)


class NotificationConsumer:
    def __init__(
        self,
        *,
        settings: ConsumerSettings,
        mongo: MongoDatabase,
        stream: RedisStreamConsumer,
    ) -> None:
        self._settings = settings
        self._mongo = mongo
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
                event = parse_order_completed(message.fields)
            except NonRetryableError as error:
                self._reject(message, error)
                return

            try:
                outcome = await self._notify(event)
            except Exception:
                # Sin XACK: el mensaje sigue pendiente y TICKET-007 lo reclamara.
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
                    customer_id=outcome.notification.customer_id,
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

    async def _notify(self, event: OrderCompletedEnvelope) -> NotifyOutcome:
        service = NotifyCustomerService(
            notifications=NotificationRepository(self._mongo.notifications)
        )
        return await service.run(event)

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

    mongo = MongoDatabase.from_settings(settings)
    stream = RedisStreamConsumer.from_settings(settings)
    stop = asyncio.Event()
    _install_stop_handlers(stop)

    try:
        dedup_index = await mongo.verify_dedup_index()
        group_created = await stream.ensure_group()
    except Exception:
        logger.exception("consumer_start_failed")
        await stream.close()
        mongo.close()
        raise

    logger.info(
        "consumer_started",
        extra=log_context(
            stream=stream.stream,
            group=stream.group,
            consumer=stream.consumer,
            group_created=group_created,
            namespace=mongo.namespace,
            dedup_index=dedup_index,
            batch_size=settings.batch_size,
            block_ms=settings.block_ms,
        ),
    )
    try:
        await NotificationConsumer(settings=settings, mongo=mongo, stream=stream).run(stop)
    finally:
        await stream.close()
        mongo.close()
        logger.info("consumer_stopped")


if __name__ == "__main__":
    asyncio.run(main())
