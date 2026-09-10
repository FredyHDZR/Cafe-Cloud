import asyncio
import logging
import signal
from contextlib import suppress

from app.domain.dead_letter import DeadLetter
from app.domain.envelope import OrderCompletedEnvelope, parse_order_completed
from app.domain.errors import NonRetryableError
from app.domain.failures import Failure, classify
from app.domain.notify import NotifyCustomerService, NotifyOutcome
from app.domain.retry import RetryPolicy
from app.domain.timestamps import to_rfc3339, utc_now
from app.infra.config import ConsumerSettings, get_consumer_settings
from app.infra.logging import configure_logging, log_context
from app.infra.mongo import MongoDatabase
from app.infra.streams import RedisStreamConsumer, StreamMessage
from app.infra.tracing import trace_id_scope
from app.repositories.notifications import NotificationRepository

logger = logging.getLogger(__name__)

FIRST_DELIVERY = 1
DELIVERY_COUNT_EXCEEDED = "delivery_count_exceeded"


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
        self._policy = RetryPolicy(
            max_attempts=settings.max_attempts,
            base_seconds=settings.backoff_base_seconds,
            max_seconds=settings.backoff_max_seconds,
            jitter_min=settings.backoff_jitter_min,
            jitter_max=settings.backoff_jitter_max,
        )
        self._inflight: set[str] = set()

    def is_inflight(self, entry_id: str) -> bool:
        return entry_id in self._inflight

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                messages = await self._stream.read()
            except Exception:
                logger.exception("consumer_read_failed")
                await wait_for(stop, self._settings.error_pause_seconds)
                continue
            for message in messages:
                if stop.is_set():
                    break
                await self.handle(message, stop=stop)

    async def handle(
        self,
        message: StreamMessage,
        *,
        stop: asyncio.Event,
        delivery_count: int = FIRST_DELIVERY,
    ) -> None:
        self._inflight.add(message.entry_id)
        try:
            with trace_id_scope(message.fields.get("trace_id")):
                await self._dispatch(message, stop=stop, delivery_count=delivery_count)
        finally:
            self._inflight.discard(message.entry_id)

    async def exhaust(self, message: StreamMessage, *, delivery_count: int) -> None:
        failure = Failure(
            retryable=False,
            reason=DELIVERY_COUNT_EXCEEDED,
            message=(
                f"el mensaje se ha entregado {delivery_count} veces, por encima de "
                f"{self._settings.janitor_max_deliveries}"
            ),
        )
        with trace_id_scope(message.fields.get("trace_id")):
            await self._dead_letter(message, failure=failure, delivery_count=delivery_count)

    async def _dispatch(
        self, message: StreamMessage, *, stop: asyncio.Event, delivery_count: int
    ) -> None:
        try:
            event = parse_order_completed(message.fields)
        except NonRetryableError as error:
            await self._dead_letter(message, failure=classify(error), delivery_count=delivery_count)
            return
        except Exception as error:
            # Sin XACK: un fallo no clasificado al leer el envelope se reclama y acaba en la DLQ
            # por cuenta de entregas, no por una decision tomada a ciegas aqui.
            logger.exception(
                "event_parse_failed",
                extra=log_context(stream_entry_id=message.entry_id, reason=classify(error).reason),
            )
            return

        context: dict[str, object] = {
            "stream_entry_id": message.entry_id,
            "event_id": str(event.event_id),
            "order_id": str(event.payload.order_id),
        }
        first_failed_at: str | None = None

        for attempt in range(1, self._settings.max_attempts + 1):
            try:
                outcome = await self._notify(event)
            except Exception as error:
                failure = classify(error)
                first_failed_at = first_failed_at or to_rfc3339(utc_now())
                if failure.retryable and not self._policy.is_last(attempt):
                    delay = self._policy.delay_for(attempt)
                    logger.warning(
                        "event_retry_scheduled",
                        extra=log_context(
                            attempt=attempt,
                            max_attempts=self._settings.max_attempts,
                            delay_seconds=round(delay, 3),
                            reason=failure.reason,
                            error=failure.message,
                            **context,
                        ),
                    )
                    await wait_for(stop, delay)
                    if stop.is_set():
                        logger.info("event_retry_abandoned", extra=log_context(**context))
                        return
                    continue
                logger.error(
                    "event_processing_failed",
                    extra=log_context(
                        attempt=attempt,
                        retryable=failure.retryable,
                        reason=failure.reason,
                        error=failure.message,
                        **context,
                    ),
                )
                await self._dead_letter(
                    message,
                    failure=failure,
                    delivery_count=delivery_count,
                    first_failed_at=first_failed_at,
                    context=context,
                )
                return
            if await self._ack(message, context=context):
                logger.info(
                    "event_acked",
                    extra=log_context(
                        customer_id=outcome.notification.customer_id,
                        duplicate=outcome.duplicate,
                        attempt=attempt,
                        delivery_count=delivery_count,
                        **context,
                    ),
                )
            return

    async def _dead_letter(
        self,
        message: StreamMessage,
        *,
        failure: Failure,
        delivery_count: int,
        first_failed_at: str | None = None,
        context: dict[str, object] | None = None,
    ) -> None:
        details = context or {"stream_entry_id": message.entry_id}
        record = DeadLetter(
            original_stream=self._stream.stream,
            original_id=message.entry_id,
            consumer_group=self._stream.group,
            delivery_count=delivery_count,
            first_failed_at=first_failed_at or to_rfc3339(utc_now()),
            last_error=failure.message,
            reason=failure.reason,
            envelope=message.fields,
        )
        try:
            dlq_entry_id = await self._stream.dead_letter(record.to_fields())
        except Exception as error:
            # Sin XACK: el mensaje sigue pendiente y el janitor volvera a por el.
            logger.exception(
                "dead_letter_failed",
                extra=log_context(
                    dlq_stream=self._stream.dlq_stream,
                    reason=classify(error).reason,
                    **details,
                ),
            )
            return
        logger.error(
            "event_dead_lettered",
            extra=log_context(
                dlq_stream=self._stream.dlq_stream,
                dlq_entry_id=dlq_entry_id,
                reason=failure.reason,
                error=failure.message,
                retryable=failure.retryable,
                delivery_count=delivery_count,
                **details,
            ),
        )
        if await self._ack(message, context=details):
            logger.info("dead_letter_acked", extra=log_context(**details))

    async def _ack(self, message: StreamMessage, *, context: dict[str, object]) -> bool:
        try:
            await self._stream.ack(message.entry_id)
        except Exception as error:
            # El efecto ya esta escrito: sin XACK se reentrega y la deduplicacion lo absorbe.
            logger.error(
                "event_ack_failed",
                extra=log_context(reason=classify(error).reason, **context),
            )
            return False
        return True

    async def _notify(self, event: OrderCompletedEnvelope) -> NotifyOutcome:
        service = NotifyCustomerService(
            notifications=NotificationRepository(self._mongo.notifications)
        )
        return await service.run(event)


class StreamJanitor:
    def __init__(
        self,
        *,
        settings: ConsumerSettings,
        stream: RedisStreamConsumer,
        consumer: NotificationConsumer,
    ) -> None:
        self._settings = settings
        self._stream = stream
        self._consumer = consumer

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await wait_for(stop, self._settings.janitor_interval_seconds)
            if stop.is_set():
                return
            try:
                await self._sweep(stop)
            except Exception:
                logger.exception("janitor_sweep_failed")

    async def _sweep(self, stop: asyncio.Event) -> None:
        claimed = await self._stream.autoclaim(
            min_idle_ms=self._settings.janitor_min_idle_ms,
            count=self._settings.janitor_batch_size,
        )
        if not claimed:
            return
        deliveries = await self._stream.delivery_counts([message.entry_id for message in claimed])
        for message in claimed:
            if stop.is_set():
                return
            await self._recover(message, deliveries.get(message.entry_id, FIRST_DELIVERY), stop)

    async def _recover(
        self, message: StreamMessage, delivery_count: int, stop: asyncio.Event
    ) -> None:
        with trace_id_scope(message.fields.get("trace_id")):
            context = log_context(
                stream_entry_id=message.entry_id,
                event_id=message.fields.get("event_id"),
                delivery_count=delivery_count,
                min_idle_ms=self._settings.janitor_min_idle_ms,
            )
            if self._consumer.is_inflight(message.entry_id):
                # El propio proceso lo esta trabajando: reclamarselo a si mismo seria duplicarlo.
                logger.debug("message_claim_skipped", extra=context)
                return
            logger.warning("message_claimed", extra=context)
            if delivery_count > self._settings.janitor_max_deliveries:
                await self._consumer.exhaust(message, delivery_count=delivery_count)
                return
        await self._consumer.handle(message, stop=stop, delivery_count=delivery_count)


async def wait_for(stop: asyncio.Event, seconds: float) -> None:
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
            dlq_stream=stream.dlq_stream,
            group_created=group_created,
            namespace=mongo.namespace,
            dedup_index=dedup_index,
            batch_size=settings.batch_size,
            block_ms=settings.block_ms,
            max_attempts=settings.max_attempts,
            janitor_interval_seconds=settings.janitor_interval_seconds,
            janitor_min_idle_ms=settings.janitor_min_idle_ms,
        ),
    )
    consumer = NotificationConsumer(settings=settings, mongo=mongo, stream=stream)
    janitor = StreamJanitor(settings=settings, stream=stream, consumer=consumer)
    try:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(consumer.run(stop))
            tasks.create_task(janitor.run(stop))
    finally:
        await stream.close()
        mongo.close()
        logger.info("consumer_stopped")


if __name__ == "__main__":
    asyncio.run(main())
