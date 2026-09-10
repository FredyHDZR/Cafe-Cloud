from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, Info, generate_latest
from prometheus_client.registry import REGISTRY

CONTENT_TYPE = CONTENT_TYPE_LATEST

SERVICE = Info("cafecloud_service", "Proceso que expone estas metricas")

HTTP_REQUESTS = Counter(
    "cafecloud_http_requests_total",
    "Respuestas HTTP servidas, por metodo, ruta y codigo",
    ["method", "path", "status"],
)
HTTP_DURATION = Histogram(
    "cafecloud_http_request_duration_seconds",
    "Duracion de las peticiones HTTP",
    ["method", "path"],
)
EVENTS_CONSUMED = Counter(
    "cafecloud_events_consumed_total",
    "Eventos consumidos del stream, por desenlace",
    ["stream", "result"],
)
EVENT_RETRIES = Counter(
    "cafecloud_event_retries_total",
    "Reintentos en proceso programados por el backoff",
    ["stream", "reason"],
)
DEAD_LETTERS = Counter(
    "cafecloud_dead_letters_total",
    "Envios a la cola de muertos, por motivo",
    ["stream", "reason"],
)
ACK_FAILURES = Counter(
    "cafecloud_event_ack_failures_total",
    "XACK fallidos: el efecto esta escrito y el mensaje se reentregara",
    ["stream"],
)
MESSAGES_CLAIMED = Counter(
    "cafecloud_messages_claimed_total",
    "Mensajes reclamados de la PEL por el janitor",
    ["stream"],
)
EVENT_PROCESSING_DURATION = Histogram(
    "cafecloud_event_processing_duration_seconds",
    "Duracion del tratamiento completo de un evento, incluida la preparacion simulada",
    ["stream"],
    buckets=(0.5, 1, 2, 3, 4, 5, 7.5, 10, 30, 60),
)
ORDER_PREPARATION_DURATION = Histogram(
    "cafecloud_order_preparation_duration_seconds",
    "Duracion de la preparacion simulada del pedido",
    buckets=(2, 2.5, 3, 3.5, 4, 4.5, 5, 7.5),
)
STREAM_PENDING_ENTRIES = Gauge(
    "cafecloud_stream_pending_entries",
    "Longitud de la PEL del grupo de consumo",
    ["stream", "group"],
)
DLQ_STREAM_LENGTH = Gauge(
    "cafecloud_dlq_stream_length",
    "Entradas acumuladas en la cola de muertos",
    ["stream"],
)
OUTBOX_ROWS = Gauge(
    "cafecloud_outbox_rows",
    "Filas del outbox por estado",
    ["state"],
)
OUTBOX_OLDEST_PENDING_AGE = Gauge(
    "cafecloud_outbox_oldest_pending_age_seconds",
    "Antiguedad del evento pendiente mas viejo: la sonda del publicador es su efecto",
)
METRICS_COLLECTION_ERRORS = Counter(
    "cafecloud_metrics_collection_errors_total",
    "Fallos al leer del almacen una metrica derivada",
    ["source"],
)

OUTBOX_STATES = ("pending", "published", "failed")
CONSUME_RESULTS = ("processed", "duplicate")


def set_service_info(service: str, version: str) -> None:
    SERVICE.info({"service": service, "version": version})


def initialise(stream: str) -> None:
    for state in OUTBOX_STATES:
        OUTBOX_ROWS.labels(state=state).set(0)
    for result in CONSUME_RESULTS:
        EVENTS_CONSUMED.labels(stream=stream, result=result)


def render() -> bytes:
    return generate_latest(REGISTRY)
