from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, Info, generate_latest
from prometheus_client.registry import REGISTRY, Collector

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
NOTIFICATIONS_INSERTED = Counter(
    "cafecloud_notifications_inserted_total",
    "Notificaciones escritas en Mongo por este proceso",
    registry=None,
)
EVENTS_CONSUMED = Counter(
    "cafecloud_events_consumed_total",
    "Eventos consumidos del stream, por desenlace",
    ["stream", "result"],
    registry=None,
)
EVENT_RETRIES = Counter(
    "cafecloud_event_retries_total",
    "Reintentos en proceso programados por el backoff",
    ["stream", "reason"],
    registry=None,
)
DEAD_LETTERS = Counter(
    "cafecloud_dead_letters_total",
    "Envios a la cola de muertos, por motivo",
    ["stream", "reason"],
    registry=None,
)
ACK_FAILURES = Counter(
    "cafecloud_event_ack_failures_total",
    "XACK fallidos: el efecto esta escrito y el mensaje se reentregara",
    ["stream"],
    registry=None,
)
MESSAGES_CLAIMED = Counter(
    "cafecloud_messages_claimed_total",
    "Mensajes reclamados de la PEL por el janitor",
    ["stream"],
    registry=None,
)
EVENT_PROCESSING_DURATION = Histogram(
    "cafecloud_event_processing_duration_seconds",
    "Duracion del tratamiento completo de un evento",
    ["stream"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
    registry=None,
)
STREAM_PENDING_ENTRIES = Gauge(
    "cafecloud_stream_pending_entries",
    "Longitud de la PEL del grupo de consumo",
    ["stream", "group"],
    registry=None,
)
DLQ_STREAM_LENGTH = Gauge(
    "cafecloud_dlq_stream_length",
    "Entradas acumuladas en la cola de muertos",
    ["stream"],
    registry=None,
)
NOTIFICATIONS_STORED = Gauge(
    "cafecloud_notifications_stored",
    "Documentos vivos en la coleccion de notificaciones",
    registry=None,
)
NOTIFICATIONS_OLDEST_AGE = Gauge(
    "cafecloud_notifications_oldest_age_seconds",
    "Antiguedad del documento mas viejo: la sonda del cleanup-job es su efecto",
    registry=None,
)
METRICS_COLLECTION_ERRORS = Counter(
    "cafecloud_metrics_collection_errors_total",
    "Fallos al leer del almacen una metrica derivada",
    ["source"],
)

CONSUME_RESULTS = ("processed", "duplicate")

# Cada rol expone lo que de verdad mide. La API no incrementa los contadores del consumidor, y
# el consumidor no cuenta los documentos de la coleccion: publicar el cero del otro seria mentir.
API_METRICS: tuple[Collector, ...] = (NOTIFICATIONS_STORED, NOTIFICATIONS_OLDEST_AGE)
CONSUMER_METRICS: tuple[Collector, ...] = (
    NOTIFICATIONS_INSERTED,
    EVENTS_CONSUMED,
    EVENT_RETRIES,
    DEAD_LETTERS,
    ACK_FAILURES,
    MESSAGES_CLAIMED,
    EVENT_PROCESSING_DURATION,
    STREAM_PENDING_ENTRIES,
    DLQ_STREAM_LENGTH,
)

_registered: set[str] = set()


def set_service_info(service: str, version: str) -> None:
    SERVICE.info({"service": service, "version": version})


def register_api_metrics() -> None:
    _register("api", API_METRICS)


def register_consumer_metrics(stream: str) -> None:
    _register("consumer", CONSUMER_METRICS)
    for result in CONSUME_RESULTS:
        EVENTS_CONSUMED.labels(stream=stream, result=result)


def _register(group: str, collectors: tuple[Collector, ...]) -> None:
    # El registro es global y los dos procesos comparten este modulo: registrar dos veces el
    # mismo grupo (una aplicacion por prueba) reventaria con "duplicated timeseries".
    if group in _registered:
        return
    _registered.add(group)
    for collector in collectors:
        REGISTRY.register(collector)


def render() -> bytes:
    return generate_latest(REGISTRY)
