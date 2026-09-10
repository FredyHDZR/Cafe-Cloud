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
ORDERS_CREATED = Counter(
    "cafecloud_orders_created_total",
    "Pedidos creados de verdad, sin contar las reproducciones",
)
IDEMPOTENCY_REPLAYS = Counter(
    "cafecloud_idempotency_replays_total",
    "Respuestas reproducidas por Idempotency-Key",
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


def set_service_info(service: str, version: str) -> None:
    SERVICE.info({"service": service, "version": version})


def render() -> bytes:
    return generate_latest(REGISTRY)


for _state in OUTBOX_STATES:
    OUTBOX_ROWS.labels(state=_state).set(0)
