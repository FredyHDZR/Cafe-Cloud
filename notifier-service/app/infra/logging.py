import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

from app.infra.config import LogLevel
from app.infra.tracing import get_trace_id

LOG_CONTEXT_FIELD = "context"


class JsonFormatter(logging.Formatter):
    def __init__(self, service_name: str) -> None:
        super().__init__()
        self._service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "service": self._service_name,
            "trace_id": get_trace_id(),
            "message": record.getMessage(),
            "logger": record.name,
        }
        context = getattr(record, LOG_CONTEXT_FIELD, None)
        if isinstance(context, dict):
            entry[LOG_CONTEXT_FIELD] = context
        if record.exc_info is not None:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False, default=str)


def configure_logging(service_name: str, log_level: LogLevel) -> None:
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter(service_name))

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(log_level)

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers = []
        logger.propagate = True


def log_context(**fields: Any) -> dict[str, dict[str, Any]]:
    return {LOG_CONTEXT_FIELD: fields}
