import json
from collections.abc import Mapping
from dataclasses import dataclass

from app.domain.timestamps import to_rfc3339, utc_now

DLQ_SUFFIX = ".dlq"
ENVELOPE_FIELD = "envelope"


@dataclass(frozen=True, slots=True)
class DeadLetter:
    original_stream: str
    original_id: str
    consumer_group: str
    delivery_count: int
    first_failed_at: str
    last_error: str
    reason: str
    envelope: Mapping[str, str]

    def to_fields(self) -> dict[str, str]:
        return {
            "original_stream": self.original_stream,
            "original_id": self.original_id,
            "consumer_group": self.consumer_group,
            "delivery_count": str(self.delivery_count),
            "first_failed_at": self.first_failed_at,
            "last_error": self.last_error,
            "reason": self.reason,
            "dead_lettered_at": to_rfc3339(utc_now()),
            ENVELOPE_FIELD: json.dumps(dict(self.envelope), ensure_ascii=False, sort_keys=True),
        }


def dlq_stream_for(stream: str) -> str:
    return f"{stream}{DLQ_SUFFIX}"
