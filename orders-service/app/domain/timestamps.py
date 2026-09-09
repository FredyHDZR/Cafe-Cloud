from datetime import UTC, datetime


def to_rfc3339(value: datetime) -> str:
    moment = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")
