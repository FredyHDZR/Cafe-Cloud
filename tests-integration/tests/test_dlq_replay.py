import json
import os
import subprocess
from datetime import UTC, datetime

from tests.support import Broker, Case, Config, envelope_fields, new_uuid, to_rfc3339

LAB_PAYLOAD = {"order_id": "00000000-0000-4000-8000-000000000000", "customer_id": "lab"}


def run_replay(config: Config, *, stream: str, dlq: str) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    environment.update(
        {
            "COMPOSE": config.compose_shim,
            "REDIS_URL": config.redis_url,
            "STREAM": stream,
            "DLQ": dlq,
        }
    )
    return subprocess.run(
        [config.dlq_replay_script],
        capture_output=True,
        text=True,
        env=environment,
        check=False,
    )


def dead_letter_fields(envelope: dict[str, str]) -> dict[str, str]:
    return {
        "original_stream": "laboratorio",
        "original_id": "0-1",
        "consumer_group": "laboratorio",
        "delivery_count": "3",
        "first_failed_at": to_rfc3339(datetime.now(UTC)),
        "last_error": "InvalidEnvelopeError: laboratorio",
        "reason": "invalid_envelope",
        "envelope": json.dumps(envelope, sort_keys=True),
    }


def test_the_replay_reinjects_the_entry_with_its_original_event_id(
    broker: Broker, case: Case, config: Config
) -> None:
    stream = case.track_stream(f"{case.customer_id}.replay")
    dlq = case.track_stream(f"{stream}.dlq")
    event_id = new_uuid()
    envelope = envelope_fields(
        event_id=event_id,
        event_type="orders.created",
        trace_id=new_uuid(),
        occurred_at=datetime.now(UTC),
        payload=LAB_PAYLOAD,
    )
    broker.publish(dlq, dead_letter_fields(envelope))

    replay = run_replay(config, stream=stream, dlq=dlq)

    assert replay.returncode == 0, replay.stdout + replay.stderr
    reinjected = broker.entries(stream)
    assert len(reinjected) == 1
    assert reinjected[0][1] == envelope
    assert reinjected[0][1]["event_id"] == event_id
    assert broker.length(dlq) == 0


def test_an_unreadable_entry_stays_in_the_dlq_and_does_not_block_the_batch(
    broker: Broker, case: Case, config: Config
) -> None:
    stream = case.track_stream(f"{case.customer_id}.broken")
    dlq = case.track_stream(f"{stream}.dlq")
    good = envelope_fields(
        event_id=new_uuid(),
        event_type="orders.created",
        trace_id=new_uuid(),
        occurred_at=datetime.now(UTC),
        payload=LAB_PAYLOAD,
    )
    broker.publish(dlq, {**dead_letter_fields(good), "envelope": "{no soy json"})
    broker.publish(dlq, dead_letter_fields(good))

    replay = run_replay(config, stream=stream, dlq=dlq)

    assert replay.returncode == 3, replay.stdout + replay.stderr
    assert "OMITIDA" in replay.stdout
    reinjected = broker.entries(stream)
    assert len(reinjected) == 1
    assert reinjected[0][1]["event_id"] == good["event_id"]
    assert broker.length(dlq) == 1
