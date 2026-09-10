import json

from tests.support import (
    Broker,
    Case,
    Config,
    NotificationsApi,
    OrdersApi,
    Warehouse,
    completed_order,
    envelope_fields,
    new_uuid,
    to_rfc3339,
    wait_until,
)

CREATED_STREAM = "orders.created"
CREATED_DLQ = "orders.created.dlq"
PROCESSOR_GROUP = "processor"


def test_a_new_event_against_a_completed_order_is_dead_lettered_and_adds_no_notification(
    orders: OrdersApi,
    notifications: NotificationsApi,
    warehouse: Warehouse,
    broker: Broker,
    case: Case,
    config: Config,
) -> None:
    order_id, _ = completed_order(
        orders=orders,
        notifications=notifications,
        warehouse=warehouse,
        case=case,
        timeout=config.flow_timeout,
    )
    source = warehouse.outbox_events("orders", order_id)[0]
    before = warehouse.order(order_id)
    assert before is not None
    notifications_before = notifications.total(case.customer_id)
    completed_events_before = len(warehouse.outbox_events("processor", order_id))
    processed_before = warehouse.processed_events(source.event_id)

    injected = new_uuid()
    case.track(
        CREATED_STREAM,
        broker.publish(
            CREATED_STREAM,
            envelope_fields(
                event_id=injected,
                event_type="orders.created",
                trace_id=new_uuid(),
                occurred_at=source.occurred_at,
                payload=source.payload,
            ),
        ),
    )

    dead_letter = wait_until(
        lambda: broker.find_dead_letter(CREATED_DLQ, injected),
        timeout=config.flow_timeout,
        description="el evento contra un pedido ya COMPLETED no llego a la DLQ",
    )
    entry_id = broker.dead_letter_entry_id(CREATED_DLQ, injected)
    assert entry_id is not None
    case.track(CREATED_DLQ, entry_id)

    assert dead_letter["reason"] == "order_already_completed"
    assert dead_letter["original_stream"] == CREATED_STREAM
    assert dead_letter["delivery_count"] == "1"
    assert json.loads(dead_letter["envelope"])["event_id"] == injected

    wait_until(
        lambda: True if broker.pending(CREATED_STREAM, PROCESSOR_GROUP) == 0 else None,
        timeout=config.flow_timeout,
        description="el evento rechazado quedo sin acusar en la PEL",
    )

    assert notifications.total(case.customer_id) == notifications_before
    assert warehouse.processed_events(injected) == 0
    assert warehouse.processed_events(source.event_id) == processed_before
    assert len(warehouse.outbox_events("processor", order_id)) == completed_events_before

    after = warehouse.order(order_id)
    assert after is not None
    assert after.status == "COMPLETED"
    assert after.completed_at == before.completed_at

    envelope = json.loads(dead_letter["envelope"])
    assert envelope["event_version"] == "1"
    assert envelope["occurred_at"] == to_rfc3339(source.occurred_at)
    assert json.loads(envelope["payload"])["order_id"] == order_id
