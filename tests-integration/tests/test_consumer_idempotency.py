from tests.support import (
    Broker,
    Case,
    Config,
    MetricsReader,
    NotificationsApi,
    OrdersApi,
    Warehouse,
    completed_order,
    wait_until,
)

COMPLETED_STREAM = "orders.completed"
NOTIFIER_GROUP = "notifier"
DUPLICATES = 'cafecloud_events_consumed_total{result="duplicate",stream="orders.completed"}'


def test_redelivering_the_same_orders_completed_does_not_create_a_second_notification(
    orders: OrdersApi,
    notifications: NotificationsApi,
    warehouse: Warehouse,
    broker: Broker,
    notifier_metrics: MetricsReader,
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
    delivered = warehouse.outbox_events("processor", order_id)[0]
    assert notifications.total(case.customer_id) == 1
    duplicates_before = notifier_metrics.value(DUPLICATES)
    dlq_before = broker.length("orders.completed.dlq")

    case.track(COMPLETED_STREAM, broker.publish(COMPLETED_STREAM, delivered.as_stream_fields()))

    wait_until(
        lambda: True if notifier_metrics.value(DUPLICATES) > duplicates_before else None,
        timeout=config.flow_timeout,
        description="notifier-consumer no conto la reentrega como duplicada",
    )
    wait_until(
        lambda: True if broker.pending(COMPLETED_STREAM, NOTIFIER_GROUP) == 0 else None,
        timeout=config.flow_timeout,
        description="la reentrega quedo sin acusar en la PEL",
    )

    assert notifications.total(case.customer_id) == 1
    assert broker.length("orders.completed.dlq") == dlq_before
