from tests.support import (
    TRACE_ID_HEADER,
    Case,
    Config,
    NotificationsApi,
    OrdersApi,
    Warehouse,
    new_uuid,
    wait_until,
)


def test_full_flow_from_post_orders_to_the_notification_of_the_customer(
    orders: OrdersApi,
    notifications: NotificationsApi,
    warehouse: Warehouse,
    case: Case,
    config: Config,
) -> None:
    trace_id = new_uuid()

    created = orders.create(
        customer_id=case.customer_id,
        key=case.key("flow"),
        trace_id=trace_id,
        items=[{"name": "cortado", "qty": 2}],
    )

    assert created.status_code == 201, created.text
    body = created.json()
    order_id = body["order_id"]
    assert body["status"] == "PENDING"
    assert body["customer_id"] == case.customer_id
    assert created.headers[TRACE_ID_HEADER] == trace_id

    created_event = wait_until(
        lambda: next(iter(warehouse.outbox_events("orders", order_id)), None),
        timeout=config.flow_timeout,
        description="POST /orders no dejo la fila de outbox del pedido",
    )
    assert created_event.event_type == "orders.created"
    assert created_event.trace_id == trace_id

    published = wait_until(
        lambda: next(
            (row for row in warehouse.outbox_events("orders", order_id) if row.published_at),
            None,
        ),
        timeout=config.flow_timeout,
        description="el publicador no marco la fila de outbox como publicada",
    )
    assert published.event_id == created_event.event_id

    wait_until(
        lambda: True if orders.status_of(order_id) == "COMPLETED" else None,
        timeout=config.flow_timeout,
        description="processor-service no dejo el pedido en COMPLETED",
    )
    stored = warehouse.order(order_id)
    assert stored is not None
    assert stored.status == "COMPLETED"
    assert stored.completed_at is not None

    assert warehouse.processed_events(created_event.event_id) == 1

    completed_events = warehouse.outbox_events("processor", order_id)
    assert len(completed_events) == 1
    assert completed_events[0].event_type == "orders.completed"

    page = wait_until(
        lambda: notifications.items(case.customer_id) or None,
        timeout=config.flow_timeout,
        description="notifier-service no publico la notificacion del cliente",
    )
    assert len(page) == 1
    notification = page[0]
    assert notification["order_id"] == order_id
    assert notification["customer_id"] == case.customer_id
    assert notification["status"] == "COMPLETED"
    assert order_id in notification["message"]
    assert notification["event_id"] == completed_events[0].event_id

    assert notification["trace_id"] == trace_id
    assert completed_events[0].trace_id == trace_id


def test_a_customer_without_notifications_gets_an_empty_page_and_not_a_404(
    notifications: NotificationsApi, case: Case
) -> None:
    response = notifications.page(case.customer_id)

    assert response.status_code == 200
    assert response.json() == {"items": [], "total": 0, "limit": 50, "offset": 0}
