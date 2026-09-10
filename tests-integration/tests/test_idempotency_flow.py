from tests.support import (
    IDEMPOTENCY_REPLAYED_HEADER,
    Case,
    Config,
    NotificationsApi,
    OrdersApi,
    Warehouse,
    new_uuid,
    wait_until,
)


def test_two_requests_with_the_same_idempotency_key_create_one_order(
    orders: OrdersApi,
    notifications: NotificationsApi,
    warehouse: Warehouse,
    case: Case,
    config: Config,
) -> None:
    key = case.key("twice")
    items = [{"name": "espresso", "qty": 1}]

    first = orders.create(customer_id=case.customer_id, key=key, trace_id=new_uuid(), items=items)
    second = orders.create(customer_id=case.customer_id, key=key, trace_id=new_uuid(), items=items)

    assert first.status_code == 201, first.text
    assert second.status_code == 201, second.text
    assert IDEMPOTENCY_REPLAYED_HEADER not in first.headers
    assert second.headers[IDEMPOTENCY_REPLAYED_HEADER] == "true"

    order_id = first.json()["order_id"]
    assert second.json() == first.json()
    assert warehouse.idempotency_rows(key) == [("completed", order_id)]

    assert len(warehouse.outbox_events("orders", order_id)) == 1

    items_seen = wait_until(
        lambda: notifications.items(case.customer_id) or None,
        timeout=config.flow_timeout,
        description="la notificacion del pedido idempotente no aparecio",
    )

    assert len(items_seen) == 1
    assert notifications.total(case.customer_id) == 1
    assert len(warehouse.outbox_events("processor", order_id)) == 1


def test_the_same_key_with_a_different_body_is_rejected_as_a_conflict(
    orders: OrdersApi, warehouse: Warehouse, case: Case
) -> None:
    key = case.key("reuse")

    first = orders.create(
        customer_id=case.customer_id,
        key=key,
        trace_id=new_uuid(),
        items=[{"name": "latte", "qty": 1}],
    )
    reused = orders.create(
        customer_id=case.customer_id,
        key=key,
        trace_id=new_uuid(),
        items=[{"name": "latte", "qty": 9}],
    )

    assert first.status_code == 201, first.text
    assert reused.status_code == 409
    assert reused.json()["error"]["code"] == "idempotency_key_reuse"

    order_id = first.json()["order_id"]
    assert warehouse.idempotency_rows(key) == [("completed", order_id)]
