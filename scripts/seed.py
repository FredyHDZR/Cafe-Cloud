#!/usr/bin/env python3
#
# Datos de ejemplo. Los pedidos se crean por `POST /orders`, nunca por INSERT directo, para que el
# flujo entero se dispare solo: outbox, publicador, processor-service y notifier-service. El seed
# los deja en PENDING y espera a que el sistema los complete, en vez de escribir el estado final.
#
#   make seed
#
# Salida: 0 todas las notificaciones entregadas, 1 error de la API, 2 tiempo agotado esperando.

import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from typing import Any

ORDERS_URL = os.environ.get("ORDERS_URL", "http://orders-service:8000").rstrip("/")
NOTIFIER_URL = os.environ.get("NOTIFIER_URL", "http://notifier-service:8000").rstrip("/")
CUSTOMER_ID = os.environ.get("SEED_CUSTOMER_ID", "cafe-demo")
ORDER_COUNT = int(os.environ.get("SEED_ORDERS", "3"))
TIMEOUT_SECONDS = float(os.environ.get("SEED_TIMEOUT_SECONDS", "60"))

BASKETS: list[list[dict[str, Any]]] = [
    [{"name": "Cortado", "qty": 1}, {"name": "Croissant", "qty": 2}],
    [{"name": "Flat white", "qty": 2}],
    [
        {"name": "Espresso doble", "qty": 1},
        {"name": "Tostada", "qty": 1},
        {"name": "Zumo", "qty": 1},
    ],
]


def request(method: str, url: str, body: dict[str, Any] | None, headers: dict[str, str]) -> Any:
    payload = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=payload, method=method, headers=headers)
    if payload is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read())


def wait_until_ready() -> None:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    for name, base in (("orders-service", ORDERS_URL), ("notifier-service", NOTIFIER_URL)):
        while True:
            try:
                request("GET", f"{base}/health/ready", None, {})
                break
            except (urllib.error.URLError, OSError, ValueError) as error:
                if time.monotonic() >= deadline:
                    print(f"{name} no llego a estar listo: {error}", file=sys.stderr)
                    raise SystemExit(2) from error
                time.sleep(0.5)


def create_orders() -> list[str]:
    created = []
    for index in range(ORDER_COUNT):
        items = BASKETS[index % len(BASKETS)]
        headers = {"Idempotency-Key": str(uuid.uuid4())}
        try:
            body = {"customer_id": CUSTOMER_ID, "items": items}
            order = request("POST", f"{ORDERS_URL}/orders", body, headers)
        except urllib.error.HTTPError as error:
            print(f"POST /orders devolvio {error.code}: {error.read().decode()}", file=sys.stderr)
            raise SystemExit(1) from error
        created.append(order["order_id"])
        names = ", ".join(f"{item['qty']}x {item['name']}" for item in items)
        print(f"  {order['order_id']}  {order['status']}  {names}")
    return created


def wait_for_notifications(expected: int) -> int:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    url = f"{NOTIFIER_URL}/notifications/{CUSTOMER_ID}?limit=200"
    while True:
        total = request("GET", url, None, {})["total"]
        if total >= expected or time.monotonic() >= deadline:
            return total
        time.sleep(0.5)


def main() -> int:
    print(f"seed: {ORDER_COUNT} pedidos para el cliente '{CUSTOMER_ID}'")
    wait_until_ready()

    page = request("GET", f"{NOTIFIER_URL}/notifications/{CUSTOMER_ID}?limit=1", None, {})
    before = page["total"]
    create_orders()

    print("esperando a que el sistema los complete y notifique...")
    total = wait_for_notifications(before + ORDER_COUNT)
    delivered = total - before
    print(f"notificaciones de '{CUSTOMER_ID}': {total} ({delivered} de esta pasada)")
    if delivered < ORDER_COUNT:
        print("tiempo agotado: faltan notificaciones por llegar", file=sys.stderr)
        return 2

    print(f"\n  curl -s localhost:8003/notifications/{CUSTOMER_ID} | jq")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
