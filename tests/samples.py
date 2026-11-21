"""A valid `order.placed` v1 event for the API and integration tests."""

import json
from typing import Any

EVENT_ID = "6f1c0d2e-8a8b-4f7e-9a59-3c1f4f0b9a10"
STORE_ID = "store-7"


def valid_event() -> dict[str, Any]:
    return {
        "event_id": EVENT_ID,
        "event_type": "order.placed",
        "schema_version": 1,
        "occurred_at": "2026-11-18T12:00:30+02:00",
        "source": "web-shop.checkout",
        "payload": {
            "order_id": "ord-1001",
            "store_id": STORE_ID,
            "amount_minor": 2599,
            "currency": "EUR",
        },
    }


def encode(event: object) -> bytes:
    return json.dumps(event).encode()
