"""A fake publisher and an in-process client for the API tests.

Requests go through httpx's `ASGITransport`, which does not run the app's lifespan: the app from
`create_app(publisher)` needs none.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx
from fastapi import FastAPI

from event_pipeline.broker.publisher import Delivered, Delivery

DELIVERED = Delivered(partition=3, offset=41)


@dataclass(frozen=True, slots=True)
class PublishCall:
    key: str
    value: bytes
    received_at: datetime


class FakePublisher:
    def __init__(
        self, outcome: Delivery | Exception = DELIVERED, unready_cause: str | None = None
    ) -> None:
        self.outcome = outcome
        self.calls: list[PublishCall] = []
        self.cause = unready_cause

    async def publish(self, *, key: str, value: bytes, received_at: datetime) -> Delivery:
        self.calls.append(PublishCall(key, value, received_at))
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome

    def unready_cause(self) -> str | None:
        return self.cause


def send(
    app: FastAPI,
    method: str = "POST",
    path: str = "/events",
    raise_app_exceptions: bool = True,
    **kwargs: Any,
) -> httpx.Response:
    async def request() -> httpx.Response:
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=raise_app_exceptions)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.request(method, path, **kwargs)

    return asyncio.run(request())


def error_of(response: httpx.Response) -> dict[str, Any]:
    body: dict[str, Any] = response.json()
    assert set(body) == {"error"}
    assert set(body["error"]) == {"code", "message", "violations"}
    error: dict[str, Any] = body["error"]
    return error
