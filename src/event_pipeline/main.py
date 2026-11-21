"""Application factories.

`create_app(publisher)`: the caller owns the publisher (it creates it before the app and closes it
after). The tests use it with a fake or a real publisher.

`create_app_from_settings()`: what `make run` serves (`uvicorn --factory`). Its lifespan creates
the `EventPublisher` from settings on startup and closes it on shutdown, after the server has
stopped taking requests and the ones in flight have finished (ADR 0003, decision 10).
"""

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Protocol

from fastapi import FastAPI

from event_pipeline.api import events, health
from event_pipeline.api.dependencies import ReadinessProbe
from event_pipeline.api.errors import register_error_handlers
from event_pipeline.broker.publisher import EventPublisher
from event_pipeline.config import Settings
from event_pipeline.services.ingest import Publisher

type Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]


class PublisherWithReadiness(Publisher, ReadinessProbe, Protocol):
    """What the routes need: `POST /events` publishes, `/ready` asks whether it could."""


def create_app(publisher: PublisherWithReadiness) -> FastAPI:
    app = _new_app(lifespan=None)
    app.state.publisher = publisher
    return app


def create_app_from_settings(settings: Settings | None = None) -> FastAPI:
    # Read when uvicorn calls the factory, not at import, so a bad setting fails the start.
    resolved = settings if settings is not None else Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Creating the producer does not connect: the app starts even if the broker is down, and
        # `/ready` answers 503 until it is reachable.
        publisher = EventPublisher.from_settings(resolved)
        app.state.publisher = publisher
        try:
            yield
        finally:
            # `close()` blocks for up to the flush timeout; a worker thread keeps the loop free.
            await asyncio.to_thread(publisher.close, resolved.publisher_close_timeout_s)

    return _new_app(lifespan)


def _new_app(lifespan: Lifespan | None) -> FastAPI:
    app = FastAPI(title="realtime-event-processing-system", lifespan=lifespan)
    register_error_handlers(app)
    app.include_router(events.router)
    app.include_router(health.router)
    return app
