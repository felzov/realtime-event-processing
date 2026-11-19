"""Application factory.

The caller owns the publisher: it creates it before the app and closes it after. Slice 4 adds a
lifespan that builds the `EventPublisher` from settings and closes it on shutdown, and `make run`.
"""

from fastapi import FastAPI

from event_pipeline.api import events
from event_pipeline.api.errors import register_error_handlers
from event_pipeline.services.ingest import Publisher


def create_app(publisher: Publisher) -> FastAPI:
    app = FastAPI(title="realtime-event-processing-system")
    app.state.publisher = publisher
    register_error_handlers(app)
    app.include_router(events.router)
    return app
