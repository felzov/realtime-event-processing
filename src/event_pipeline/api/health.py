"""`/health` (liveness) and `/ready` (readiness).

`/health` answers 200 whenever the process can serve HTTP; it checks nothing else, so a broker
outage never makes an orchestrator restart a healthy process. `/ready` answers 200 only when a
`POST /events` could succeed now: the producer's poll thread is alive, the broker answers and the
topic exists. Phase 3 adds the database and the migration revision.
"""

import logging
from typing import Literal

from fastapi import APIRouter, status
from pydantic import BaseModel

from event_pipeline.api.dependencies import AppReadiness
from event_pipeline.api.errors import ErrorResponse, NotReadyError

logger = logging.getLogger(__name__)

router = APIRouter()


class Healthy(BaseModel):
    status: Literal["ok"] = "ok"


class Ready(BaseModel):
    status: Literal["ready"] = "ready"


@router.get("/health", summary="Liveness: the process serves HTTP")
async def health() -> Healthy:
    return Healthy()


@router.get(
    "/ready",
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ErrorResponse}},
    summary="Readiness: events can be published now",
)
def ready(probe: AppReadiness) -> Ready:
    # A plain `def`: FastAPI runs it in its thread pool, because the metadata request blocks for
    # up to the ready timeout and must not block the event loop that serves `POST /events`.
    cause = probe.unready_cause()
    if cause is not None:
        # Probes repeat every few seconds, so an outage logs one line per probe.
        logger.warning("not ready: %s", cause)
        raise NotReadyError
    return Ready()
