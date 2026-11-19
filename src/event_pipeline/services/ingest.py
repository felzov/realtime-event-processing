"""Ingest one event: validate it, stamp `received_at`, publish it and report the broker's answer.

ADR 0003: the value on the topic is the client's raw body, unchanged; the key is `store_id`;
`received_at` travels as a header and is set here, never taken from the client (ADR 0001,
decision 4). Success means the broker acknowledged the write; anything else raises.
"""

import logging
from datetime import UTC, datetime
from typing import Protocol, assert_never
from uuid import UUID

from event_pipeline.broker.publisher import Delivered, Delivery, NotDelivered, PublishFailure
from event_pipeline.domain.events import parse_event

logger = logging.getLogger(__name__)


class Publisher(Protocol):
    """What ingest needs from `broker.publisher.EventPublisher`; the API tests pass a fake."""

    async def publish(self, *, key: str, value: bytes, received_at: datetime) -> Delivery: ...


class EventNotConfirmedError(Exception):
    """The broker did not confirm the write. It may still have stored it (ADR 0003, decision 8)."""

    def __init__(self, outcome: NotDelivered) -> None:
        super().__init__(f"{outcome.failure}: {outcome.cause}")
        self.failure = outcome.failure
        self.cause = outcome.cause


async def ingest_event(raw: bytes, publisher: Publisher) -> UUID:
    """Publish `raw` if it is a valid event and return its `event_id` once the broker acknowledged.

    Raises `EventRejectedError` (nothing is published) or `EventNotConfirmedError`.
    """
    # Before parsing, so it is as close to receipt as possible; a rejected event discards it.
    received_at = datetime.now(UTC)
    event = parse_event(raw)

    outcome = await publisher.publish(
        key=event.payload.store_id, value=raw, received_at=received_at
    )
    # Success is matched explicitly, never as "not a failure": the API must not answer 202 for an
    # outcome it did not recognise (architecture invariants). A new `Delivery` variant fails mypy
    # here; a wrong type at runtime raises, which the API answers with 500.
    match outcome:
        case Delivered():
            return event.event_id
        case NotDelivered():
            # The event_id is client-generated but validated as a UUID, so it is safe to log and
            # lets an operator match a client's retry. The cause is a Kafka error name, never the
            # message.
            level = (
                logging.WARNING
                if outcome.failure is PublishFailure.BROKER_UNAVAILABLE
                else logging.ERROR
            )
            logger.log(
                level,
                "event %s not confirmed by the broker: %s (%s)",
                event.event_id,
                outcome.failure,
                outcome.cause,
            )
            raise EventNotConfirmedError(outcome)
        case _:
            assert_never(outcome)
