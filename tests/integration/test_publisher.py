import asyncio
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime

import pytest
from confluent_kafka.admin import AdminClient

from event_pipeline.broker.publisher import (
    RECEIVED_AT_HEADER,
    Delivered,
    EventPublisher,
    NotDelivered,
    PublishFailure,
)
from event_pipeline.broker.topics import TopicSpec, ensure_topic
from event_pipeline.config import Settings
from tests.integration.kafka_helpers import read_at

TIMEOUT_S = 10.0
CLOSE_TIMEOUT_S = 5.0
RECEIVED_AT = datetime(2026, 11, 19, 15, 36, 33, 191000, tzinfo=UTC)


@pytest.fixture
def topic(admin: AdminClient, unique_topic: Callable[[], str]) -> str:
    spec = TopicSpec(unique_topic(), partitions=3, replication_factor=1)
    ensure_topic(admin, spec, TIMEOUT_S)
    return spec.name


@pytest.fixture
def publisher(topic: str, settings_for: Callable[..., Settings]) -> Iterator[EventPublisher]:
    publisher = EventPublisher.from_settings(settings_for(events_topic=topic))
    yield publisher
    publisher.close(CLOSE_TIMEOUT_S)


def test_delivered_message_is_on_the_topic_as_reported(
    publisher: EventPublisher, topic: str, kafka_bootstrap: str
) -> None:
    value = b'{"any": "bytes, stored as sent"}'

    outcome = asyncio.run(publisher.publish(key="store-1", value=value, received_at=RECEIVED_AT))

    assert isinstance(outcome, Delivered)
    message = read_at(kafka_bootstrap, topic, outcome.partition, outcome.offset)
    assert message.key() == b"store-1"
    assert message.value() == value
    assert message.headers() == [(RECEIVED_AT_HEADER, b"2026-11-19T15:36:33.191000Z")]


def test_concurrent_publishes_each_get_their_own_report(
    publisher: EventPublisher, topic: str, kafka_bootstrap: str
) -> None:
    # Every request waits on its own future; a mix-up between callbacks would show as a value
    # read back from another request's position.
    values = [f'{{"n": {n}}}'.encode() for n in range(20)]

    async def publish_all() -> list[Delivered | NotDelivered]:
        return await asyncio.gather(
            *(
                publisher.publish(key=f"store-{n % 5}", value=v, received_at=RECEIVED_AT)
                for n, v in enumerate(values)
            )
        )

    outcomes = asyncio.run(publish_all())

    delivered = [o for o in outcomes if isinstance(o, Delivered)]
    assert len(delivered) == len(values)
    assert len({(d.partition, d.offset) for d in delivered}) == len(values)
    for value, d in zip(values, delivered, strict=True):
        assert read_at(kafka_bootstrap, topic, d.partition, d.offset).value() == value


def test_same_key_lands_on_the_same_partition(publisher: EventPublisher) -> None:
    async def publish_twice() -> list[Delivered | NotDelivered]:
        return [
            await publisher.publish(key="store-7", value=b"{}", received_at=RECEIVED_AT)
            for _ in range(2)
        ]

    first, second = asyncio.run(publish_twice())

    assert isinstance(first, Delivered)
    assert isinstance(second, Delivered)
    assert first.partition == second.partition
    assert second.offset == first.offset + 1


def test_unreachable_broker_answers_broker_unavailable_in_bounded_time(
    unique_topic: Callable[[], str],
) -> None:
    # Port 1 on 127.0.0.1: nothing listens, so every connection is refused.
    settings = Settings(  # type: ignore[call-arg]
        _env_file=None,
        kafka_bootstrap_servers="127.0.0.1:1",
        events_topic=unique_topic(),
        kafka_delivery_timeout_ms=2000,
        # Well past the bound asserted below, so a slow report still counts as librdkafka's.
        publish_backstop_s=6.0,
    )
    publisher = EventPublisher.from_settings(settings)
    try:
        started = time.monotonic()
        outcome = asyncio.run(publisher.publish(key="s", value=b"{}", received_at=RECEIVED_AT))
        elapsed = time.monotonic() - started
    finally:
        publisher.close(timeout_s=1.0)

    # librdkafka's report, not the backstop: delivery.timeout.ms plus its ~1 s check interval.
    assert outcome == NotDelivered(PublishFailure.BROKER_UNAVAILABLE, "_MSG_TIMED_OUT")
    assert elapsed < settings.kafka_delivery_timeout_ms / 1000 + 2.5


def test_publisher_is_ready_when_its_topic_exists(publisher: EventPublisher) -> None:
    assert publisher.unready_cause() is None


def test_missing_topic_makes_the_publisher_unready(
    settings_for: Callable[..., Settings], unique_topic: Callable[[], str]
) -> None:
    # Never created: the broker refuses auto-creation and says the topic is unknown.
    publisher = EventPublisher.from_settings(settings_for(events_topic=unique_topic()))
    try:
        cause = publisher.unready_cause()
    finally:
        publisher.close(CLOSE_TIMEOUT_S)

    assert cause == "UNKNOWN_TOPIC_OR_PART"
