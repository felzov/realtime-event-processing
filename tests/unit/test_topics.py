"""`ensure_topic` against a fake admin client, for the cases a real broker shows only by chance."""

import time
from collections.abc import Iterator
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any, cast

import pytest
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

from event_pipeline.broker.topics import TopicMismatchError, TopicSpec, ensure_topic

SPEC = TopicSpec("orders.placed.v1", partitions=6, replication_factor=1)


@dataclass
class FakePartition:
    replicas: list[int]


@dataclass
class FakeTopic:
    error: KafkaError | None
    partitions: dict[int, FakePartition] = field(default_factory=dict)


@dataclass
class FakeMetadata:
    topics: dict[str, FakeTopic]


def topic_ok(partitions: int = 6) -> FakeTopic:
    return FakeTopic(None, {p: FakePartition([1]) for p in range(partitions)})


def topic_error(code: int) -> FakeTopic:
    return FakeTopic(KafkaError(code))


# What the fake's create_topics future does: resolve, never resolve, or fail with a code.
CREATED, HANG = "created", "hang"


class FakeAdmin:
    """create_topics answers `create` (TOPIC_ALREADY_EXISTS by default); list_topics replays
    `answers`."""

    def __init__(
        self, answers: list[FakeTopic | None], create: str | int = KafkaError.TOPIC_ALREADY_EXISTS
    ) -> None:
        self._answers: Iterator[FakeTopic | None] = iter(answers)
        self._create = create
        self.metadata_calls = 0

    def create_topics(self, new_topics: list[Any], **_: Any) -> dict[str, Future[None]]:
        future: Future[None] = Future()
        if isinstance(self._create, int):
            future.set_exception(KafkaException(KafkaError(self._create)))
        elif self._create == CREATED:
            future.set_result(None)
        return {new_topics[0].topic: future}

    def list_topics(self, topic: str, timeout: float) -> FakeMetadata:
        self.metadata_calls += 1
        answer = next(self._answers)
        return FakeMetadata({} if answer is None else {topic: answer})


def as_admin(fake: FakeAdmin) -> AdminClient:
    return cast(AdminClient, fake)


@pytest.mark.parametrize(
    "transient",
    [KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError.LEADER_NOT_AVAILABLE],
    ids=["unknown_topic", "leader_not_available"],
)
def test_metadata_not_yet_propagated_is_retried(transient: int) -> None:
    # Seen against Kafka 4.3.2: right after creation, 3 of 30 metadata requests for the new
    # topic answered UNKNOWN_TOPIC_OR_PART.
    fake = FakeAdmin([topic_error(transient), topic_error(transient), topic_ok()])

    assert ensure_topic(as_admin(fake), SPEC, timeout_s=5.0) == "exists"
    assert fake.metadata_calls == 3


def test_topic_missing_from_the_metadata_answer_is_retried() -> None:
    fake = FakeAdmin([None, topic_ok()])

    assert ensure_topic(as_admin(fake), SPEC, timeout_s=5.0) == "exists"
    assert fake.metadata_calls == 2


def test_new_topic_is_created_without_a_metadata_check() -> None:
    fake = FakeAdmin([], create=CREATED)

    assert ensure_topic(as_admin(fake), SPEC, timeout_s=5.0) == "created"
    assert fake.metadata_calls == 0


def test_create_error_other_than_exists_is_raised_without_a_metadata_check() -> None:
    fake = FakeAdmin([topic_ok()], create=KafkaError.TOPIC_AUTHORIZATION_FAILED)

    with pytest.raises(KafkaException) as exc_info:
        ensure_topic(as_admin(fake), SPEC, timeout_s=5.0)
    assert exc_info.value.args[0].code() == KafkaError.TOPIC_AUTHORIZATION_FAILED
    assert fake.metadata_calls == 0


def test_create_with_no_answer_times_out_and_says_the_outcome_is_unknown() -> None:
    fake = FakeAdmin([], create=HANG)
    started = time.monotonic()

    with pytest.raises(KafkaException) as exc_info:
        ensure_topic(as_admin(fake), SPEC, timeout_s=0.2)

    err = exc_info.value.args[0]
    assert err.code() == KafkaError._TIMED_OUT
    assert "may or may not have been created" in err.str()
    # Waits a grace period past the request timeout, so librdkafka's own error normally wins.
    assert 1.1 <= time.monotonic() - started < 2.0


def test_metadata_that_never_settles_names_the_contradiction() -> None:
    fake = FakeAdmin([topic_error(KafkaError.UNKNOWN_TOPIC_OR_PART)] * 1000)

    with pytest.raises(KafkaException) as exc_info:
        ensure_topic(as_admin(fake), SPEC, timeout_s=0.3)
    err = exc_info.value.args[0]
    assert err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART
    assert "as existing, but its metadata stayed UNKNOWN_TOPIC_OR_PART" in err.str()
    assert 2 <= fake.metadata_calls <= 4  # retried until the deadline, not once and not forever


def test_other_metadata_errors_are_not_retried() -> None:
    fake = FakeAdmin([topic_error(KafkaError.TOPIC_AUTHORIZATION_FAILED), topic_ok()])

    with pytest.raises(KafkaException) as exc_info:
        ensure_topic(as_admin(fake), SPEC, timeout_s=5.0)
    assert exc_info.value.args[0].code() == KafkaError.TOPIC_AUTHORIZATION_FAILED
    assert fake.metadata_calls == 1


def test_partition_count_mismatch_is_reported() -> None:
    fake = FakeAdmin([topic_ok(partitions=3)])

    with pytest.raises(TopicMismatchError, match="has 3 partitions, declared 6"):
        ensure_topic(as_admin(fake), SPEC, timeout_s=5.0)


def test_replication_factor_mismatch_is_reported() -> None:
    topic = topic_ok()
    topic.partitions[0] = FakePartition([1, 2])
    fake = FakeAdmin([topic])

    with pytest.raises(TopicMismatchError, match=r"replication factor \[1, 2\], declared 1"):
        ensure_topic(as_admin(fake), SPEC, timeout_s=5.0)
