"""Explicit topic declaration.

The broker runs with `auto.create.topics.enable=false`, so a misspelt topic name fails instead of
silently creating a topic with the broker's default partition count. Topics are created only here,
by `make topics`; the API and the consumer never create them.
"""

import sys
import time
from dataclasses import dataclass
from typing import Literal

from confluent_kafka import KafkaError, KafkaException

# The documented import path. The package's stubs import NewTopic into `admin` without
# re-exporting it, so mypy flags the statement; the runtime import is fine.
from confluent_kafka.admin import (  # type: ignore[attr-defined]
    AdminClient,
    NewTopic,
    TopicMetadata,
)

from event_pipeline.broker.config import admin_config
from event_pipeline.config import Settings


@dataclass(frozen=True, slots=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int


class TopicMismatchError(Exception):
    """The topic exists, but not as declared. Changing it in place would move stores between
    partitions, so this is never repaired automatically."""


# Extra wait on top of the admin request timeout before giving up on librdkafka's answer.
_RESULT_GRACE_S = 1.0


def events_topic(settings: Settings) -> TopicSpec:
    return TopicSpec(
        name=settings.events_topic,
        partitions=settings.events_topic_partitions,
        replication_factor=settings.events_topic_replication_factor,
    )


def ensure_topic(
    admin: AdminClient, spec: TopicSpec, timeout_s: float
) -> Literal["created", "exists"]:
    """Create the topic, or check that the existing one matches the declaration.

    Raises `TopicMismatchError` if an existing topic differs, and `KafkaException` if the broker
    cannot be reached, refuses the request, or has no usable metadata for the topic in time.
    """
    future = admin.create_topics(
        [
            NewTopic(
                spec.name,
                num_partitions=spec.partitions,
                replication_factor=spec.replication_factor,
            )
        ],
        request_timeout=timeout_s,
        operation_timeout=timeout_s,
    )[spec.name]
    try:
        # librdkafka resolves the future itself when `request_timeout` expires, with an error that
        # names the cause (for example a refused connection); the grace period lets it do so.
        future.result(timeout=timeout_s + _RESULT_GRACE_S)
    except TimeoutError:
        # Only if librdkafka never answered. The request may still have reached the broker.
        raise KafkaException(
            KafkaError(
                KafkaError._TIMED_OUT,
                f"no answer within {timeout_s + _RESULT_GRACE_S} s; the topic may or may not "
                "have been created, rerun `make topics`",
            )
        ) from None
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
    else:
        return "created"

    _check_existing(admin, spec, timeout_s)
    return "exists"


# A just-created topic reaches the broker's metadata cache asynchronously, even on one node: against
# Kafka 4.3.2, 3 of 30 metadata requests made right after creation answered UNKNOWN_TOPIC_OR_PART.
_METADATA_NOT_YET_PROPAGATED = frozenset(
    {KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError.LEADER_NOT_AVAILABLE}
)
_METADATA_RETRY_INTERVAL_S = 0.1


def _check_existing(admin: AdminClient, spec: TopicSpec, timeout_s: float) -> None:
    topic = _wait_for_metadata(admin, spec.name, timeout_s)

    partitions = len(topic.partitions)
    if partitions != spec.partitions:
        raise TopicMismatchError(
            f"topic {spec.name} has {partitions} partitions, declared {spec.partitions}"
        )
    replication = {len(p.replicas) for p in topic.partitions.values()}
    if replication != {spec.replication_factor}:
        raise TopicMismatchError(
            f"topic {spec.name} has replication factor {sorted(replication)}, "
            f"declared {spec.replication_factor}"
        )


def _wait_for_metadata(admin: AdminClient, name: str, timeout_s: float) -> TopicMetadata:
    deadline = time.monotonic() + timeout_s
    while True:
        remaining = max(deadline - time.monotonic(), 0.001)
        topic = admin.list_topics(topic=name, timeout=remaining).topics.get(name)
        if topic is None:
            # Missing from the answer: treated like UNKNOWN_TOPIC_OR_PART.
            error = KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)
        elif topic.error is None:
            return topic
        else:
            error = topic.error
        if error.code() not in _METADATA_NOT_YET_PROPAGATED:
            raise KafkaException(error)
        if time.monotonic() + _METADATA_RETRY_INTERVAL_S >= deadline:
            # The broker just answered TOPIC_ALREADY_EXISTS; say so, or "unknown topic" reads as
            # a contradiction (a topic still being deleted can do this).
            raise KafkaException(
                KafkaError(
                    error.code(),
                    f"broker reported topic {name} as existing, but its metadata stayed "
                    f"{error.name()} for {timeout_s} s",
                )
            )
        time.sleep(_METADATA_RETRY_INTERVAL_S)


def main(settings: Settings) -> int:
    spec = events_topic(settings)
    admin = AdminClient(admin_config(settings))
    try:
        outcome = ensure_topic(admin, spec, settings.kafka_admin_timeout_s)
    except TopicMismatchError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KafkaException as exc:
        print(
            f"error: topic {spec.name} at {settings.kafka_bootstrap_servers}: "
            f"{exc.args[0].name()}: {exc.args[0].str()}",
            file=sys.stderr,
        )
        return 1
    print(f"topic {spec.name}: {outcome} ({spec.partitions} partitions)")
    return 0


if __name__ == "__main__":
    sys.exit(main(Settings()))
