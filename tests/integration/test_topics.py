import time
from collections.abc import Callable

import pytest
from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient

from event_pipeline.broker.config import producer_config
from event_pipeline.broker.topics import TopicMismatchError, TopicSpec, ensure_topic, main
from event_pipeline.config import Settings

TIMEOUT_S = 10.0


def partition_count(admin: AdminClient, name: str) -> int:
    return len(admin.list_topics(topic=name, timeout=TIMEOUT_S).topics[name].partitions)


def test_creates_a_missing_topic_with_the_declared_partitions(
    admin: AdminClient, unique_topic: Callable[[], str]
) -> None:
    spec = TopicSpec(unique_topic(), partitions=6, replication_factor=1)

    assert ensure_topic(admin, spec, TIMEOUT_S) == "created"
    assert partition_count(admin, spec.name) == 6


def test_second_run_finds_the_topic_and_changes_nothing(
    admin: AdminClient, unique_topic: Callable[[], str]
) -> None:
    spec = TopicSpec(unique_topic(), partitions=3, replication_factor=1)
    ensure_topic(admin, spec, TIMEOUT_S)

    assert ensure_topic(admin, spec, TIMEOUT_S) == "exists"
    assert partition_count(admin, spec.name) == 3


def test_existing_topic_with_other_partition_count_fails_loudly(
    admin: AdminClient, unique_topic: Callable[[], str]
) -> None:
    name = unique_topic()
    ensure_topic(admin, TopicSpec(name, partitions=3, replication_factor=1), TIMEOUT_S)

    with pytest.raises(TopicMismatchError, match="has 3 partitions, declared 6"):
        ensure_topic(admin, TopicSpec(name, partitions=6, replication_factor=1), TIMEOUT_S)
    assert partition_count(admin, name) == 3


def test_replication_factor_above_broker_count_is_refused(
    admin: AdminClient, unique_topic: Callable[[], str]
) -> None:
    # One broker cannot hold 3 replicas; the broker's refusal is raised, not swallowed.
    with pytest.raises(KafkaException) as exc_info:
        ensure_topic(admin, TopicSpec(unique_topic(), 1, replication_factor=3), TIMEOUT_S)
    assert exc_info.value.args[0].code() == KafkaError.INVALID_REPLICATION_FACTOR


def test_producing_to_an_undeclared_topic_does_not_create_it(
    admin: AdminClient,
    unique_topic: Callable[[], str],
    settings_for: Callable[..., Settings],
) -> None:
    name = unique_topic()
    reports: list[KafkaError | None] = []

    def on_delivery(err: KafkaError | None, _msg: Message) -> None:
        reports.append(err)

    producer = Producer(producer_config(settings_for(kafka_delivery_timeout_ms=3000)))
    producer.produce(name, key=b"store-1", value=b"{}", on_delivery=on_delivery)
    producer.flush(TIMEOUT_S)

    assert len(reports) == 1
    # Measured: librdkafka waits for the topic until delivery.timeout.ms and then times the message
    # out; it does not report "unknown topic". A misspelt topic therefore looks like an outage.
    assert reports[0] is not None
    assert reports[0].code() == KafkaError._MSG_TIMED_OUT
    assert name not in admin.list_topics(timeout=TIMEOUT_S).topics


def test_make_topics_entry_point_creates_then_finds_the_topic(
    unique_topic: Callable[[], str],
    settings_for: Callable[..., Settings],
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = settings_for(events_topic=unique_topic(), events_topic_partitions=2)

    assert main(settings) == 0
    assert main(settings) == 0

    out = capsys.readouterr().out.splitlines()
    assert out == [
        f"topic {settings.events_topic}: created (2 partitions)",
        f"topic {settings.events_topic}: exists (2 partitions)",
    ]


def test_make_topics_entry_point_exits_1_on_mismatch(
    unique_topic: Callable[[], str],
    settings_for: Callable[..., Settings],
    capsys: pytest.CaptureFixture[str],
) -> None:
    name = unique_topic()
    assert main(settings_for(events_topic=name, events_topic_partitions=2)) == 0

    assert main(settings_for(events_topic=name, events_topic_partitions=4)) == 1
    assert "has 2 partitions, declared 4" in capsys.readouterr().err


def test_make_topics_entry_point_exits_1_when_kafka_is_unreachable(
    unique_topic: Callable[[], str], capsys: pytest.CaptureFixture[str]
) -> None:
    # Port 1 on localhost: nothing listens, so every connection is refused.
    settings = Settings(  # type: ignore[call-arg]
        _env_file=None,
        kafka_bootstrap_servers="127.0.0.1:1",
        events_topic=unique_topic(),
        kafka_admin_timeout_s=2.0,
    )

    started = time.monotonic()

    assert main(settings) == 1
    # Bounded: the admin timeout plus librdkafka's own answer, not a hang.
    assert time.monotonic() - started < settings.kafka_admin_timeout_s + 2.0
    err = capsys.readouterr().err
    assert "127.0.0.1:1" in err
    assert "_TRANSPORT" in err or "_TIMED_OUT" in err
