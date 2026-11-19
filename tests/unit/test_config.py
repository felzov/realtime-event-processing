from typing import Any

import pytest
from pydantic import ValidationError

from event_pipeline.broker.config import producer_config
from event_pipeline.broker.topics import TopicSpec, events_topic
from event_pipeline.config import Settings


def make_settings(**overrides: Any) -> Settings:
    # No `.env` file, so a developer's local file cannot change what the test sees.
    return Settings(_env_file=None, **overrides)  # type: ignore[call-arg]


def test_defaults_describe_the_local_topic() -> None:
    assert events_topic(make_settings()) == TopicSpec("orders.placed.v1", 6, 1)


def test_settings_come_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092")
    monkeypatch.setenv("KAFKA_DELIVERY_TIMEOUT_MS", "2000")

    settings = make_settings()

    assert settings.kafka_bootstrap_servers == "kafka-1:9092,kafka-2:9092"
    assert settings.kafka_delivery_timeout_ms == 2000


def test_producer_config_sets_the_adr_0002_guarantees() -> None:
    config = producer_config(make_settings(kafka_delivery_timeout_ms=4000))

    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert config["partitioner"] == "murmur2_random"
    assert config["delivery.timeout.ms"] == 4000
    assert config["bootstrap.servers"] == "localhost:9092"


@pytest.mark.parametrize(
    ("delivery_ms", "backstop_s"),
    [(5000, 5.0), (5000, 4.9), (5000, 6.999)],
    ids=["equal", "shorter", "under_2s_margin"],
)
def test_backstop_must_outlast_the_delivery_timeout(delivery_ms: int, backstop_s: float) -> None:
    with pytest.raises(ValidationError, match="publish_backstop_s must be longer"):
        make_settings(kafka_delivery_timeout_ms=delivery_ms, publish_backstop_s=backstop_s)


def test_backstop_with_2s_margin_is_accepted() -> None:
    # librdkafka reports _MSG_TIMED_OUT up to about a second after delivery.timeout.ms.
    settings = make_settings(kafka_delivery_timeout_ms=5000, publish_backstop_s=7.0)

    assert settings.publish_backstop_s == 7.0


@pytest.mark.parametrize(
    "overrides",
    [
        {"events_topic": "orders placed"},
        {"events_topic": ""},
        {"events_topic": "x" * 250},
        {"events_topic_partitions": 0},
        {"events_topic_replication_factor": 0},
        {"kafka_delivery_timeout_ms": 0},
        {"kafka_admin_timeout_s": 0},
        {"kafka_bootstrap_servers": ""},
    ],
    ids=[
        "topic_with_space",
        "topic_empty",
        "topic_too_long",
        "zero_partitions",
        "zero_replication",
        "zero_delivery_timeout",
        "zero_admin_timeout",
        "empty_bootstrap",
    ],
)
def test_invalid_settings_are_rejected(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        make_settings(**overrides)


def test_validation_errors_do_not_echo_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_DELIVERY_TIMEOUT_MS", "not-a-number-SECRET")

    with pytest.raises(ValidationError) as exc_info:
        make_settings()

    assert "SECRET" not in str(exc_info.value)
