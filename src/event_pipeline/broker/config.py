"""The only places Kafka client configuration is built (ADR 0002)."""

from event_pipeline.config import Settings

# The value types librdkafka accepts in a configuration dict.
type ClientConfig = dict[str, str | int | float | bool]


def producer_config(settings: Settings) -> ClientConfig:
    """Configuration for every producer that writes events.

    - `acks=all` and idempotence: a delivery report means the broker stored the message, and
      librdkafka's own retries neither duplicate nor reorder messages within a partition.
    - `murmur2_random`: keys land on the same partitions as with Kafka's Java tools. librdkafka's
      default partitioner maps them differently, which would break per-store order on a replay
      (ADR 0002, decision 3).
    """
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "partitioner": "murmur2_random",
        "delivery.timeout.ms": settings.kafka_delivery_timeout_ms,
    }


def admin_config(settings: Settings) -> ClientConfig:
    return {"bootstrap.servers": settings.kafka_bootstrap_servers}
