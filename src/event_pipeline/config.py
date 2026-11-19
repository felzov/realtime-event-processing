"""Settings from environment variables, or from a local `.env` file.

All limits and timeouts are explicit here. The defaults are placeholders for a local single-broker
setup, not measured values.
"""

from typing import Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# librdkafka checks message timeouts about once a second, so `_MSG_TIMED_OUT` can arrive up to about
# a second after `delivery.timeout.ms` (5.6 s for 5000 ms in the ADR 0002 spike); waiting for the
# poll that delivers the report adds a little more.
BACKSTOP_MARGIN_MS = 2_000


class Settings(BaseSettings):
    # Unknown keys in `.env` are ignored. Validation errors would otherwise print the values read,
    # so they are hidden; there are no secrets yet, but the database password arrives in phase 3.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    # Comma-separated host:port list. Plaintext and unauthenticated: local development only.
    # 127.0.0.1 rather than localhost, matching the published port (compose.yaml).
    kafka_bootstrap_servers: str = Field(default="127.0.0.1:9092", min_length=1)

    # Kafka allows up to 249 characters from this set. The partition count and the replication
    # factor are part of the topic's contract: changing the count moves stores to other
    # partitions, so it means a new `.v2` topic (ADR 0001, decision 6).
    events_topic: str = Field(
        default="orders.placed.v1", min_length=1, max_length=249, pattern=r"^[A-Za-z0-9._-]+$"
    )
    events_topic_partitions: int = Field(default=6, ge=1)
    # 1 because there is one local broker; a real deployment needs at least 3 (ADR 0002).
    events_topic_replication_factor: int = Field(default=1, ge=1)

    # Bounds topic creation and, separately, the metadata wait that checks an existing topic, so
    # `make topics` can take about twice this in the worst case.
    kafka_admin_timeout_s: float = Field(default=10.0, gt=0)

    # librdkafka's limit for one message, retries included, after which the delivery report
    # carries `_MSG_TIMED_OUT` (ADR 0002, decision 4). The ingest API answers 503 then.
    kafka_delivery_timeout_ms: int = Field(default=5_000, gt=0)
    # Outer limit on waiting for a delivery report. Only a bug (a report that never arrives)
    # should reach it, so it must outlast the delivery timeout by BACKSTOP_MARGIN_MS.
    publish_backstop_s: float = Field(default=10.0, gt=0)

    @model_validator(mode="after")
    def _backstop_outlasts_delivery_timeout(self) -> Self:
        # Otherwise the backstop would fire before librdkafka reports, turning an ordinary
        # broker timeout into the "should never happen" path.
        if self.publish_backstop_s * 1000 < self.kafka_delivery_timeout_ms + BACKSTOP_MARGIN_MS:
            raise ValueError(
                "publish_backstop_s must be longer than kafka_delivery_timeout_ms by at least "
                f"{BACKSTOP_MARGIN_MS} ms"
            )
        return self
