"""Fixtures for tests against a real Kafka broker (`make up`).

Every test creates its own topics with a unique `test.` name, so tests never share state and never
touch `orders.placed.v1`.
"""

import os
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from event_pipeline.config import Settings

_HERE = Path(__file__).parent


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # A module-level `pytestmark` in a conftest is not applied to test modules, so the marker is
    # added here: every test in this directory is an integration test, and `make test` skips it.
    for item in items:
        if _HERE in item.path.parents:
            item.add_marker(pytest.mark.integration)


ADMIN_TIMEOUT_S = 10.0


@pytest.fixture(scope="session")
def kafka_bootstrap() -> str:
    # Fail, not skip: a CI job that silently skips its broker tests would look green.
    servers = os.environ.get("TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not servers:
        pytest.fail(
            "TEST_KAFKA_BOOTSTRAP_SERVERS is not set; run `make up` and `make test-integration`"
        )
    try:
        AdminClient({"bootstrap.servers": servers}).list_topics(timeout=ADMIN_TIMEOUT_S)
    except KafkaException as exc:
        pytest.fail(f"Kafka at {servers} did not answer: {exc.args[0].str()}")
    return servers


@pytest.fixture
def admin(kafka_bootstrap: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": kafka_bootstrap})


@pytest.fixture
def unique_topic() -> Callable[[], str]:
    return lambda: f"test.{uuid.uuid4().hex}"


@pytest.fixture
def settings_for(kafka_bootstrap: str) -> Callable[..., Settings]:
    def make(**overrides: Any) -> Settings:
        return Settings(  # type: ignore[call-arg]
            _env_file=None, kafka_bootstrap_servers=kafka_bootstrap, **overrides
        )

    return make
