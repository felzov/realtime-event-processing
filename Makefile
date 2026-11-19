.RECIPEPREFIX = >

.PHONY: install lint format typecheck test test-integration check check-all up down topics

# The broker from `make up`. Override for another broker; a direct `pytest -m integration` run
# without it fails instead of skipping.
TEST_KAFKA_BOOTSTRAP_SERVERS ?= localhost:9092
export TEST_KAFKA_BOOTSTRAP_SERVERS

install:
> uv sync

lint:
> uv run --locked ruff check .
> uv run --locked ruff format --check .

format:
> uv run --locked ruff format .
> uv run --locked ruff check --fix .

typecheck:
> uv run --locked mypy

# Unit tests; no database or broker needed.
test:
> uv run --locked pytest -m "not integration"

# Needs `make up`. Tests create their own uniquely named topics.
test-integration:
> uv run --locked pytest -m integration

# What CI runs on each Python version (CI also scans git history for secrets).
check: lint typecheck test

check-all: check test-integration

# Starts Kafka and waits until it is healthy.
up:
> docker compose up -d --wait

# Stops the containers and keeps the data volume. Deleting it (`down -v`) is a manual decision.
down:
> docker compose down

# Creates orders.placed.v1, or checks that the existing topic matches its declaration.
topics:
> uv run --locked python -m event_pipeline.broker.topics
