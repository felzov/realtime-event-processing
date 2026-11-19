# realtime-event-processing

A small real-time event pipeline: an ingest API writes versioned `order.placed` events to a broker
(Apache Kafka, [ADR 0002](docs/adr/0002-broker-and-client-stack.md)), a consumer group validates
and deduplicates them, and per-store, per-minute order counts and revenue are kept in PostgreSQL
behind a query API.

The goal is explicit, tested failure behaviour (duplicates, consumer restarts, offsets, late and
poison events), not feature breadth.

## Status

Phase 1 (contracts) in progress. Nothing described above is implemented yet.

- [Architecture](docs/architecture.md): target design, data flow, crash points, build order
- [Architecture decision records](docs/adr/README.md)

## Development

Needs [uv](https://docs.astral.sh/uv/) (it installs Python 3.12 from `.python-version`).

```sh
make install                   # uv sync
uv run pre-commit install      # optional: lint, typecheck and secret scan on each commit
make check                     # lint + typecheck + unit tests, as CI runs them
make format                    # apply ruff formatting and fixes
```

Integration tests need Docker:

```sh
make up                        # Kafka 4.3.2 on 127.0.0.1:9092, waits until healthy
make topics                    # declare orders.placed.v1 (6 partitions); safe to rerun
make test-integration          # tests against that broker, on their own test.* topics
make down                      # stop; the data volume is kept
```

Integration tests create their own `test.<id>` topics and do not delete them; they stay in the
Kafka data volume until it is removed.

The broker refuses to create topics on first use, so `make topics` must run before anything
writes to `orders.placed.v1`.
