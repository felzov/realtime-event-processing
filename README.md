# realtime-event-processing

A small real-time event pipeline: an ingest API writes versioned `order.placed` events to a broker
(Kafka, pending ADR 0002), a consumer group validates and deduplicates them, and per-store,
per-minute order counts and revenue are kept in PostgreSQL behind a query API.

The goal is explicit, tested failure behaviour (duplicates, consumer restarts, offsets, late and
poison events), not feature breadth.

## Status

Phase 1 (contracts) in progress. Nothing described above is implemented yet.
