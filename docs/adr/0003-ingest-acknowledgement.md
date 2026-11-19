# ADR 0003 — Ingest acknowledgement

- **Status:** Accepted
- **Date:** 2026-11-19

## Context

`POST /events` takes one event from a client and must answer honestly: `202 Accepted` only after
the broker acknowledged the write with `acks=all`, an error otherwise, and never a hang when the
broker is unavailable. ADR 0002 chose `confluent-kafka` with `acks=all`, idempotence,
`partitioner=murmur2_random` and an explicit `delivery.timeout.ms`, and left to this ADR how the
API waits for the delivery report, what it answers in each case, what the message looks like on
the topic, and how `received_at` travels with it (ADR 0001, decision 4).

The API is a FastAPI application, so routes can be asynchronous. `confluent-kafka` offers two ways
to wait for a delivery report from asyncio code:

- a synchronous `Producer`, whose delivery callbacks run only inside `poll()` or `flush()`, plus a
  thread that calls `poll()` and hands each result to the event loop;
- `confluent_kafka.aio.AIOProducer`, which returns an awaitable future per message.

### The `AIOProducer` check

Run on 2026-11-19 against the project's Compose broker (Kafka 4.3.2), `confluent-kafka` 2.16.2,
Python 3.12.15, with a throwaway script that was not committed. `AIOProducer` was created with the
producer configuration of ADR 0002 and its default batching (`batch_size=1000`,
`buffer_timeout=1.0`).

- **Headers:** `produce(..., headers=[...])` raised `NotImplementedError` ("Headers are not
  supported in AIOProducer batch mode"). The library's own source says to use the synchronous
  `Producer` when headers are needed.
- **Latency of a single message:** three sequential produces of one message each took 2.0 s from
  `produce()` to the delivery result. The producer buffers messages until `batch_size` is reached
  or a buffer timer expires, so a lone request waits for the timer. One run of three messages, not
  a measurement; smaller `buffer_timeout` values were not tried.

## Decision

1. **The API produces directly** to `orders.placed.v1`. There is no outbox and no database on the
   ingest path: the topic is the durable input log (architecture invariants), so the event exists
   once the broker acknowledged it.
2. **The message value is the client's raw request body**, unchanged, after `parse_event` accepted
   it. The consumer parses it again. The API does not re-serialise the parsed event, so what the
   client sent is exactly what is stored; comparisons in later phases (for example a replay)
   compare parsed events, not bytes.
3. **The message key is `store_id`**, UTF-8 encoded (ADR 0001, decision 5). The producer is built
   only by `producer_config()` (ADR 0002, decision 4). `delivery.timeout.ms` defaults to 5000.
4. **`received_at` is a Kafka header** named `received_at`, RFC 3339 in UTC with microseconds and
   `Z` (`2026-11-19T15:36:33.191000Z`). It is not a body field, so decision 2 holds and the client's
   bytes stay untouched. This closes ADR 0001, decision 4.
5. **Waiting:** the route is asynchronous. Each API process has one synchronous `Producer` and one
   poll thread (`broker/publisher.py`). `publish()` creates an asyncio future, produces with a
   delivery callback, and awaits the future. The callback runs on the poll thread and resolves the
   future with `loop.call_soon_threadsafe`, because asyncio futures are not thread-safe.
6. **Backstop:** the await is wrapped in `asyncio.wait_for` with `publish_backstop_s`, which the
   settings require to be at least `delivery.timeout.ms` + 2 s (librdkafka reports a timed-out
   message up to about a second late). librdkafka always reports within that time, so the backstop
   firing means a bug; it is logged at `ERROR` and answered as unconfirmed.
7. **Outcomes and HTTP answers** (`publish()` returns the outcome; the route maps it in slice 3):

   | Outcome | When | HTTP answer |
   |---------|------|-------------|
   | `Delivered(partition, offset)` | Delivery report without an error | `202` `{event_id}` |
   | — | Body fails `parse_event` | `422` `event_rejected`, violations `[{path, code}]` |
   | — | Body over 16 KiB (`Content-Length` or counted while reading, before parsing) | `413` `body_too_large` |
   | `broker_unavailable` | Report or `produce()` error with `_MSG_TIMED_OUT` or a retriable error; local queue full (`BufferError`) | `503` `broker_unavailable` |
   | `rejected` | Report or `produce()` error with a non-retriable error | `500` `internal_error` |
   | `publisher_failed` | Poll thread dead (detected before producing); backstop fired; report without an offset; error in the delivery callback | `500` `internal_error` |

   Log lines carry the Kafka error name (for example `_MSG_TIMED_OUT`), never the message.
8. **An answer other than `202` does not mean "not written".** A `503` or `500` means the write
   was not confirmed. The broker may still have stored the message (for example a timeout after the
   broker wrote it but before the acknowledgement arrived). The client retries with the same
   `event_id`, and the consumer's deduplication (phase 3) absorbs the copy.
9. **A dead poll thread is visible.** `is_polling()` turns false when the thread ends, every later
   `publish()` answers `publisher_failed` at once without producing, and `/ready` (slice 4) reports
   the process as not ready.
10. **Shutdown:** `close(timeout_s)` stops the poll thread and flushes for at most `timeout_s`,
    returning how many messages are still without a report. Those may or may not reach the broker;
    their requests have already ended. It is called only after the API has stopped taking requests
    (a request still in flight would wait for the backstop), and it blocks its caller, so the
    lifespan runs it in a worker thread (slice 4).
11. **A failure while handling one report stays with that report.** The delivery callback catches
    any exception, logs it, and answers that request `publisher_failed`, so the poll thread keeps
    serving other requests.

## Consequences

- The route reads top to bottom: validate, `await publish()`, map the outcome. The only thread
  hand-off is inside `broker/publisher.py`, which has unit tests for the two races it must survive:
  a report for a request that already ended, and a report after the event loop closed. Either one
  raising would kill the poll thread.
- The ingest latency is the broker round trip, not a batching timer. librdkafka still batches
  messages that arrive together (`linger.ms`, default 5 ms); no latency figure is claimed.
- A client sees `503` after about `delivery.timeout.ms` (+ about 1 s) when the broker is
  unreachable, not at once: librdkafka keeps retrying until then. The integration test asserts the
  bound for an unreachable bootstrap address.
- Each API process holds one producer and one extra thread. More processes mean more producers;
  idempotence is per producer, so it prevents duplicates from librdkafka's own retries, not from a
  client retrying through another process. That is the consumer's deduplication.
- The topic carries the client's bytes, so anything the client put in valid fields (for example a
  free-text `order_id`) is stored as sent. Validation (ADR 0001) bounds what those fields can be.

### Limitations

- A **fatal** producer error (possible with idempotence, for example after an unrecoverable
  sequence error) makes the producer unusable. It is answered `500` per request and logged once
  at `ERROR` by the producer's `error_cb` (non-fatal client errors at `WARNING`), but `/ready` does
  not detect it and the producer is not recreated. The fatal path itself is not tested: no way was
  found to trigger one against a single local broker.
- A **misspelt or missing topic** is not reported as such: librdkafka waits for the topic until
  `delivery.timeout.ms` and reports `_MSG_TIMED_OUT` (measured in phase 2 slice 1), so it looks
  like an outage (`503`). `/ready` checks that the topic exists (slice 4).
- An **unresponsive** broker (connection open, no answers) is tested only in the ADR 0002 spike, not
  in the test suite (deferred to phase 7, `PLAN.md` D5).

## Alternatives considered

- **`AIOProducer`.** Native awaitable futures and no poll thread to manage. Rejected on the check
  above: it does not support headers in its batch mode, which decision 4 needs, and its default
  buffer timer made each lone request wait about 2 s.
- **A synchronous route in FastAPI's thread pool**, calling `produce()` and then `flush()` or
  `poll()` until its own report arrives. Simple, but `flush()` waits for every outstanding message
  and blocks a pool thread per request; with concurrent requests one slow message would delay
  others' answers. Rejected.
- **`received_at` as a body field added by the server.** The API would have to re-serialise the
  event, so the stored bytes would no longer be the client's (decision 2), and the field would have
  to be both forbidden for clients and required on the topic. Rejected for a header.
- **Answering `202` before the delivery report** (fire and forget). Faster, but the API would report
  acceptance it did not observe, which breaks an architecture invariant. Rejected.
- **A transactional outbox** (write to PostgreSQL, relay to Kafka). Gives an acknowledgement
  without the broker, at the cost of a database on the ingest path and a relay process. Not needed:
  the broker's acknowledgement is the durable point in this design. Rejected for this project.
- **Kafka transactions.** Atomic writes across several messages or with consumer offsets; the
  ingest path writes one message per request, so there is nothing to make atomic. Rejected.
