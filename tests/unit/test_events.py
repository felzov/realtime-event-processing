"""Acceptance criteria 1 and 2 (PLAN.md, phase 1): parsing and rejecting events."""

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from event_pipeline.domain.errors import ROOT_PATH, EventRejectedError, ReasonCode, Violation
from event_pipeline.domain.events import (
    MAX_AMOUNT_MINOR,
    MAX_BODY_BYTES,
    OrderPlacedEvent,
    parse_event,
)
from event_pipeline.domain.windows import window_start

EVENT_ID = "6f1c0d2e-8a8b-4f7e-9a59-3c1f4f0b9a10"


def valid_event() -> dict[str, Any]:
    return {
        "event_id": EVENT_ID,
        "event_type": "order.placed",
        "schema_version": 1,
        "occurred_at": "2026-11-18T12:00:30+02:00",
        "source": "web-shop.checkout",
        "payload": {
            "order_id": "ord-1001",
            "store_id": "store-7",
            "amount_minor": 2599,
            "currency": "EUR",
        },
    }


def encode(event: object) -> bytes:
    return json.dumps(event).encode()


def rejection(raw: bytes) -> EventRejectedError:
    with pytest.raises(EventRejectedError) as exc_info:
        parse_event(raw)
    return exc_info.value


# --- Criterion 1: a valid event parses, occurred_at is tz-aware UTC ---------------------------


def test_valid_event_parses_into_typed_fields() -> None:
    event = parse_event(encode(valid_event()))

    assert isinstance(event, OrderPlacedEvent)
    assert event.event_id == UUID(EVENT_ID)
    assert event.schema_version == 1
    assert event.source == "web-shop.checkout"
    assert event.payload.order_id == "ord-1001"
    assert event.payload.store_id == "store-7"
    assert event.payload.amount_minor == 2599
    assert event.payload.currency == "EUR"


def test_occurred_at_is_normalised_to_utc() -> None:
    event = parse_event(encode(valid_event()))

    assert event.occurred_at == datetime(2026, 11, 18, 10, 0, 30, tzinfo=UTC)
    assert event.occurred_at.tzinfo is UTC


@pytest.mark.parametrize("amount", [0, MAX_AMOUNT_MINOR])
def test_amount_bounds_are_inclusive(amount: int) -> None:
    event = valid_event()
    event["payload"]["amount_minor"] = amount

    assert parse_event(encode(event)).payload.amount_minor == amount


@pytest.mark.parametrize(
    ("occurred_at", "expected"),
    [
        ("2026-11-18T10:00:30Z", datetime(2026, 11, 18, 10, 0, 30, tzinfo=UTC)),
        ("2026-11-18t10:00:30z", datetime(2026, 11, 18, 10, 0, 30, tzinfo=UTC)),
        ("2026-11-18T10:00:30+00:00", datetime(2026, 11, 18, 10, 0, 30, tzinfo=UTC)),
        ("2026-11-18T10:00:30.123456Z", datetime(2026, 11, 18, 10, 0, 30, 123456, tzinfo=UTC)),
        ("2026-11-18T15:45:30+05:45", datetime(2026, 11, 18, 10, 0, 30, tzinfo=UTC)),
    ],
    ids=["Z", "lowercase-t-z", "plus-zero", "fraction", "non-whole-hour"],
)
def test_rfc3339_forms_parse_to_utc(occurred_at: str, expected: datetime) -> None:
    event = valid_event()
    event["occurred_at"] = occurred_at

    parsed = parse_event(encode(event)).occurred_at

    assert parsed == expected
    assert parsed.tzinfo is UTC


def test_offset_timestamp_from_the_wire_lands_in_its_utc_window() -> None:
    # Criterion 3 end to end: "+02:00" text -> parsed -> same window as the UTC instant.
    event = valid_event()  # occurred_at 12:00:30+02:00 = 10:00:30 UTC

    occurred_at = parse_event(encode(event)).occurred_at

    assert window_start(occurred_at) == datetime(2026, 11, 18, 10, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("field", "value"),
    [("source", "s" * 64), ("order_id", "o" * 64), ("store_id", "s" * 64)],
)
def test_string_fields_accept_their_maximum_length(field: str, value: str) -> None:
    event = valid_event()
    if field == "source":
        event["source"] = value
    else:
        event["payload"][field] = value

    parse_event(encode(event))


def test_body_exactly_at_the_size_limit_is_accepted() -> None:
    raw = encode(valid_event())
    padded = raw + b" " * (MAX_BODY_BYTES - len(raw))  # JSON allows trailing whitespace
    assert len(padded) == MAX_BODY_BYTES

    parse_event(padded)


# --- Criterion 2: rejected with a stable code and the field path -------------------------------


def _without(field: str) -> dict[str, Any]:
    event = valid_event()
    del event[field]
    return event


def _without_payload(field: str) -> dict[str, Any]:
    event = valid_event()
    del event["payload"][field]
    return event


def _with(field: str, value: object) -> dict[str, Any]:
    event = valid_event()
    event[field] = value
    return event


def _with_payload(field: str, value: object) -> dict[str, Any]:
    event = valid_event()
    event["payload"][field] = value
    return event


REJECTIONS: list[tuple[str, dict[str, Any], Violation]] = [
    (
        "missing envelope field",
        _without("event_id"),
        Violation(ReasonCode.MISSING_FIELD, "event_id"),
    ),
    (
        "missing payload field",
        _without_payload("currency"),
        Violation(ReasonCode.MISSING_FIELD, "payload.currency"),
    ),
    (
        "unknown envelope field",
        _with("received_at", "2026-11-18T12:00:00Z"),
        Violation(ReasonCode.UNKNOWN_FIELD, ROOT_PATH),
    ),
    (
        "unknown payload field",
        _with_payload("customer_email", "x"),
        Violation(ReasonCode.UNKNOWN_FIELD, "payload"),
    ),
    (
        "string amount",
        _with_payload("amount_minor", "2599"),
        Violation(ReasonCode.INVALID_TYPE, "payload.amount_minor"),
    ),
    (
        "float amount",
        _with_payload("amount_minor", 25.99),
        Violation(ReasonCode.INVALID_TYPE, "payload.amount_minor"),
    ),
    (
        "bool amount",
        _with_payload("amount_minor", True),
        Violation(ReasonCode.INVALID_TYPE, "payload.amount_minor"),
    ),
    (
        "negative amount",
        _with_payload("amount_minor", -1),
        Violation(ReasonCode.OUT_OF_RANGE, "payload.amount_minor"),
    ),
    (
        "amount over the cap",
        _with_payload("amount_minor", MAX_AMOUNT_MINOR + 1),
        Violation(ReasonCode.OUT_OF_RANGE, "payload.amount_minor"),
    ),
    (
        "string schema_version",
        _with("schema_version", "1"),
        Violation(ReasonCode.INVALID_TYPE, "schema_version"),
    ),
    (
        "payload not an object",
        _with("payload", [1, 2]),
        Violation(ReasonCode.INVALID_TYPE, "payload"),
    ),
    (
        "unknown event_type",
        _with("event_type", "order.refunded"),
        Violation(ReasonCode.UNKNOWN_EVENT_TYPE, "event_type"),
    ),
    (
        "unsupported schema_version",
        _with("schema_version", 2),
        Violation(ReasonCode.UNSUPPORTED_SCHEMA_VERSION, "schema_version"),
    ),
    (
        "naive occurred_at",
        _with("occurred_at", "2026-11-18T12:00:30"),
        Violation(ReasonCode.NAIVE_DATETIME, "occurred_at"),
    ),
    (
        "date-only occurred_at",
        _with("occurred_at", "2026-11-18"),
        Violation(ReasonCode.INVALID_FORMAT, "occurred_at"),
    ),
    (
        "unix timestamp occurred_at",
        _with("occurred_at", 1763467230),
        Violation(ReasonCode.INVALID_TYPE, "occurred_at"),
    ),
    (
        "malformed event_id",
        _with("event_id", "not-a-uuid"),
        Violation(ReasonCode.INVALID_FORMAT, "event_id"),
    ),
    ("bad source", _with("source", "Web Shop"), Violation(ReasonCode.INVALID_FORMAT, "source")),
    (
        "lowercase currency",
        _with_payload("currency", "eur"),
        Violation(ReasonCode.INVALID_FORMAT, "payload.currency"),
    ),
    (
        "4-letter currency",
        _with_payload("currency", "EURO"),
        Violation(ReasonCode.INVALID_FORMAT, "payload.currency"),
    ),
    (
        "empty order_id",
        _with_payload("order_id", ""),
        Violation(ReasonCode.INVALID_FORMAT, "payload.order_id"),
    ),
    (
        "store_id too long",
        _with_payload("store_id", "s" * 65),
        Violation(ReasonCode.INVALID_FORMAT, "payload.store_id"),
    ),
    # store_id is narrower than source: no dots, no uppercase, no trailing newline.
    (
        "store_id with a dot",
        _with_payload("store_id", "store.7"),
        Violation(ReasonCode.INVALID_FORMAT, "payload.store_id"),
    ),
    (
        "uppercase store_id",
        _with_payload("store_id", "Store7"),
        Violation(ReasonCode.INVALID_FORMAT, "payload.store_id"),
    ),
    (
        "store_id with trailing newline",
        _with_payload("store_id", "store7\n"),
        Violation(ReasonCode.INVALID_FORMAT, "payload.store_id"),
    ),
    ("empty source", _with("source", ""), Violation(ReasonCode.INVALID_FORMAT, "source")),
    ("source too long", _with("source", "s" * 65), Violation(ReasonCode.INVALID_FORMAT, "source")),
    (
        "order_id too long",
        _with_payload("order_id", "o" * 65),
        Violation(ReasonCode.INVALID_FORMAT, "payload.order_id"),
    ),
    (
        "order_id with NUL",
        _with_payload("order_id", "ord\x001"),
        Violation(ReasonCode.INVALID_FORMAT, "payload.order_id"),
    ),
    (
        "order_id with newline",
        _with_payload("order_id", "ord\n1"),
        Violation(ReasonCode.INVALID_FORMAT, "payload.order_id"),
    ),
    (
        "order_id with DEL",
        _with_payload("order_id", "ord\x7f"),
        Violation(ReasonCode.INVALID_FORMAT, "payload.order_id"),
    ),
    (
        "bool schema_version",
        _with("schema_version", True),
        Violation(ReasonCode.INVALID_TYPE, "schema_version"),
    ),
    # occurred_at must follow the RFC 3339 date-time grammar, not whatever Pydantic can parse.
    (
        "unix seconds as a string",
        _with("occurred_at", "1763467230"),
        Violation(ReasonCode.INVALID_FORMAT, "occurred_at"),
    ),
    (
        "unix seconds with fraction as a string",
        _with("occurred_at", "1763467230.5"),
        Violation(ReasonCode.INVALID_FORMAT, "occurred_at"),
    ),
    (
        "occurred_at without seconds",
        _with("occurred_at", "2026-11-18T12:00Z"),
        Violation(ReasonCode.INVALID_FORMAT, "occurred_at"),
    ),
    (
        "space instead of T",
        _with("occurred_at", "2026-11-18 12:00:30Z"),
        Violation(ReasonCode.INVALID_FORMAT, "occurred_at"),
    ),
    (
        "hour-only offset",
        _with("occurred_at", "2026-11-18T12:00:30+02"),
        Violation(ReasonCode.INVALID_FORMAT, "occurred_at"),
    ),
    (
        "trailing newline after timestamp",
        _with("occurred_at", "2026-11-18T12:00:30Z\n"),
        Violation(ReasonCode.INVALID_FORMAT, "occurred_at"),
    ),
    (
        "unknown local offset -00:00",
        _with("occurred_at", "2026-11-18T12:00:30-00:00"),
        Violation(ReasonCode.NAIVE_DATETIME, "occurred_at"),
    ),
    # Valid RFC 3339 instants that fall outside Python's datetime range once converted to UTC.
    (
        "occurred_at below the datetime range in UTC",
        _with("occurred_at", "0001-01-01T00:00:00+01:00"),
        Violation(ReasonCode.OUT_OF_RANGE, "occurred_at"),
    ),
    (
        "occurred_at above the datetime range in UTC",
        _with("occurred_at", "9999-12-31T23:59:59-10:00"),
        Violation(ReasonCode.OUT_OF_RANGE, "occurred_at"),
    ),
]


@pytest.mark.parametrize(
    ("event", "expected"),
    [(event, expected) for _, event, expected in REJECTIONS],
    ids=[name for name, _, _ in REJECTIONS],
)
def test_invalid_event_is_rejected_with_code_and_path(
    event: dict[str, Any], expected: Violation
) -> None:
    assert rejection(encode(event)).violations == (expected,)


def test_oversize_body_is_rejected() -> None:
    raw = encode(valid_event())
    oversize = raw + b" " * (MAX_BODY_BYTES + 1 - len(raw))

    assert rejection(oversize).violations == (Violation(ReasonCode.BODY_TOO_LARGE, ROOT_PATH),)


def test_size_is_checked_before_json_parsing() -> None:
    # Invalid JSON *and* oversize: only the size is reported, so the parser never saw the body.
    oversize_garbage = b"{" + b" " * MAX_BODY_BYTES

    assert rejection(oversize_garbage).violations == (
        Violation(ReasonCode.BODY_TOO_LARGE, ROOT_PATH),
    )


def test_duplicate_key_is_rejected_even_when_the_last_value_is_valid() -> None:
    # Without the check, the JSON parser keeps the last value and the invalid -5 is never seen.
    raw = encode(valid_event()).replace(
        b'"amount_minor": 2599', b'"amount_minor": -5, "amount_minor": 2599'
    )
    assert raw.count(b'"amount_minor"') == 2

    assert rejection(raw).violations == (Violation(ReasonCode.INVALID_JSON, ROOT_PATH),)


def test_duplicate_envelope_key_is_rejected() -> None:
    raw = encode(valid_event()).replace(b'"source": ', b'"source": "other", "source": ')

    assert rejection(raw).violations == (Violation(ReasonCode.INVALID_JSON, ROOT_PATH),)


def test_payload_is_not_validated_when_the_envelope_stage_fails() -> None:
    event = _with("event_type", "order.refunded")
    event["payload"]["amount_minor"] = -1

    assert rejection(encode(event)).violations == (
        Violation(ReasonCode.UNKNOWN_EVENT_TYPE, "event_type"),
    )


def test_every_payload_violation_is_reported() -> None:
    event = valid_event()
    event["payload"]["amount_minor"] = -1
    event["payload"]["currency"] = "eur"

    assert set(rejection(encode(event)).violations) == {
        Violation(ReasonCode.OUT_OF_RANGE, "payload.amount_minor"),
        Violation(ReasonCode.INVALID_FORMAT, "payload.currency"),
    }


@pytest.mark.parametrize(
    "raw", [b'{"event_id": ', b"", b"\xff\xfe"], ids=["truncated", "empty", "not-utf8"]
)
def test_invalid_json_is_rejected(raw: bytes) -> None:
    assert rejection(raw).violations == (Violation(ReasonCode.INVALID_JSON, ROOT_PATH),)


def test_non_object_body_is_rejected() -> None:
    assert rejection(b"[1, 2]").violations == (Violation(ReasonCode.INVALID_TYPE, ROOT_PATH),)


def test_every_violation_in_a_stage_is_reported() -> None:
    event = valid_event()
    del event["source"]
    event["occurred_at"] = "2026-11-18T12:00:30"

    assert set(rejection(encode(event)).violations) == {
        Violation(ReasonCode.MISSING_FIELD, "source"),
        Violation(ReasonCode.NAIVE_DATETIME, "occurred_at"),
    }


# --- Rejections never echo input ---------------------------------------------------------------

SECRET = "SENTINEL-4111111111111111"


@pytest.mark.parametrize(
    "event",
    [
        _with_payload("order_id", SECRET * 4),  # too long
        _with_payload("currency", SECRET),
        _with_payload("amount_minor", SECRET),
        _with("event_id", SECRET),
        _with("occurred_at", SECRET),
        _with_payload(SECRET, 1),  # the unknown key itself is client input
        _with(SECRET, 1),
    ],
    ids=["too-long", "pattern", "type", "uuid", "datetime", "unknown-payload-key", "unknown-key"],
)
def test_rejection_does_not_echo_input(event: dict[str, Any]) -> None:
    error = rejection(encode(event))

    assert SECRET not in str(error)
    assert SECRET not in repr(error.violations)
    # Pydantic's own error text contains the input; it must not be reachable through the
    # exception chain, neither as the cause nor as the (suppressed) context.
    assert error.__cause__ is None
    assert error.__context__ is None


def test_rejected_error_requires_a_violation() -> None:
    with pytest.raises(ValueError, match="at least one violation"):
        EventRejectedError(())
