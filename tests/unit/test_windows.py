"""Acceptance criterion 3 (PLAN.md, phase 1): event-time window assignment."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from event_pipeline.domain.windows import WINDOW_SIZE, window_start

NOON = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)


def test_window_size_is_one_minute() -> None:
    assert timedelta(minutes=1) == WINDOW_SIZE


@pytest.mark.parametrize(
    "occurred_at",
    [
        datetime(2026, 11, 18, 12, 0, 0, 0, tzinfo=UTC),
        datetime(2026, 11, 18, 12, 0, 30, tzinfo=UTC),
        datetime(2026, 11, 18, 12, 0, 59, 999_999, tzinfo=UTC),
    ],
    ids=["start", "middle", "last-microsecond"],
)
def test_instants_within_a_minute_share_a_window(occurred_at: datetime) -> None:
    assert window_start(occurred_at) == NOON


def test_next_minute_starts_the_next_window() -> None:
    assert window_start(datetime(2026, 11, 18, 12, 1, tzinfo=UTC)) == NOON + WINDOW_SIZE


def test_offset_input_buckets_like_its_utc_equivalent() -> None:
    plus_two = timezone(timedelta(hours=2))
    local = datetime(2026, 11, 18, 14, 0, 45, tzinfo=plus_two)  # 12:00:45 UTC

    start = window_start(local)

    assert start == NOON
    assert start.tzinfo is UTC


def test_non_whole_hour_offset_buckets_by_utc_minute() -> None:
    # +05:45 (Nepal): 17:46:10 local is 12:01:10 UTC.
    nepal = timezone(timedelta(hours=5, minutes=45))

    assert window_start(datetime(2026, 11, 18, 17, 46, 10, tzinfo=nepal)) == NOON + WINDOW_SIZE


def test_instant_before_the_epoch_floors_downwards() -> None:
    assert window_start(datetime(1969, 12, 31, 23, 59, 59, tzinfo=UTC)) == datetime(
        1969, 12, 31, 23, 59, tzinfo=UTC
    )


def test_naive_datetime_is_refused() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        window_start(datetime(2026, 11, 18, 12, 0))
