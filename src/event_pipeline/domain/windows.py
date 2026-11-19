"""Event-time window assignment (ADR 0001, decision 1).

Windows are tumbling, aligned to the Unix epoch in UTC, and half-open:
``[window_start, window_start + WINDOW_SIZE)``. The late-event policy is not here (ADR 0005).
"""

from datetime import UTC, datetime, timedelta

WINDOW_SIZE = timedelta(minutes=1)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def window_start(occurred_at: datetime) -> datetime:
    """Return the UTC start of the window that contains ``occurred_at``.

    Raises ``ValueError`` for a naive datetime: guessing its offset would put the event in the
    wrong window, and the contract already rejects naive timestamps at parse time.
    """
    if occurred_at.utcoffset() is None:
        raise ValueError("occurred_at must be timezone-aware")
    since_epoch = occurred_at.astimezone(UTC) - _EPOCH
    return _EPOCH + (since_epoch // WINDOW_SIZE) * WINDOW_SIZE
