from __future__ import annotations

from datetime import UTC, datetime

from app.models import WINDOW_1DAY, WINDOW_1HOUR, WINDOW_1MONTH, WINDOW_ALL_TIME


SECONDS_PER_HOUR = 60 * 60
SECONDS_PER_DAY = 24 * SECONDS_PER_HOUR


def bucket_start(window: str, occurred_at: int) -> int:
    if window == WINDOW_1HOUR:
        return occurred_at - (occurred_at % SECONDS_PER_HOUR)
    if window == WINDOW_1DAY:
        return occurred_at - (occurred_at % SECONDS_PER_DAY)
    if window == WINDOW_1MONTH:
        dt = datetime.fromtimestamp(occurred_at, UTC)
        return int(datetime(dt.year, dt.month, 1, tzinfo=UTC).timestamp())
    if window == WINDOW_ALL_TIME:
        return 0
    raise ValueError(f"unsupported window: {window}")


def bucket_end(window: str, start: int) -> int | None:
    if window == WINDOW_1HOUR:
        return start + SECONDS_PER_HOUR
    if window == WINDOW_1DAY:
        return start + SECONDS_PER_DAY
    if window == WINDOW_1MONTH:
        dt = datetime.fromtimestamp(start, UTC)
        if dt.month == 12:
            end = datetime(dt.year + 1, 1, 1, tzinfo=UTC)
        else:
            end = datetime(dt.year, dt.month + 1, 1, tzinfo=UTC)
        return int(end.timestamp())
    if window == WINDOW_ALL_TIME:
        return None
    raise ValueError(f"unsupported window: {window}")
