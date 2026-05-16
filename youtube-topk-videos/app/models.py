from __future__ import annotations

from dataclasses import dataclass


WINDOW_1HOUR = "1hour"
WINDOW_1DAY = "1day"
WINDOW_1MONTH = "1month"
WINDOW_ALL_TIME = "all_time"
WINDOWS = (WINDOW_1HOUR, WINDOW_1DAY, WINDOW_1MONTH, WINDOW_ALL_TIME)


@dataclass(frozen=True)
class ViewEvent:
    event_id: str
    video_id: str
    occurred_at: int
    kafka_partition: int = 0
    kafka_offset: int | None = None


@dataclass(frozen=True)
class CountDelta:
    window: str
    bucket_start: int
    video_id: str
    shard_id: int
    view_count: int


@dataclass(frozen=True)
class TopKEntry:
    rank: int
    video_id: str
    view_count: int
    window: str
    bucket_start: int


@dataclass(frozen=True)
class ScaleProjection:
    daily_views: int
    events_per_second: float
    micro_batches_per_day: int
    max_counter_rows_per_hot_video_window: int
    aggregate_windows_per_event: int
    write_amplification_per_batch_key: int

