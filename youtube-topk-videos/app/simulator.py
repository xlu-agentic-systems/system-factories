from __future__ import annotations

import math
import random
from collections import Counter
from collections.abc import Iterator

from app.models import ScaleProjection, ViewEvent, WINDOWS, WINDOW_1HOUR
from app.time_windows import bucket_start


def generate_zipfian_views(
    total_events: int,
    distinct_videos: int,
    start_time: int,
    duration_seconds: int,
    skew: float = 1.1,
    seed: int = 7,
) -> Iterator[ViewEvent]:
    if total_events < 0:
        raise ValueError("total_events must be non-negative")
    if distinct_videos <= 0:
        raise ValueError("distinct_videos must be positive")
    if duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")

    rng = random.Random(seed)
    video_ids = [f"video_{i:06d}" for i in range(distinct_videos)]
    weights = [1.0 / ((i + 1) ** skew) for i in range(distinct_videos)]

    for offset in range(total_events):
        video_id = rng.choices(video_ids, weights=weights, k=1)[0]
        yield ViewEvent(
            event_id=f"evt_{seed}_{offset}",
            video_id=video_id,
            occurred_at=start_time + (offset % duration_seconds),
            kafka_partition=offset % 512,
            kafka_offset=offset,
        )


def generate_zipfian_hourly_count_batches(
    total_events: int,
    distinct_videos: int,
    start_time: int,
    duration_seconds: int,
    batch_size: int = 100_000,
    skew: float = 1.1,
    seed: int = 7,
) -> Iterator[list[tuple[int, str, int]]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if total_events < 0:
        raise ValueError("total_events must be non-negative")
    if distinct_videos <= 0:
        raise ValueError("distinct_videos must be positive")
    if duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")

    rng = random.Random(seed)
    video_ids = [f"video_{i:06d}" for i in range(distinct_videos)]
    weights = [1.0 / ((i + 1) ** skew) for i in range(distinct_videos)]

    produced = 0
    while produced < total_events:
        size = min(batch_size, total_events - produced)
        videos = rng.choices(video_ids, weights=weights, k=size)
        counts: Counter[tuple[int, str]] = Counter()
        for index, video_id in enumerate(videos):
            event_offset = produced + index
            occurred_at = start_time + (event_offset % duration_seconds)
            counts[(bucket_start(WINDOW_1HOUR, occurred_at), video_id)] += 1
        produced += size
        yield [
            (hour_start, video_id, view_count)
            for (hour_start, video_id), view_count in counts.items()
        ]


def generate_projected_zipfian_hourly_count_batches(
    total_events: int,
    distinct_videos: int,
    start_time: int,
    duration_seconds: int,
    batch_rows: int = 100_000,
    skew: float = 1.1,
) -> Iterator[list[tuple[int, str, int]]]:
    if batch_rows <= 0:
        raise ValueError("batch_rows must be positive")
    if total_events < 0:
        raise ValueError("total_events must be non-negative")
    if distinct_videos <= 0:
        raise ValueError("distinct_videos must be positive")
    if duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")

    hours = max(1, math.ceil(duration_seconds / 3600))
    hour_starts = [bucket_start(WINDOW_1HOUR, start_time + hour * 3600) for hour in range(hours)]
    total_weight = sum(1.0 / ((rank + 1) ** skew) for rank in range(distinct_videos))
    cumulative_weight = 0.0
    allocated = 0
    rows: list[tuple[int, str, int]] = []

    for rank in range(distinct_videos):
        cumulative_weight += 1.0 / ((rank + 1) ** skew)
        target_allocated = round(total_events * cumulative_weight / total_weight)
        video_count = target_allocated - allocated
        allocated = target_allocated
        if video_count <= 0:
            continue

        video_id = f"video_{rank:06d}"
        base = video_count // hours
        remainder = video_count % hours
        if base:
            for hour_index, hour_start in enumerate(hour_starts):
                count = base + (1 if hour_index < remainder else 0)
                rows.append((hour_start, video_id, count))
                if len(rows) >= batch_rows:
                    yield rows
                    rows = []
        else:
            step = max(1, hours // max(1, remainder))
            hour_index = rank % hours
            for _ in range(remainder):
                rows.append((hour_starts[hour_index], video_id, 1))
                if len(rows) >= batch_rows:
                    yield rows
                    rows = []
                hour_index = (hour_index + step) % hours

    if rows:
        yield rows


def project_scale(
    daily_views: int = 70_000_000_000,
    batch_size: int = 50_000,
    shard_count: int = 20,
    aggregate_window_count: int = len(WINDOWS),
) -> ScaleProjection:
    if daily_views <= 0:
        raise ValueError("daily_views must be positive")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")

    return ScaleProjection(
        daily_views=daily_views,
        events_per_second=daily_views / 86_400,
        micro_batches_per_day=math.ceil(daily_views / batch_size),
        max_counter_rows_per_hot_video_window=shard_count,
        aggregate_windows_per_event=aggregate_window_count,
        write_amplification_per_batch_key=2,
    )
