from __future__ import annotations

from app.kafka_io import event_from_json, event_to_json
from app.models import WINDOW_1DAY, WINDOW_1HOUR, WINDOW_1MONTH, WINDOW_ALL_TIME, ViewEvent
from app.query import TopKQueryService
from app.simulator import (
    generate_projected_zipfian_hourly_count_batches,
    generate_zipfian_views,
    project_scale,
)
from app.storage import SQLiteTopKStorage, AGGREGATE_TABLES
from app.stream import ShardedBatchAggregator, TopKStreamProcessor
from app.time_windows import bucket_start


def make_storage(tmp_path):
    return SQLiteTopKStorage(tmp_path / "topk.sqlite3", k_limit=1000)


def test_topk_is_materialized_for_hour_day_month_and_all_time(tmp_path) -> None:
    storage = make_storage(tmp_path)
    processor = TopKStreamProcessor(storage)
    events = [
        ViewEvent(event_id=f"a_{i}", video_id="video_a", occurred_at=1_700_000_000 + i)
        for i in range(5)
    ] + [
        ViewEvent(event_id=f"b_{i}", video_id="video_b", occurred_at=1_700_000_000 + i)
        for i in range(3)
    ] + [
        ViewEvent(event_id="c_0", video_id="video_c", occurred_at=1_700_000_000)
    ]

    assert processor.process_batch(events) == 9

    query = TopKQueryService(storage)
    for window in (WINDOW_1HOUR, WINDOW_1DAY, WINDOW_1MONTH, WINDOW_ALL_TIME):
        top = query.topk_at(window, 1_700_000_000, 2)
        assert [(entry.video_id, entry.view_count) for entry in top] == [
            ("video_a", 5),
            ("video_b", 3),
        ]


def test_hot_video_is_spread_across_counter_shards_but_queries_read_totals(tmp_path) -> None:
    storage = make_storage(tmp_path)
    processor = TopKStreamProcessor(storage, ShardedBatchAggregator(shard_count=20))
    events = [
        ViewEvent(event_id=f"hot_{i}", video_id="hot_video", occurred_at=1_700_000_000)
        for i in range(50_000)
    ]

    processor.process_batch(events)

    hour_start = bucket_start(WINDOW_1HOUR, 1_700_000_000)
    shard_counts = storage.shard_counts(WINDOW_1HOUR, hour_start, "hot_video")
    top = storage.topk(WINDOW_1HOUR, hour_start, 1)

    assert len(shard_counts) == 20
    assert sum(shard_counts.values()) == 50_000
    assert top[0].video_id == "hot_video"
    assert top[0].view_count == 50_000
    assert storage.video_total(WINDOW_1HOUR, hour_start, "hot_video") == 50_000


def test_batching_coalesces_many_events_into_bounded_counter_rows(tmp_path) -> None:
    storage = make_storage(tmp_path)
    processor = TopKStreamProcessor(storage, ShardedBatchAggregator(shard_count=20))
    events = [
        ViewEvent(event_id=f"event_{i}", video_id="same_video", occurred_at=1_700_000_000)
        for i in range(100_000)
    ]

    processor.process_batch(events)

    assert storage.row_count("view_count_shards") <= 20 * 4
    assert storage.row_count("aggregate_1hour") == 1
    assert storage.row_count("aggregate_1day") == 1
    assert storage.row_count("aggregate_1month") == 1
    assert storage.row_count("aggregate_all_time") == 1
    assert storage.row_count("topk_snapshots") == 4


def test_native_hourly_table_answers_topk_by_runtime_sum(tmp_path) -> None:
    storage = make_storage(tmp_path)
    hour_1 = bucket_start(WINDOW_1HOUR, 1_700_000_000)
    hour_2 = hour_1 + 3600
    rows = [
        (hour_1, "video_a", 5),
        (hour_2, "video_a", 7),
        (hour_1, "video_b", 6),
        (hour_2, "video_b", 1),
        (hour_1, "video_c", 3),
    ]

    storage.apply_native_hourly_counts(rows)
    affected = storage.apply_precomputed_hourly_counts(rows)

    day_start = bucket_start(WINDOW_1DAY, hour_1)
    assert affected
    assert storage.native_topk(WINDOW_1DAY, day_start, 2) == storage.topk(
        WINDOW_1DAY, day_start, 2
    )
    assert [(entry.video_id, entry.view_count) for entry in storage.native_topk(WINDOW_1DAY, day_start, 2)] == [
        ("video_a", 12),
        ("video_b", 7),
    ]


def test_storage_uses_four_explicit_aggregate_tables(tmp_path) -> None:
    storage = make_storage(tmp_path)

    assert storage.aggregate_tables() == AGGREGATE_TABLES
    assert set(storage.aggregate_tables().values()) == {
        "aggregate_1hour",
        "aggregate_1day",
        "aggregate_1month",
        "aggregate_all_time",
    }


def test_kafka_event_json_round_trip() -> None:
    event = ViewEvent(
        event_id="evt_1",
        video_id="video_1",
        occurred_at=1_700_000_000,
        kafka_partition=17,
        kafka_offset=42,
    )

    decoded = event_from_json(event_to_json(event), partition=18, offset=43)

    assert decoded == ViewEvent(
        event_id="evt_1",
        video_id="video_1",
        occurred_at=1_700_000_000,
        kafka_partition=18,
        kafka_offset=43,
    )


def test_zipfian_stream_simulation_produces_exact_topk(tmp_path) -> None:
    storage = make_storage(tmp_path)
    processor = TopKStreamProcessor(storage)
    events = list(
        generate_zipfian_views(
            total_events=20_000,
            distinct_videos=200,
            start_time=1_700_000_000,
            duration_seconds=3600,
            seed=11,
        )
    )
    expected: dict[str, int] = {}
    for event in events:
        expected[event.video_id] = expected.get(event.video_id, 0) + 1

    processor.process_stream(events, batch_size=2_000)

    top = storage.topk(WINDOW_ALL_TIME, 0, 10)
    expected_top = sorted(expected.items(), key=lambda item: (-item[1], item[0]))[:10]
    assert [(entry.video_id, entry.view_count) for entry in top] == expected_top


def test_projected_generator_preserves_event_count() -> None:
    batches = list(
        generate_projected_zipfian_hourly_count_batches(
            total_events=50_000,
            distinct_videos=100,
            start_time=1_700_000_000,
            duration_seconds=86_400,
            batch_rows=500,
        )
    )

    assert sum(count for batch in batches for _, _, count in batch) == 50_000


def test_70_billion_daily_scale_projection() -> None:
    projection = project_scale(daily_views=70_000_000_000, batch_size=50_000, shard_count=20)

    assert int(projection.events_per_second) == 810_185
    assert projection.micro_batches_per_day == 1_400_000
    assert projection.max_counter_rows_per_hot_video_window == 20
    assert projection.aggregate_windows_per_event == 4
