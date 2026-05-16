# Experiment Details

This project compares two architectures for YouTube-style top-k video views.

## Workload

The target production scale is:

```text
70,000,000,000 views/day
810,185 average views/second
k <= 1000
windows = 1hour, 1day, 1month, all_time
```

The checked benchmark uses a 50M event-equivalent workload:

```text
events=50,000,000
videos=20,000
duration_days=30
k=100
iterations=5
distribution=Zipf-like popularity
```

The benchmark uses `generation_mode=projected` by default. This keeps the total count exactly equal to 50M while generating hourly `(hour_start, video_id, count)` rows directly. That avoids spending most of the run constructing 50M Python event objects and lets the experiment focus on serving-table size and top-k query latency.

Use `--generation-mode sampled` to draw every event individually. That mode is closer to an event-by-event simulator but is much slower.

## Compared Architectures

### Precomputed Top-K

The precomputed path models the intended streaming architecture:

```text
Kafka events
  -> sharded batch aggregation
  -> view_count_shards
  -> aggregate_1hour, aggregate_1day, aggregate_1month, aggregate_all_time
  -> topk_snapshots
```

Writes are heavier because the system spends compute ahead of time. Reads are cheap because top-k is already materialized:

```sql
SELECT rank, video_id, view_count
FROM topk_snapshots
WHERE window = ? AND bucket_start = ? AND rank <= ?
ORDER BY rank;
```

### Native Runtime Aggregation

The baseline path stores only hourly counts:

```text
native_hourly_counts(hour_start, video_id, view_count)
```

Hour queries can read one bucket directly. Day, month, and all-time queries must aggregate at read time:

```sql
SELECT video_id, SUM(view_count) AS view_count
FROM native_hourly_counts
WHERE hour_start >= ? AND hour_start < ?
GROUP BY video_id
ORDER BY view_count DESC, video_id ASC
LIMIT ?;
```

This is intentionally expensive for wider windows because the database must scan many hourly rows, group by video, sort, and limit on every query.

## Commands

Small smoke benchmark:

```bash
python scripts/benchmark_architectures.py \
  --events 1000000 \
  --videos 50000 \
  --duration-days 7 \
  --batch-size 100000 \
  --k 100 \
  --iterations 5
```

Checked 50M event-equivalent benchmark:

```bash
python scripts/benchmark_architectures.py \
  --events 50000000 \
  --videos 20000 \
  --duration-days 30 \
  --batch-size 250000 \
  --k 100 \
  --iterations 5
```

Materialize sharded counter rows during the benchmark load:

```bash
python scripts/benchmark_architectures.py \
  --events 50000000 \
  --videos 20000 \
  --duration-days 30 \
  --batch-size 250000 \
  --k 100 \
  --iterations 5 \
  --write-shards
```

## Interpretation

The benchmark is not claiming SQLite is the production serving database. SQLite is used here to make the tradeoff visible on one machine. The important result is the shape:

- Hour window: both paths are similar because both can read one hour bucket.
- Day window: native runtime aggregation starts paying for a multi-hour scan and group.
- Month/all-time: native runtime aggregation becomes much more expensive because it scans and groups the largest row ranges.
- Precomputed top-k: query latency remains bounded by a small indexed lookup into `topk_snapshots`.

At 70B views/day, the production system should use Kafka for retention, Flink or an equivalent stream processor for sharded batch aggregation, and a serving store designed for high write volume plus indexed top-k reads.

