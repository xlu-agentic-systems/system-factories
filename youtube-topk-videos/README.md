# YouTube Top-K Videos

Reference implementation of a Kafka-backed top-k video view aggregation system. Each Kafka event is one video view. The implementation uses Redpanda locally as a Kafka-compatible broker and SQLite for serving tables, while the write path models the production shape: sharded counters, micro-batches, four precomputed aggregate tables, and precomputed top-k snapshots.

## Goals

- Support tumbling `1hour`, `1day`, `1month`, and `all_time` windows.
- Support `k <= 1000`.
- Avoid one write per event to a single hot aggregate row.
- Keep top-k queries fast by reading precomputed rankings, not running runtime `SUM()`.
- Include a native baseline that stores hourly counts only and computes day/month/all-time top-k at read time.
- Simulate the write pattern and scale math for 70 billion views per day.

## Architecture

The native approach writes every view into one row such as:

```text
(window='1hour', bucket_start, video_id) += 1
```

That row becomes a bottleneck for hot videos. At 70 billion views/day, average throughput is about 810k events/sec, and real traffic is usually much peakier than the average.

This project uses a four-stage path:

1. Kafka provides raw `ViewEvent` records on the `video-views` topic.
2. `ShardedBatchAggregator` maps every event into `20` counter shards per video/window and coalesces events inside a micro-batch.
3. `SQLiteTopKStorage.apply_deltas` batch-upserts sharded counters and the four merged aggregate tables.
4. The same transaction refreshes `topk_snapshots` for affected tumbling windows.

The read path is intentionally simple:

```text
GET top-k(window, timestamp, k) -> topk_snapshots where rank <= k
```

It does not scan raw events and does not sum shard rows at query time.

## Data Model

```text
view_count_shards(window, bucket_start, video_id, shard_id, view_count)
aggregate_1hour(bucket_start, video_id, view_count)
aggregate_1day(bucket_start, video_id, view_count)
aggregate_1month(bucket_start, video_id, view_count)
aggregate_all_time(bucket_start, video_id, view_count)
topk_snapshots(window, bucket_start, rank, video_id, view_count)
native_hourly_counts(hour_start, video_id, view_count)
```

`view_count_shards` absorbs hot writes. The four `aggregate_*` tables are the premerged serving counts for the four query windows. `topk_snapshots` is the query-serving index.

`native_hourly_counts` is the intentionally expensive baseline. It stores only hourly video counts. For `1day`, `1month`, and `all_time` top-k queries, it must scan a time range, `GROUP BY video_id`, `SUM(view_count)`, sort, and limit at runtime.

For a single hot video in one hour, the local implementation writes at most `20` shard rows for that window instead of repeatedly updating one row per event. Because the aggregator batches first, 100k events for one video become at most `20` counter deltas per window in one flush.

## Kafka Local Run

Start Kafka-compatible Redpanda:

```bash
docker compose up -d
```

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Produce synthetic view events into Kafka:

```bash
python scripts/kafka_produce_views.py \
  --events 1000000 \
  --videos 1000000 \
  --daily-views 70000000000 \
  --batch-size 50000
```

Consume Kafka events into the sharded counter and aggregate tables:

```bash
python scripts/kafka_consume_topk.py \
  --max-messages 1000000 \
  --batch-size 50000 \
  --k 10
```

The producer prints the actual local producer rate plus the 70B/day target rate. It does not pretend a laptop has produced 70 billion Kafka messages; it uses real Kafka messages for the sample and reports how that sample maps onto the target production rate.

## Flink Mapping

This repository does not embed a Flink runtime. The local Kafka consumer performs the same micro-batch aggregate stage in Python. In production, the same flow maps to a Flink topology:

```text
KafkaSource<ViewEvent>
  -> keyBy(event_id/kafka partition shard)
  -> tumblingEventTimeWindows(1h, 1d, calendar-month)
  -> aggregate local shard counts
  -> upsert sharded counter table
  -> upsert materialized video total
  -> update top-k state/table for affected bucket
```

For production, use Kafka retention plus object storage as the source of truth, Flink checkpointing for exactly-once batch commits, and an OLAP or LSM-backed serving store for the four `aggregate_*` tables and `topk_snapshots`.

## Local Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/simulate_stream.py --events 200000 --videos 20000 --batch-size 50000 --k 10
```

`simulate_stream.py` bypasses Kafka and is useful for quick algorithm checks. The Kafka path above is the actual broker-backed event simulation.

## Benchmark Both Architectures

Run a small benchmark smoke test:

```bash
python scripts/benchmark_architectures.py \
  --events 1000000 \
  --videos 50000 \
  --duration-days 7 \
  --batch-size 100000 \
  --k 100 \
  --iterations 5
```

Run the larger 50M event-equivalent benchmark:

```bash
python scripts/benchmark_architectures.py \
  --events 50000000 \
  --videos 20000 \
  --duration-days 30 \
  --batch-size 250000 \
  --k 100 \
  --iterations 5
```

The benchmark loads both paths from the same simulated event distribution:

- Precomputed path: `view_count_shards`, four `aggregate_*` tables, and `topk_snapshots`.
- Native path: `native_hourly_counts` only, with runtime `SUM/GROUP BY/ORDER BY` for non-hour windows.

The output reports table row counts and p50/p95/max query latency by window for both architectures.

By default, the 50M benchmark uses `--generation-mode projected`: it creates an event-equivalent Zipfian distribution whose counts sum to exactly 50M without constructing 50M Python event objects. Use `--generation-mode sampled` when you explicitly want every event randomly sampled; that is much slower. Add `--write-shards` when you want to materialize the sharded counter table during the benchmark load instead of focusing on query latency.

See [docs/experiment-details.md](docs/experiment-details.md) for methodology and [docs/benchmark-results.md](docs/benchmark-results.md) for a checked 50M event-equivalent benchmark result.

Run tests:

```bash
pytest -q
```

## Scale Notes

`70,000,000,000 / 86,400 = ~810,185` average views/sec.

With `batch_size=50,000`, that is `1,400,000` micro-batches/day. A hot video batch touches bounded rows:

```text
20 shards * 4 aggregate windows = 80 sharded counter rows per hot video per batch
4 merged total rows
4 top-k snapshot refreshes
```

The exact number of touched rows depends on how many distinct videos appear in a batch, but it scales with `(distinct videos in batch * windows * shards used)`, not with raw event count.

This is the core tradeoff: spend streaming compute and storage writes ahead of time so the user-facing top-k query is a small indexed lookup.
