# Benchmark Results

Local benchmark run on the SQLite implementation with:

```text
events=50,000,000
videos=20,000
duration_days=30
generation_mode=projected
write_shards=False
k=100
iterations=5
```

The projected generator preserves the 50M event-equivalent view count without creating 50M Python objects. This run focused on query latency, so it loaded the native hourly table, the four precomputed aggregate tables, and top-k snapshots. It did not materialize the sharded counter table during the benchmark load.

## Loaded Rows

```text
native_hourly_counts_rows=7,587,074
view_count_shards_rows=0
aggregate_1hour_rows=7,587,074
aggregate_1day_rows=545,657
aggregate_1month_rows=39,987
aggregate_all_time_rows=20,000
topk_snapshots_rows=754,000
load_seconds=146.30
```

## Query Latency

```text
1hour:
  precomputed p50=0.166ms p95=0.328ms max=0.328ms
  native      p50=0.155ms p95=0.195ms max=0.195ms

1day:
  precomputed p50=0.256ms p95=0.277ms max=0.277ms
  native      p50=40.534ms p95=40.859ms max=40.859ms
  p50_speedup=158.4x

1month:
  precomputed p50=0.301ms p95=0.313ms max=0.313ms
  native      p50=737.610ms p95=746.051ms max=746.051ms
  p50_speedup=2450.9x

all_time:
  precomputed p50=0.332ms p95=0.378ms max=0.378ms
  native      p50=1173.219ms p95=1179.702ms max=1179.702ms
  p50_speedup=3537.3x
```

The hour query is similar because both paths can read one hour bucket directly. The gap appears on day, month, and all-time windows, where the native path scans `native_hourly_counts`, groups by `video_id`, sums counts, sorts, and limits at runtime.

