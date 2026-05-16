from __future__ import annotations

import argparse
import os
import statistics
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.models import WINDOW_1DAY, WINDOW_1HOUR, WINDOW_1MONTH, WINDOW_ALL_TIME
from app.simulator import (
    generate_projected_zipfian_hourly_count_batches,
    generate_zipfian_hourly_count_batches,
    project_scale,
)
from app.storage import SQLiteTopKStorage
from app.time_windows import bucket_start


WINDOWS = (WINDOW_1HOUR, WINDOW_1DAY, WINDOW_1MONTH, WINDOW_ALL_TIME)


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[index]


def measure_query_latencies(
    storage: SQLiteTopKStorage,
    query_time: int,
    k: int,
    iterations: int,
) -> dict[str, dict[str, dict[str, float]]]:
    results: dict[str, dict[str, dict[str, float]]] = {}
    for window in WINDOWS:
        start = bucket_start(window, query_time)
        precomputed: list[float] = []
        native: list[float] = []
        for _ in range(iterations):
            before = time.perf_counter()
            storage.topk(window, start, k)
            precomputed.append((time.perf_counter() - before) * 1000)

            before = time.perf_counter()
            storage.native_topk(window, start, k)
            native.append((time.perf_counter() - before) * 1000)

        results[window] = {
            "precomputed_ms": summarize(precomputed),
            "native_runtime_ms": summarize(native),
        }
    return results


def summarize(values: list[float]) -> dict[str, float]:
    return {
        "p50": statistics.median(values),
        "p95": percentile(values, 95),
        "max": max(values),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark precomputed top-k snapshots against native runtime GROUP BY/SUM queries."
    )
    parser.add_argument("--db", default="data/architecture-benchmark.sqlite3")
    parser.add_argument("--events", type=int, default=50_000_000)
    parser.add_argument("--videos", type=int, default=20_000)
    parser.add_argument("--duration-days", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=250_000)
    parser.add_argument("--k", type=int, default=100)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--start-time", type=int, default=1_700_000_000)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--shards", type=int, default=20)
    parser.add_argument(
        "--generation-mode",
        choices=("projected", "sampled"),
        default="projected",
        help="projected preserves total event count quickly; sampled draws every event.",
    )
    parser.add_argument(
        "--write-shards",
        action="store_true",
        help="Also materialize view_count_shards during benchmark load.",
    )
    args = parser.parse_args()

    if args.k <= 0 or args.k > 1000:
        raise ValueError("k must be between 1 and 1000")

    storage = SQLiteTopKStorage(args.db, k_limit=1000)
    projection = project_scale(daily_views=70_000_000_000, batch_size=args.batch_size)
    duration_seconds = args.duration_days * 86_400
    started = time.perf_counter()
    loaded = 0
    affected_buckets: set[tuple[str, int]] = set()

    if args.generation_mode == "sampled":
        batches = generate_zipfian_hourly_count_batches(
            total_events=args.events,
            distinct_videos=args.videos,
            start_time=args.start_time,
            duration_seconds=duration_seconds,
            batch_size=args.batch_size,
            seed=args.seed,
        )
    else:
        batches = generate_projected_zipfian_hourly_count_batches(
            total_events=args.events,
            distinct_videos=args.videos,
            start_time=args.start_time,
            duration_seconds=duration_seconds,
            batch_rows=args.batch_size,
        )

    for batch in batches:
        loaded += sum(view_count for _, _, view_count in batch)
        storage.apply_native_hourly_counts(batch)
        affected_buckets.update(
            storage.apply_precomputed_hourly_counts(
                batch,
                shard_count=args.shards,
                refresh_snapshots=False,
                write_shards=args.write_shards,
            )
        )
        if loaded and loaded % max(args.batch_size * 10, 1) == 0:
            print(f"loaded_events={loaded:,}", flush=True)

    refreshed = storage.refresh_topk_snapshots(affected_buckets)
    load_seconds = time.perf_counter() - started
    query_time = args.start_time + min(duration_seconds - 1, max(0, duration_seconds // 2))
    latencies = measure_query_latencies(storage, query_time, args.k, args.iterations)

    print("\ninput")
    print(f"event_equivalent_views={args.events:,}")
    print(f"videos={args.videos:,}")
    print(f"duration_days={args.duration_days}")
    print(f"generation_mode={args.generation_mode}")
    print(f"write_shards={args.write_shards}")
    print(f"target_70b_avg_rate={projection.events_per_second:,.0f} events/sec")
    print(f"target_micro_batches_per_day={projection.micro_batches_per_day:,}")

    print("\nload")
    print(f"loaded_events={loaded:,}")
    print(f"load_seconds={load_seconds:,.2f}")
    print(f"refreshed_topk_buckets={refreshed:,}")
    for table in (
        "native_hourly_counts",
        "view_count_shards",
        "aggregate_1hour",
        "aggregate_1day",
        "aggregate_1month",
        "aggregate_all_time",
        "topk_snapshots",
    ):
        print(f"{table}_rows={storage.row_count(table):,}")

    print("\nquery_latency_ms")
    for window, paths in latencies.items():
        pre = paths["precomputed_ms"]
        native = paths["native_runtime_ms"]
        speedup = native["p50"] / max(pre["p50"], 0.001)
        print(
            f"{window}: "
            f"precomputed p50={pre['p50']:.3f} p95={pre['p95']:.3f} max={pre['max']:.3f}; "
            f"native p50={native['p50']:.3f} p95={native['p95']:.3f} max={native['max']:.3f}; "
            f"p50_speedup={speedup:,.1f}x"
        )


if __name__ == "__main__":
    main()
