from __future__ import annotations

import argparse
import json
import queue
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path

from app.store import PostgresLikeStore, WriteResult
from app.workload import generate_operations, post_ids


@dataclass(frozen=True)
class BenchmarkResult:
    mode: str
    operations: int
    posts: int
    workers: int
    successes: int
    elapsed_s: float
    throughput_ops_s: float
    avg_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    hottest_post_count: int


def run_benchmark(
    dsn: str,
    mode: str,
    operations: int,
    posts: int,
    workers: int,
) -> BenchmarkResult:
    import psycopg

    store = PostgresLikeStore(dsn)
    store.reset(post_ids(posts))
    workload = generate_operations(operations, mode=mode, posts=posts)
    pool_size = min(workers, operations)
    connections = [psycopg.connect(dsn, autocommit=False) for _ in range(pool_size)]
    pool: queue.LifoQueue = queue.LifoQueue()
    for conn in connections:
        pool.put(conn)

    def execute(operation) -> WriteResult:
        conn = pool.get()
        try:
            return store.like_with_connection(conn, operation.post_id, operation.user_id)
        finally:
            pool.put(conn)

    started = time.perf_counter()
    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(execute, workload))
    finally:
        for conn in connections:
            conn.close()
    elapsed_s = time.perf_counter() - started
    counts = store.counts()
    latencies = [result.elapsed_ms for result in results]
    successes = sum(1 for result in results if result.ok)

    return BenchmarkResult(
        mode=mode,
        operations=operations,
        posts=posts,
        workers=workers,
        successes=successes,
        elapsed_s=round(elapsed_s, 3),
        throughput_ops_s=round(operations / elapsed_s, 1),
        avg_ms=round(statistics.mean(latencies), 3),
        p50_ms=round(_percentile(latencies, 0.50), 3),
        p95_ms=round(_percentile(latencies, 0.95), 3),
        p99_ms=round(_percentile(latencies, 0.99), 3),
        max_ms=round(max(latencies), 3),
        hottest_post_count=max(counts.values()) if counts else 0,
    )


def format_results(results: list[BenchmarkResult]) -> str:
    lines = [
        "mode         ops      posts  workers  ok       ops/s     avg_ms  p50_ms  p95_ms   p99_ms   hottest_post",
        "-----------  -------  -----  -------  -------  --------  ------  ------  -------  -------  ------------",
    ]
    for result in results:
        lines.append(
            f"{result.mode:<11}  "
            f"{result.operations:>7}  "
            f"{result.posts:>5}  "
            f"{result.workers:>7}  "
            f"{result.successes:>7}  "
            f"{result.throughput_ops_s:>8.1f}  "
            f"{result.avg_ms:>6.2f}  "
            f"{result.p50_ms:>6.2f}  "
            f"{result.p95_ms:>7.2f}  "
            f"{result.p99_ms:>7.2f}  "
            f"{result.hottest_post_count:>12}"
        )
    return "\n".join(lines)


def _percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = min(len(ordered) - 1, max(0, int(len(ordered) * pct)))
    return ordered[index]


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark true PostgreSQL hot-row versus distributed-row counter writes.")
    parser.add_argument("--dsn", default="postgresql://postgres:postgres@127.0.0.1:58132/likes")
    parser.add_argument("--operations", type=int, default=10_000)
    parser.add_argument("--posts", type=int, default=1_000)
    parser.add_argument("--workers", type=int, default=128)
    parser.add_argument("--mode", choices=["hot", "distributed", "all"], default="all")
    parser.add_argument("--output", default="results/latest.json")
    args = parser.parse_args()

    modes = ["hot", "distributed"] if args.mode == "all" else [args.mode]
    results = [
        run_benchmark(
            dsn=args.dsn,
            mode=mode,
            operations=args.operations,
            posts=args.posts,
            workers=args.workers,
        )
        for mode in modes
    ]
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps([asdict(result) for result in results], indent=2), encoding="utf-8")
    print(format_results(results))


if __name__ == "__main__":
    main()
