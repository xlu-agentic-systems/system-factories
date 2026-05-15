from __future__ import annotations

import argparse
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path

from app.dynamodb_store import LikeStore, WriteResult
from app.workload import generate_operations, post_ids


@dataclass(frozen=True)
class BenchmarkResult:
    mode: str
    operations: int
    posts: int
    workers: int
    successes: int
    conflicts: int
    elapsed_s: float
    throughput_ops_s: float
    avg_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    hottest_post_count: int


def run_benchmark(
    endpoint_url: str,
    mode: str,
    operations: int,
    posts: int,
    workers: int,
    seed: int,
    unlike_ratio: float = 0.0,
) -> BenchmarkResult:
    store = LikeStore(endpoint_url=endpoint_url)
    ids = post_ids(posts)
    store.reset(ids)
    workload = generate_operations(operations, mode=mode, posts=posts, seed=seed, unlike_ratio=unlike_ratio)

    def execute(operation) -> WriteResult:
        if operation.action == "like":
            return store.like(operation.post_id, operation.user_id)
        return store.unlike(operation.post_id, operation.user_id)

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(execute, workload))
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
        conflicts=operations - successes,
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
        "mode         ops      posts  workers  ok       conflicts  ops/s    avg_ms  p95_ms  p99_ms  hottest_post",
        "-----------  -------  -----  -------  -------  ---------  -------  ------  ------  ------  ------------",
    ]
    for result in results:
        lines.append(
            f"{result.mode:<11}  "
            f"{result.operations:>7}  "
            f"{result.posts:>5}  "
            f"{result.workers:>7}  "
            f"{result.successes:>7}  "
            f"{result.conflicts:>9}  "
            f"{result.throughput_ops_s:>7.1f}  "
            f"{result.avg_ms:>6.2f}  "
            f"{result.p95_ms:>6.2f}  "
            f"{result.p99_ms:>6.2f}  "
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
    parser = argparse.ArgumentParser(description="Benchmark DynamoDB like/unlike counter hot-key behavior.")
    parser.add_argument("--endpoint-url", default="http://127.0.0.1:58000")
    parser.add_argument("--operations", type=int, default=10_000)
    parser.add_argument("--posts", type=int, default=1_000)
    parser.add_argument("--workers", type=int, default=128)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--unlike-ratio", type=float, default=0.0)
    parser.add_argument("--mode", choices=["hot", "distributed", "zipf", "all"], default="all")
    parser.add_argument("--output", default="results/latest.json")
    args = parser.parse_args()

    modes = ["hot", "distributed"] if args.mode == "all" else [args.mode]
    results = [
        run_benchmark(
            endpoint_url=args.endpoint_url,
            mode=mode,
            operations=args.operations,
            posts=args.posts,
            workers=args.workers,
            seed=args.seed,
            unlike_ratio=args.unlike_ratio,
        )
        for mode in modes
    ]
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps([asdict(result) for result in results], indent=2), encoding="utf-8")
    print(format_results(results))


if __name__ == "__main__":
    main()
