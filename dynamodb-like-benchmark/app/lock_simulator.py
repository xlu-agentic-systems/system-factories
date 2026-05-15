from __future__ import annotations

import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from app.workload import generate_operations, post_ids


@dataclass(frozen=True)
class LockSimResult:
    mode: str
    operations: int
    posts: int
    workers: int
    update_ms: float
    elapsed_s: float
    throughput_ops_s: float
    avg_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    hottest_post_count: int


class RowLockCounterStore:
    def __init__(self, posts: list[str], update_ms: float) -> None:
        self.update_seconds = update_ms / 1000
        self._locks = {post_id: threading.Lock() for post_id in posts}
        self._counts = {post_id: 0 for post_id in posts}

    def increment(self, post_id: str) -> float:
        started = time.perf_counter()
        with self._locks[post_id]:
            if self.update_seconds > 0:
                time.sleep(self.update_seconds)
            self._counts[post_id] += 1
        return (time.perf_counter() - started) * 1000

    def hottest_post_count(self) -> int:
        return max(self._counts.values()) if self._counts else 0


def run_lock_simulation(
    mode: str,
    operations: int,
    posts: int,
    workers: int,
    update_ms: float,
    seed: int = 42,
) -> LockSimResult:
    ids = post_ids(posts)
    store = RowLockCounterStore(ids, update_ms)
    workload = generate_operations(operations, mode=mode, posts=posts, seed=seed)

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        latencies = list(executor.map(lambda operation: store.increment(operation.post_id), workload))
    elapsed_s = time.perf_counter() - started

    return LockSimResult(
        mode=mode,
        operations=operations,
        posts=posts,
        workers=workers,
        update_ms=update_ms,
        elapsed_s=round(elapsed_s, 3),
        throughput_ops_s=round(operations / elapsed_s, 1),
        avg_ms=round(statistics.mean(latencies), 3),
        p50_ms=round(_percentile(latencies, 0.50), 3),
        p95_ms=round(_percentile(latencies, 0.95), 3),
        p99_ms=round(_percentile(latencies, 0.99), 3),
        max_ms=round(max(latencies), 3),
        hottest_post_count=store.hottest_post_count(),
    )


def format_lock_results(results: list[LockSimResult]) -> str:
    lines = [
        "mode         ops      posts  workers  update_ms  ops/s     avg_ms  p95_ms   p99_ms   hottest_post",
        "-----------  -------  -----  -------  ---------  --------  ------  -------  -------  ------------",
    ]
    for result in results:
        lines.append(
            f"{result.mode:<11}  "
            f"{result.operations:>7}  "
            f"{result.posts:>5}  "
            f"{result.workers:>7}  "
            f"{result.update_ms:>9.2f}  "
            f"{result.throughput_ops_s:>8.1f}  "
            f"{result.avg_ms:>6.2f}  "
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

