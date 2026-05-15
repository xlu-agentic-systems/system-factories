from __future__ import annotations

from app.benchmark import BenchmarkResult, format_results
from app.workload import generate_operations, post_ids


def test_hot_workload_targets_one_post():
    operations = generate_operations(total=100, mode="hot", posts=10)

    assert {operation.post_id for operation in operations} == {"post-000001"}


def test_distributed_workload_spreads_evenly():
    operations = generate_operations(total=100, mode="distributed", posts=10)
    counts = {post_id: 0 for post_id in post_ids(10)}
    for operation in operations:
        counts[operation.post_id] += 1

    assert set(counts.values()) == {10}


def test_zipf_workload_has_hot_posts():
    operations = generate_operations(total=1000, mode="zipf", posts=100, seed=123)
    top_20 = {f"post-{index:06d}" for index in range(1, 21)}
    hot_count = sum(1 for operation in operations if operation.post_id in top_20)

    assert hot_count > 700


def test_format_results_includes_modes():
    result = BenchmarkResult(
        mode="hot",
        operations=10,
        posts=1,
        workers=4,
        successes=10,
        conflicts=0,
        elapsed_s=1.0,
        throughput_ops_s=10.0,
        avg_ms=1.0,
        p50_ms=1.0,
        p95_ms=2.0,
        p99_ms=3.0,
        max_ms=4.0,
        hottest_post_count=10,
    )

    assert "hot" in format_results([result])

