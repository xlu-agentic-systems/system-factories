from __future__ import annotations

from app.benchmark import BenchmarkResult, format_results
from app.workload import generate_operations


def test_hot_workload_targets_one_post():
    operations = generate_operations(total=100, mode="hot", posts=10)

    assert {operation.post_id for operation in operations} == {"post-000001"}


def test_distributed_workload_spreads_evenly():
    operations = generate_operations(total=100, mode="distributed", posts=10)
    counts = {}
    for operation in operations:
        counts[operation.post_id] = counts.get(operation.post_id, 0) + 1

    assert set(counts.values()) == {10}


def test_format_results_includes_latency_columns():
    result = BenchmarkResult(
        mode="hot",
        operations=10,
        posts=1,
        workers=2,
        successes=10,
        elapsed_s=1.0,
        throughput_ops_s=10.0,
        avg_ms=1.0,
        p50_ms=1.0,
        p95_ms=2.0,
        p99_ms=3.0,
        max_ms=4.0,
        hottest_post_count=10,
    )

    output = format_results([result])

    assert "hot" in output
    assert "p95_ms" in output

