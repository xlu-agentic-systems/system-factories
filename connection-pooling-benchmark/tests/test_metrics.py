from __future__ import annotations

from app.metrics import percentile, summarize_latencies


def test_percentile_handles_empty_and_bounds() -> None:
    assert percentile([], 95) == 0.0
    assert percentile([1.0], 95) == 1.0
    assert percentile([1.0, 2.0, 3.0], 0) == 1.0
    assert percentile([1.0, 2.0, 3.0], 100) == 3.0


def test_summarize_latencies() -> None:
    summary = summarize_latencies([1, 2, 3, 4, 5])

    assert summary.count == 5
    assert summary.p50_ms == 3
    assert summary.p95_ms == 5
    assert summary.p99_ms == 5
    assert summary.max_ms == 5

