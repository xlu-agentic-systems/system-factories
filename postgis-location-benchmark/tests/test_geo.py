from __future__ import annotations

from app.benchmark import QueryMetric, summarize
from app.geo import bounding_box, generate_locations, generate_query_points


def test_seed_generation_is_deterministic():
    assert generate_locations(5, seed=123) == generate_locations(5, seed=123)
    assert generate_query_points(5, seed=123) == generate_query_points(5, seed=123)


def test_us_wide_generation_stays_in_us_bounds():
    rows = generate_locations(100, seed=123, distribution="us-wide")
    queries = generate_query_points(100, seed=456, distribution="us-wide")

    assert all(24.5 <= row.lat <= 49.5 for row in rows)
    assert all(-124.8 <= row.lon <= -66.9 for row in rows)
    assert all(24.5 <= query.lat <= 49.5 for query in queries)
    assert all(-124.8 <= query.lon <= -66.9 for query in queries)


def test_bounding_box_contains_center():
    lat = 40.7128
    lon = -74.0060
    min_lat, max_lat, min_lon, max_lon = bounding_box(lat, lon, 5_000)

    assert min_lat < lat < max_lat
    assert min_lon < lon < max_lon


def test_summary_groups_by_strategy():
    metrics = [
        QueryMetric("raw-lat-lon", 1_000, 10, 3.0),
        QueryMetric("raw-lat-lon", 1_000, 20, 5.0),
        QueryMetric("postgis-geography", 1_000, 10, 1.0),
    ]

    result = {summary.strategy: summary for summary in summarize(metrics)}

    assert result["raw-lat-lon"].avg_ms == 4.0
    assert result["raw-lat-lon"].avg_rows == 15.0
    assert result["postgis-geography"].p95_ms == 1.0
